/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/mergetree/merge_tree_writer.h"

#include <algorithm>
#include <cassert>
#include <unordered_set>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/io/async_key_value_producer_and_consumer.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/io/key_value_data_file_writer.h"
#include "paimon/core/io/key_value_meta_projection_consumer.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/mergetree/compact/sort_merge_reader_with_loser_tree.h"
#include "paimon/core/mergetree/write_buffer.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/format/file_format.h"
#include "paimon/format/writer_builder.h"

namespace paimon {
class FormatStatsExtractor;

Result<std::shared_ptr<MergeTreeWriter>> MergeTreeWriter::Create(
    int64_t last_sequence_number, const std::vector<std::string>& trimmed_primary_keys,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
    int64_t schema_id, const std::shared_ptr<arrow::Schema>& value_schema,
    const CoreOptions& options, const std::shared_ptr<CompactManager>& compact_manager,
    const std::shared_ptr<IOManager>& io_manager, const std::shared_ptr<MemoryPool>& pool) {
    auto write_schema = SpecialFields::CompleteSequenceAndValueKindField(value_schema);
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<WriteBuffer> write_buffer,
        WriteBuffer::Create(last_sequence_number, value_schema, trimmed_primary_keys,
                            options.GetSequenceField(), key_comparator, user_defined_seq_comparator,
                            merge_function_wrapper, options, io_manager, pool));
    return std::shared_ptr<MergeTreeWriter>(
        new MergeTreeWriter(pool, trimmed_primary_keys, options, path_factory, key_comparator,
                            user_defined_seq_comparator, merge_function_wrapper, schema_id,
                            write_schema, compact_manager, std::move(write_buffer)));
}

MergeTreeWriter::MergeTreeWriter(
    const std::shared_ptr<MemoryPool>& pool, const std::vector<std::string>& trimmed_primary_keys,
    const CoreOptions& options, const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
    int64_t schema_id, const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<CompactManager>& compact_manager,
    std::unique_ptr<WriteBuffer>&& write_buffer)
    : pool_(pool),
      trimmed_primary_keys_(trimmed_primary_keys),
      options_(options),
      path_factory_(path_factory),
      key_comparator_(key_comparator),
      user_defined_seq_comparator_(user_defined_seq_comparator),
      merge_function_wrapper_(merge_function_wrapper),
      schema_id_(schema_id),
      write_schema_(write_schema),
      compact_manager_(compact_manager),
      write_buffer_(std::move(write_buffer)),
      metrics_(std::make_shared<MetricsImpl>()) {}

Status MergeTreeWriter::DoClose() {
    // Request cancellation and wait for running compaction to exit.
    // This avoids reusing cancellation state while an old task is still running.
    compact_manager_->CancelAndWaitCompaction();
    PAIMON_RETURN_NOT_OK(Sync());
    PAIMON_RETURN_NOT_OK(compact_manager_->Close());

    // delete temporary files
    std::vector<std::shared_ptr<DataFileMeta>> delete_files;
    delete_files.reserve(new_files_.size() + compact_after_.size());
    delete_files.insert(delete_files.end(), new_files_.begin(), new_files_.end());
    for (const auto& file : compact_after_) {
        // Upgrade file is required by previous snapshot, so we should ensure that this file is
        // not the output of upgraded.
        auto in_compact_before =
            std::any_of(compact_before_.begin(), compact_before_.end(),
                        [&file](const std::shared_ptr<DataFileMeta>& candidate) {
                            return candidate->file_name == file->file_name;
                        });
        if (!in_compact_before) {
            delete_files.push_back(file);
        }
    }
    for (const auto& file : delete_files) {
        // Keep Java parity: temporary file cleanup is quiet.
        [[maybe_unused]] auto s = options_.GetFileSystem()->Delete(path_factory_->ToPath(file));
    }

    write_buffer_->Clear();
    new_files_.clear();
    deleted_files_.clear();
    compact_before_.clear();
    compact_after_.clear();

    if (compact_deletion_file_) {
        compact_deletion_file_->Clean();
        compact_deletion_file_.reset();
    }
    return Status::OK();
}

Status MergeTreeWriter::FlushMemory() {
    PAIMON_ASSIGN_OR_RAISE(bool has_remaining_quota, write_buffer_->FlushMemory());
    if (!has_remaining_quota) {
        PAIMON_RETURN_NOT_OK(FlushWriteBuffer(/*wait_for_latest_compaction=*/false,
                                              /*forced_full_compaction=*/false));
    }
    return Status::OK();
}

Status MergeTreeWriter::Write(std::unique_ptr<RecordBatch>&& moved_batch) {
    PAIMON_ASSIGN_OR_RAISE(bool has_remaining_quota, write_buffer_->Write(std::move(moved_batch)));
    if (!has_remaining_quota) {
        return FlushWriteBuffer(/*wait_for_latest_compaction=*/false,
                                /*forced_full_compaction=*/false);
    }
    return Status::OK();
}

Status MergeTreeWriter::Compact(bool full_compaction) {
    return FlushWriteBuffer(/*wait_for_latest_compaction=*/true, full_compaction);
}

Status MergeTreeWriter::Sync() {
    return TrySyncLatestCompaction(/*blocking=*/true);
}

Status MergeTreeWriter::TrySyncLatestCompaction(bool blocking) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<CompactResult>> result,
                           compact_manager_->GetCompactionResult(blocking));
    if (result) {
        PAIMON_RETURN_NOT_OK(UpdateCompactResult(result.value()));
    }
    return Status::OK();
}

Status MergeTreeWriter::UpdateCompactResult(const std::shared_ptr<CompactResult>& compact_result) {
    std::unordered_set<std::string> after_files;
    after_files.reserve(compact_result->After().size());
    for (const auto& file : compact_result->After()) {
        after_files.insert(file->file_name);
    }

    auto in_compact_before = [this](const std::string& file_name) {
        return std::any_of(compact_before_.begin(), compact_before_.end(),
                           [&file_name](const std::shared_ptr<DataFileMeta>& meta) {
                               return meta->file_name == file_name;
                           });
    };

    for (const auto& file : compact_result->Before()) {
        auto compact_after_it =
            std::find_if(compact_after_.begin(), compact_after_.end(),
                         [&file](const std::shared_ptr<DataFileMeta>& candidate) {
                             return candidate->file_name == file->file_name;
                         });
        if (compact_after_it != compact_after_.end()) {
            compact_after_.erase(compact_after_it);
            // This is an intermediate file (not a new data file), which is no longer needed
            // after compaction and can be deleted directly, but upgrade file is required by
            // previous snapshot and following snapshot, so we should ensure:
            // 1. This file is not the output of upgraded.
            // 2. This file is not the input of upgraded.
            if (!in_compact_before(file->file_name) &&
                after_files.find(file->file_name) == after_files.end()) {
                auto fs = options_.GetFileSystem();
                [[maybe_unused]] auto s = fs->Delete(path_factory_->ToPath(file));
            }
        } else {
            compact_before_.push_back(file);
        }
    }

    compact_after_.insert(compact_after_.end(), compact_result->After().begin(),
                          compact_result->After().end());
    // TODO(yonghao.fyh): support compact changelog
    return UpdateCompactDeletionFile(compact_result->DeletionFile());
}

Status MergeTreeWriter::UpdateCompactDeletionFile(
    const std::shared_ptr<CompactDeletionFile>& new_deletion_file) {
    if (new_deletion_file) {
        if (compact_deletion_file_ == nullptr) {
            compact_deletion_file_ = new_deletion_file;
        } else {
            PAIMON_ASSIGN_OR_RAISE(compact_deletion_file_,
                                   new_deletion_file->MergeOldFile(compact_deletion_file_));
        }
    }
    return Status::OK();
}

Result<CommitIncrement> MergeTreeWriter::PrepareCommit(bool wait_compaction) {
    PAIMON_RETURN_NOT_OK(FlushWriteBuffer(wait_compaction, /*forced_full_compaction=*/false));
    if (options_.CommitForceCompact()) {
        wait_compaction = true;
    }
    // Decide again whether to wait here.
    // For example, in the case of repeated failures in writing, it is possible that Level 0
    // files were successfully committed, but failed to restart during the compaction phase,
    // which may result in an increasing number of Level 0 files. This wait can avoid this
    // situation.
    if (compact_manager_->ShouldWaitForPreparingCheckpoint()) {
        wait_compaction = true;
    }
    PAIMON_RETURN_NOT_OK(TrySyncLatestCompaction(wait_compaction));
    return DrainIncrement();
}

Result<bool> MergeTreeWriter::CompactNotCompleted() {
    PAIMON_RETURN_NOT_OK(compact_manager_->TriggerCompaction(/*full_compaction=*/false));
    return compact_manager_->CompactNotCompleted();
}

Status MergeTreeWriter::FlushWriteBuffer(bool wait_for_latest_compaction,
                                         bool forced_full_compaction) {
    if (!write_buffer_->IsEmpty()) {
        if (compact_manager_->ShouldWaitForLatestCompaction()) {
            wait_for_latest_compaction = true;
        }
        auto cleanup_guard = ScopeGuard([&]() { write_buffer_->Clear(); });
        // 1. flush write buffer to get sorted readers
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
                               write_buffer_->CreateReaders());
        // 2. prepare loser tree sort merge reader
        auto sort_merge_reader = std::make_unique<SortMergeReaderWithLoserTree>(
            std::move(readers), key_comparator_, user_defined_seq_comparator_,
            merge_function_wrapper_);
        // 3. project key value to arrow array
        auto create_consumer = [target_schema = write_schema_, pool = pool_]()
            -> Result<std::unique_ptr<RowToArrowArrayConverter<KeyValue, KeyValueBatch>>> {
            return KeyValueMetaProjectionConsumer::Create(target_schema, pool);
        };
        // consumer batch size is WriteBatchSize
        auto async_key_value_producer_consumer =
            std::make_unique<AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch>>(
                std::move(sort_merge_reader), create_consumer, options_.GetWriteBatchSize(),
                /*projection_thread_num=*/1, pool_);
        auto rolling_writer = CreateRollingRowWriter();
        ScopeGuard write_guard([&]() -> void {
            rolling_writer->Abort();
            async_key_value_producer_consumer->Close();
        });
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(KeyValueBatch key_value_batch,
                                   async_key_value_producer_consumer->NextBatch());
            if (key_value_batch.batch == nullptr) {
                break;
            }
            PAIMON_RETURN_NOT_OK(rolling_writer->Write(std::move(key_value_batch)));
        }
        PAIMON_RETURN_NOT_OK(rolling_writer->Close());
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> flushed_files,
                               rolling_writer->GetResult());
        async_key_value_producer_consumer->Close();
        write_guard.Release();

        for (const auto& flushed_file : flushed_files) {
            new_files_.emplace_back(flushed_file);
            PAIMON_RETURN_NOT_OK(compact_manager_->AddNewFile(flushed_file));
        }
        metrics_->Merge(rolling_writer->GetMetrics());
    }
    PAIMON_RETURN_NOT_OK(TrySyncLatestCompaction(wait_for_latest_compaction));
    PAIMON_RETURN_NOT_OK(compact_manager_->TriggerCompaction(forced_full_compaction));
    return Status::OK();
}

Result<CommitIncrement> MergeTreeWriter::DrainIncrement() {
    DataIncrement data_increment(std::move(new_files_), std::move(deleted_files_), {});
    CompactIncrement compact_increment(std::move(compact_before_), std::move(compact_after_), {});
    auto drain_deletion_file = compact_deletion_file_;

    new_files_.clear();
    deleted_files_.clear();
    compact_before_.clear();
    compact_after_.clear();
    compact_deletion_file_ = nullptr;

    return CommitIncrement(data_increment, compact_increment, drain_deletion_file);
}

std::unique_ptr<RollingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>
MergeTreeWriter::CreateRollingRowWriter() const {
    auto create_file_writer = [&]()
        -> Result<std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>> {
        ::ArrowSchema arrow_schema;
        ScopeGuard guard([&arrow_schema]() { ArrowSchemaRelease(&arrow_schema); });
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*write_schema_, &arrow_schema));
        auto format = options_.GetWriteFileFormat(/*level=*/0);
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<WriterBuilder> writer_builder,
            format->CreateWriterBuilder(&arrow_schema, options_.GetWriteBatchSize()));
        writer_builder->WithMemoryPool(pool_);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*write_schema_, &arrow_schema));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatStatsExtractor> stats_extractor,
                               format->CreateStatsExtractor(&arrow_schema));
        auto converter = [](KeyValueBatch key_value_batch, ArrowArray* array) -> Status {
            ArrowArrayMove(key_value_batch.batch.get(), array);
            return Status::OK();
        };
        auto writer = std::make_unique<KeyValueDataFileWriter>(
            options_.GetWriteFileCompression(0), converter, schema_id_, /*level=*/0,
            FileSource::Append(), trimmed_primary_keys_, stats_extractor, write_schema_,
            path_factory_->IsExternalPath(), pool_);
        PAIMON_RETURN_NOT_OK(
            writer->Init(options_.GetFileSystem(), path_factory_->NewPath(), writer_builder));
        return writer;
    };
    return std::make_unique<RollingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>(
        options_.GetTargetFileSize(/*has_primary_key=*/true), create_file_writer);
}

}  // namespace paimon
