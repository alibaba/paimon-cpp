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

#include "paimon/core/append/append_only_writer.h"

#include <functional>
#include <map>
#include <string>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "paimon/common/data/shredding/shredding_write_plan_factories.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/long_counter.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/append_data_file_writer_factory.h"
#include "paimon/core/io/blob_data_file_writer_factory.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/io/multiple_blob_file_writer.h"
#include "paimon/core/io/rolling_blob_file_writer.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/io/shredding_append_data_file_writer_factory.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/operation/blob_file_context.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/macros.h"
#include "paimon/metrics.h"
#include "paimon/record_batch.h"

namespace paimon {

AppendOnlyWriter::AppendOnlyWriter(
    const CoreOptions& options, int64_t schema_id,
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::optional<std::vector<std::string>>& write_cols, int64_t max_sequence_number,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<CompactManager>& compact_manager,
    const std::shared_ptr<MapSharedShreddingContext>& shredding_context,
    const std::shared_ptr<MemoryPool>& memory_pool)
    : options_(options),
      schema_id_(schema_id),
      write_schema_(write_schema),
      write_cols_(write_cols),
      seq_num_counter_(std::make_shared<LongCounter>(max_sequence_number + 1)),
      path_factory_(path_factory),
      compact_manager_(compact_manager),
      memory_pool_(memory_pool),
      metrics_(std::make_shared<MetricsImpl>()),
      shredding_context_(shredding_context) {}

AppendOnlyWriter::~AppendOnlyWriter() = default;

Status AppendOnlyWriter::Write(std::unique_ptr<RecordBatch>&& batch) {
    for (const auto& row_kind : batch->GetRowKind()) {
        if (PAIMON_UNLIKELY(row_kind != RecordBatch::RowKind::INSERT)) {
            PAIMON_ASSIGN_OR_RAISE(const RowKind* kind,
                                   RowKind::FromByteValue(static_cast<int8_t>(row_kind)));
            return Status::Invalid("Append only writer can not accept record batch with RowKind ",
                                   kind->Name());
        }
    }
    if (writer_ == nullptr) {
        PAIMON_ASSIGN_OR_RAISE(writer_, CreateRollingRowWriter());
    }

    if (!inline_descriptor_fields_.empty() || !inline_view_fields_.empty()) {
        auto data_type = arrow::struct_(write_schema_->fields());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                          arrow::ImportArray(batch->GetData(), data_type));
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
        PAIMON_RETURN_NOT_OK(BlobUtils::ValidateBlobInlineFields(
            struct_array, inline_descriptor_fields_, "blob-descriptor-field"));
        PAIMON_RETURN_NOT_OK(BlobUtils::ValidateBlobInlineFields(struct_array, inline_view_fields_,
                                                                 "blob-view-field"));
        ::ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, &c_array));
        return writer_->Write(&c_array);
    }

    return writer_->Write(batch->GetData());
}

Result<CommitIncrement> AppendOnlyWriter::PrepareCommit(bool wait_compaction) {
    PAIMON_RETURN_NOT_OK(
        Flush(/*wait_for_latest_compaction=*/false, /*forced_full_compaction=*/false));
    PAIMON_RETURN_NOT_OK(TrySyncLatestCompaction(wait_compaction || options_.CommitForceCompact()));
    return DrainIncrement();
}

Result<CommitIncrement> AppendOnlyWriter::DrainIncrement() {
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

Status AppendOnlyWriter::TrySyncLatestCompaction(bool blocking) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<CompactResult>> result,
                           compact_manager_->GetCompactionResult(blocking));
    if (result.has_value()) {
        const auto& compaction_result = result.value();
        const auto& before = compaction_result->Before();
        compact_before_.insert(compact_before_.end(), before.begin(), before.end());
        const auto& after = compaction_result->After();
        compact_after_.insert(compact_after_.end(), after.begin(), after.end());
        PAIMON_RETURN_NOT_OK(UpdateCompactDeletionFile(compaction_result->DeletionFile()));
    }
    return Status::OK();
}

Status AppendOnlyWriter::UpdateCompactDeletionFile(
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

Status AppendOnlyWriter::Flush(bool wait_for_latest_compaction, bool forced_full_compaction) {
    std::vector<std::shared_ptr<DataFileMeta>> flushed_files;
    if (writer_) {
        PAIMON_RETURN_NOT_OK(writer_->Close());
        PAIMON_ASSIGN_OR_RAISE(flushed_files, writer_->GetResult());
    }
    // add new generated files
    for (const auto& flushed_file : flushed_files) {
        PAIMON_RETURN_NOT_OK(compact_manager_->AddNewFile(flushed_file));
    }
    PAIMON_RETURN_NOT_OK(TrySyncLatestCompaction(wait_for_latest_compaction));
    PAIMON_RETURN_NOT_OK(compact_manager_->TriggerCompaction(forced_full_compaction));
    new_files_.insert(new_files_.end(), flushed_files.begin(), flushed_files.end());
    if (writer_) {
        metrics_->Merge(writer_->GetMetrics());
        writer_.reset();
    }
    return Status::OK();
}

AppendOnlyWriter::RollingFileWriterResult AppendOnlyWriter::CreateRollingRowWriter() {
    auto blob_context = BlobFileContext::Create(write_schema_, options_);

    // Save inline descriptor and view fields for validation in Write()
    if (blob_context) {
        inline_descriptor_fields_ = blob_context->GetDescriptorFields();
        inline_view_fields_ = blob_context->GetViewFields();
    }

    if (blob_context && blob_context->RequireBlobFileWriter()) {
        // Use context-aware schema separation: inline BLOB fields stay in main
        auto schemas =
            BlobUtils::SeparateBlobSchema(write_schema_, blob_context->GetInlineFields());
        return CreateRollingBlobWriter(schemas, blob_context->GetInlineFields());
    }

    // No BLOB fields, or all BLOB fields are inline and no .blob files are needed.
    return std::make_unique<RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
        options_.GetTargetFileSize(/*has_primary_key=*/false),
        GetDataFileWriterFactory(write_schema_, write_cols_));
}

AppendOnlyWriter::WriterFactory AppendOnlyWriter::GetDataFileWriterFactory(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::optional<std::vector<std::string>>& write_cols) const {
    if (auto plan_factory = ShreddingWritePlanFactories::SelectActive(
            options_, schema, shredding_context_, memory_pool_)) {
        return std::make_shared<ShreddingAppendDataFileWriterFactory>(
            options_, schema_id_, schema, write_cols, seq_num_counter_, FileSource::Append(),
            path_factory_, plan_factory, memory_pool_);
    }
    return std::make_shared<AppendDataFileWriterFactory>(options_, schema_id_, schema, write_cols,
                                                         seq_num_counter_, FileSource::Append(),
                                                         path_factory_, memory_pool_);
}

AppendOnlyWriter::WriterFactory AppendOnlyWriter::GetBlobFileWriterFactory(
    const std::shared_ptr<arrow::Schema>& single_field_schema,
    const std::optional<std::vector<std::string>>& write_cols) const {
    std::shared_ptr<DataFilePathFactory> path_factory = path_factory_;
    return std::make_shared<BlobDataFileWriterFactory>(options_, schema_id_, single_field_schema,
                                                       write_cols, seq_num_counter_, path_factory,
                                                       memory_pool_);
}

AppendOnlyWriter::RollingFileWriterResult AppendOnlyWriter::CreateRollingBlobWriter(
    const BlobUtils::SeparatedSchemas& schemas, const std::set<std::string>& inline_fields) const {
    // Multiple blob fields are supported. Each blob field gets its own rolling file writer
    // via MultipleBlobFileWriter.
    auto blob_schema = schemas.blob_schema;
    MultipleBlobFileWriter::BlobWriterCreator blob_writer_creator =
        [this, blob_schema](const std::string& blob_field_name)
        -> Result<
            std::unique_ptr<RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>> {
        // Create a single-field schema for this blob field
        auto field = blob_schema->GetFieldByName(blob_field_name);
        if (!field) {
            return Status::Invalid(
                fmt::format("Blob field '{}' not found in blob schema", blob_field_name));
        }
        auto single_field_schema = arrow::schema({field});
        std::vector<std::string> write_cols = {blob_field_name};
        auto single_blob_file_writer_factory =
            GetBlobFileWriterFactory(single_field_schema, write_cols);
        return std::make_unique<RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
            options_.GetBlobTargetFileSize(), single_blob_file_writer_factory);
    };

    return std::make_unique<RollingBlobFileWriter>(
        options_.GetTargetFileSize(/*has_primary_key=*/false),
        GetDataFileWriterFactory(schemas.main_schema, schemas.main_schema->field_names()),
        blob_schema, blob_writer_creator, arrow::struct_(write_schema_->fields()), inline_fields);
}

Status AppendOnlyWriter::Sync() {
    return TrySyncLatestCompaction(/*blocking=*/true);
}

Status AppendOnlyWriter::Close() {
    // Request cancellation and wait for running compaction to exit.
    // This avoids reusing cancellation state while an old task is still running.
    compact_manager_->CancelAndWaitCompaction();
    PAIMON_RETURN_NOT_OK(Sync());

    PAIMON_RETURN_NOT_OK(compact_manager_->Close());
    auto fs = options_.GetFileSystem();
    for (const auto& file : compact_after_) {
        // AppendOnlyCompactManager will rewrite the file and no file upgrade will occur, so we
        // can directly delete the file in compact_after_.
        [[maybe_unused]] auto s = fs->Delete(path_factory_->ToPath(file));
    }

    if (writer_) {
        writer_->Abort();
        writer_.reset();
    }

    if (compact_deletion_file_ != nullptr) {
        compact_deletion_file_->Clean();
    }
    return Status::OK();
}
}  // namespace paimon
