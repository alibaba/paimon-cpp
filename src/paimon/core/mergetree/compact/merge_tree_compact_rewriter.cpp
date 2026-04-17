/*
 * Copyright 2026-present Alibaba Inc.
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
#include "paimon/core/mergetree/compact/merge_tree_compact_rewriter.h"

#include <cassert>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/key_value_data_file_writer.h"
#include "paimon/core/io/key_value_meta_projection_consumer.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/format/file_format.h"
#include "paimon/format/writer_builder.h"
#include "paimon/read_context.h"
namespace paimon {
MergeTreeCompactRewriter::MergeTreeCompactRewriter(
    const BinaryRow& partition, int32_t bucket, int64_t schema_id,
    const std::vector<std::string>& trimmed_primary_keys, const CoreOptions& options,
    const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<arrow::Schema>& write_schema, DeletionVector::Factory dv_factory,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    std::unique_ptr<MergeFileSplitRead>&& merge_file_split_read,
    MergeFunctionWrapperFactory merge_function_wrapper_factory,
    const std::shared_ptr<CancellationController>& cancellation_controller,
    const std::shared_ptr<MemoryPool>& pool)
    : options_(options),
      merge_file_split_read_(std::move(merge_file_split_read)),
      pool_(pool),
      partition_(partition),
      bucket_(bucket),
      schema_id_(schema_id),
      trimmed_primary_keys_(trimmed_primary_keys),
      data_schema_(data_schema),
      write_schema_(write_schema),
      dv_factory_(std::move(dv_factory)),
      path_factory_cache_(path_factory_cache),
      merge_function_wrapper_factory_(std::move(merge_function_wrapper_factory)),
      cancellation_controller_(cancellation_controller) {
    assert(cancellation_controller_ != nullptr);
}

Result<std::unique_ptr<MergeTreeCompactRewriter>> MergeTreeCompactRewriter::Create(
    int32_t bucket, const BinaryRow& partition, const std::shared_ptr<TableSchema>& table_schema,
    DeletionVector::Factory dv_factory,
    const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
    const CoreOptions& options,
    const std::shared_ptr<CancellationController>& cancellation_controller,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema->TrimmedPrimaryKeys());
    auto data_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    auto write_schema = SpecialFields::CompleteSequenceAndValueKindField(data_schema);

    // TODO(xinyu.lxy): set executor
    ReadContextBuilder read_context_builder(path_factory_cache->RootPath());
    read_context_builder.SetOptions(options.ToMap())
        .WithFileSystem(options.GetFileSystem())
        .EnablePrefetch(true)
        .SetPrefetchMaxParallelNum(1)
        .SetPrefetchBatchCount(3)
        .WithMemoryPool(pool);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context,
                           read_context_builder.Finish());
    // TODO(xinyu.lxy): temporarily disabled pre-buffer for parquet, which may cause high memory
    // usage during compaction. Will fix via parquet format refactor.
    auto new_options = options.ToMap();
    if (new_options.find("parquet.read.enable-pre-buffer") == new_options.end()) {
        new_options["parquet.read.enable-pre-buffer"] = "false";
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> internal_context,
                           InternalReadContext::Create(read_context, table_schema, new_options));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        path_factory_cache->GetOrCreatePathFactory(options.GetFileFormat()->Identifier()));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<MergeFileSplitRead> merge_file_split_read,
        MergeFileSplitRead::Create(path_factory, internal_context, pool, CreateDefaultExecutor()));
    auto merge_function_wrapper_factory =
        [](int32_t output_level) -> Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>> {
        return std::shared_ptr<MergeFunctionWrapper<KeyValue>>();
    };
    return std::unique_ptr<MergeTreeCompactRewriter>(new MergeTreeCompactRewriter(
        partition, bucket, table_schema->Id(), trimmed_primary_keys, options, data_schema,
        write_schema, std::move(dv_factory), path_factory_cache, std::move(merge_file_split_read),
        merge_function_wrapper_factory, cancellation_controller, pool));
}

Result<CompactResult> MergeTreeCompactRewriter::Upgrade(int32_t output_level,
                                                        const std::shared_ptr<DataFileMeta>& file) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> upgraded_file,
                           file->Upgrade(output_level));
    return CompactResult({file}, {upgraded_file});
}

Result<CompactResult> MergeTreeCompactRewriter::Rewrite(
    int32_t output_level, bool drop_delete, const std::vector<std::vector<SortedRun>>& sections) {
    return RewriteCompaction(output_level, drop_delete, sections);
}

std::vector<std::shared_ptr<DataFileMeta>> MergeTreeCompactRewriter::ExtractFilesFromSections(
    const std::vector<std::vector<SortedRun>>& sections) {
    std::vector<std::shared_ptr<DataFileMeta>> files;
    for (const auto& section : sections) {
        for (const auto& sorted_run : section) {
            auto files_in_run = sorted_run.Files();
            files.insert(files.end(), files_in_run.begin(), files_in_run.end());
        }
    }
    return files;
}

std::unique_ptr<MergeTreeCompactRewriter::KeyValueRollingFileWriter>
MergeTreeCompactRewriter::CreateRollingRowWriter(int32_t level) {
    auto create_file_writer = [this, level]()
        -> Result<std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>> {
        ::ArrowSchema arrow_schema{};
        ScopeGuard guard([&arrow_schema]() { ArrowSchemaRelease(&arrow_schema); });
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*write_schema_, &arrow_schema));
        auto format = options_.GetWriteFileFormat(level);
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
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                               CreateDataFilePathFactory(format->Identifier()));

        auto writer = std::make_unique<KeyValueDataFileWriter>(
            options_.GetWriteFileCompression(level), converter, schema_id_, level,
            FileSource::Compact(), trimmed_primary_keys_, stats_extractor, write_schema_,
            data_file_path_factory->IsExternalPath(), pool_);
        PAIMON_RETURN_NOT_OK(writer->Init(options_.GetFileSystem(),
                                          data_file_path_factory->NewPath(), writer_builder));
        return writer;
    };
    return std::make_unique<MergeTreeCompactRewriter::KeyValueRollingFileWriter>(
        options_.GetTargetFileSize(/*has_primary_key=*/true), create_file_writer);
}

Result<MergeTreeCompactRewriter::KeyValueConsumerCreator>
MergeTreeCompactRewriter::GenerateKeyValueConsumer() const {
    if (!merge_file_split_read_) {
        return Status::Invalid(
            "merge_file_split_read in MergeTreeCompactRewriter cannot be nullptr");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> target_to_src_mapping,
                           ArrowUtils::CreateProjection(
                               /*src_schema=*/merge_file_split_read_->GetValueSchema(),
                               /*target_fields=*/data_schema_->fields()));
    return MergeTreeCompactRewriter::KeyValueConsumerCreator(
        [target_schema = write_schema_, pool = pool_,
         target_to_src_mapping = std::move(target_to_src_mapping)]()
            -> Result<std::unique_ptr<RowToArrowArrayConverter<KeyValue, KeyValueBatch>>> {
            return KeyValueMetaProjectionConsumer::Create(target_schema, target_to_src_mapping,
                                                          pool);
        });
}

Result<std::shared_ptr<DataFilePathFactory>> MergeTreeCompactRewriter::CreateDataFilePathFactory(
    const std::string& format) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                           path_factory_cache_->GetOrCreatePathFactory(format));
    return path_factory->CreateDataFilePathFactory(partition_, bucket_);
}

Status MergeTreeCompactRewriter::MergeReadAndWrite(
    int32_t output_level, bool drop_delete, const std::vector<SortedRun>& section,
    const MergeTreeCompactRewriter::KeyValueConsumerCreator& create_consumer,
    MergeTreeCompactRewriter::KeyValueRollingFileWriter* rolling_writer,
    std::vector<std::shared_ptr<MergeTreeCompactRewriter::KeyValueMergeReader>>*
        reader_holders_ptr) {
    if (!merge_file_split_read_) {
        return Status::Invalid(
            "merge_file_split_read in MergeTreeCompactRewriter cannot be nullptr");
    }
    auto& reader_holders = *reader_holders_ptr;
    // prepare sort merge reader
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                           CreateDataFilePathFactory(options_.GetFileFormat()->Identifier()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MergeFunctionWrapper<KeyValue>> wrapper,
                           merge_function_wrapper_factory_(output_level));
    merge_file_split_read_->SetMergeFunctionWrapper(wrapper);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortMergeReader> sort_merge_reader,
                           merge_file_split_read_->CreateSortMergeReaderForSection(
                               section, partition_, dv_factory_,
                               /*predicate=*/nullptr, data_file_path_factory, drop_delete));
    if (!rolling_writer) {
        // Short-circuit logic: for no rolling writers, simply iterating through the KeyValue
        // iterator is sufficient to ensure lookup merge function take effect.
        while (true) {
            if (cancellation_controller_->IsCancelled()) {
                return Status::Cancelled("Compaction is cancelled");
            }
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortMergeReader::Iterator> key_value_iter,
                                   sort_merge_reader->NextBatch());
            if (key_value_iter == nullptr) {
                break;
            }
            while (true) {
                PAIMON_ASSIGN_OR_RAISE(bool has_next, key_value_iter->HasNext());
                if (!has_next) {
                    break;
                }
                [[maybe_unused]] KeyValue kv = key_value_iter->Next();
            }
        }
        return Status::OK();
    }

    // consumer batch size is WriteBatchSize
    auto async_key_value_producer_consumer =
        std::make_shared<AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch>>(
            std::move(sort_merge_reader), create_consumer, options_.GetWriteBatchSize(),
            /*projection_thread_num=*/1, pool_);
    reader_holders.push_back(async_key_value_producer_consumer);
    // read KeyValueBatch from SortMergeReader and write to RollingWriter
    while (true) {
        if (cancellation_controller_->IsCancelled()) {
            return Status::Cancelled("Compaction is cancelled");
        }
        PAIMON_ASSIGN_OR_RAISE(KeyValueBatch key_value_batch,
                               async_key_value_producer_consumer->NextBatch());
        if (key_value_batch.batch == nullptr) {
            break;
        }
        PAIMON_RETURN_NOT_OK(rolling_writer->Write(std::move(key_value_batch)));
    }
    return Status::OK();
}

Result<CompactResult> MergeTreeCompactRewriter::RewriteCompaction(
    int32_t output_level, bool drop_delete, const std::vector<std::vector<SortedRun>>& sections) {
    PAIMON_ASSIGN_OR_RAISE(MergeTreeCompactRewriter::KeyValueConsumerCreator create_consumer,
                           GenerateKeyValueConsumer());

    std::vector<std::shared_ptr<MergeTreeCompactRewriter::KeyValueMergeReader>> reader_holders;
    auto rolling_writer = CreateRollingRowWriter(output_level);

    ScopeGuard write_guard([&]() -> void {
        rolling_writer->Abort();
        merge_file_split_read_.reset();
        for (const auto& reader : reader_holders) {
            reader->Close();
        }
    });

    for (const auto& section : sections) {
        PAIMON_RETURN_NOT_OK(MergeReadAndWrite(output_level, drop_delete, section, create_consumer,
                                               rolling_writer.get(), &reader_holders));
    }

    PAIMON_RETURN_NOT_OK(rolling_writer->Close());

    auto before = ExtractFilesFromSections(sections);
    NotifyRewriteCompactBefore(before);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> after,
                           rolling_writer->GetResult());
    PAIMON_ASSIGN_OR_RAISE(after, NotifyRewriteCompactAfter(after));
    write_guard.Release();

    return CompactResult(before, after);
}

}  // namespace paimon
