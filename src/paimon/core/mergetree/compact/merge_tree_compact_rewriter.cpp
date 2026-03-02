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

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/core/io/async_key_value_producer_and_consumer.h"
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
    int32_t bucket, const BinaryRow& partition, int64_t schema_id,
    const std::vector<std::string>& trimmed_primary_keys, const CoreOptions& options,
    const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<arrow::Schema>& write_schema,
    std::unique_ptr<MergeFileSplitRead>&& merge_file_split_read,
    const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      bucket_(bucket),
      partition_(partition),
      schema_id_(schema_id),
      trimmed_primary_keys_(trimmed_primary_keys),
      options_(options),
      data_schema_(data_schema),
      write_schema_(write_schema),
      merge_file_split_read_(std::move(merge_file_split_read)) {}

Result<std::unique_ptr<MergeTreeCompactRewriter>> MergeTreeCompactRewriter::Create(
    int32_t bucket, const BinaryRow& partition, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& options,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Executor>& executor) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema->TrimmedPrimaryKeys());
    auto data_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    auto write_schema = SpecialFields::CompleteSequenceAndValueKindField(data_schema);

    ReadContextBuilder read_context_builder(path_factory->RootPath());
    read_context_builder.SetOptions(options.ToMap())
        .EnablePrefetch(true)
        .WithMemoryPool(pool)
        .WithExecutor(executor);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context,
                           read_context_builder.Finish());

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> interal_context,
        InternalReadContext::Create(read_context, table_schema, options.ToMap()));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<MergeFileSplitRead> merge_file_split_read,
        MergeFileSplitRead::Create(path_factory, interal_context, pool, executor));
    return std::unique_ptr<MergeTreeCompactRewriter>(new MergeTreeCompactRewriter(
        bucket, partition, table_schema->Id(), trimmed_primary_keys, options, data_schema,
        write_schema, std::move(merge_file_split_read), pool));
}

Result<CompactResult> MergeTreeCompactRewriter::Rewrite(
    int32_t output_level, bool drop_delete, const std::vector<std::vector<SortedRun>>& sections) {
    return RewriteCompaction(output_level, drop_delete, sections);
}

std::unique_ptr<RollingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>
MergeTreeCompactRewriter::CreateRollingRowWriter(
    int32_t level, const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const {
    auto create_file_writer = [&]()
        -> Result<std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>> {
        ::ArrowSchema arrow_schema;
        ScopeGuard guard([&arrow_schema]() { ArrowSchemaRelease(&arrow_schema); });
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*write_schema_, &arrow_schema));
        auto format = options_.GetWriteFileFormat();
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
            options_.GetFileCompression(), converter, schema_id_, level, FileSource::Compact(),
            trimmed_primary_keys_, stats_extractor, write_schema_,
            data_file_path_factory->IsExternalPath(), pool_);
        PAIMON_RETURN_NOT_OK(writer->Init(options_.GetFileSystem(),
                                          data_file_path_factory->NewPath(), writer_builder));
        return writer;
    };
    return std::make_unique<RollingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>(
        options_.GetTargetFileSize(/*has_primary_key=*/true), create_file_writer);
}

Result<CompactResult> MergeTreeCompactRewriter::RewriteCompaction(
    int32_t output_level, bool drop_delete, const std::vector<std::vector<SortedRun>>& sections) {
    auto path_factory = merge_file_split_read_->GetPathFactory();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                           path_factory->CreateDataFilePathFactory(partition_, bucket_));

    PAIMON_ASSIGN_OR_RAISE(
        std::vector<int32_t> target_to_src_mapping,
        ArrowUtils::CreateProjection(
            /*src=*/merge_file_split_read_->GetValueSchema(), /*target=*/data_schema_->fields()));
    auto create_consumer = [target_schema = write_schema_, pool = pool_, target_to_src_mapping]()
        -> Result<std::unique_ptr<RowToArrowArrayConverter<KeyValue, KeyValueBatch>>> {
        return KeyValueMetaProjectionConsumer::Create(target_schema, target_to_src_mapping, pool);
    };

    std::vector<std::unique_ptr<AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch>>>
        reader_holders;
    auto rolling_writer = CreateRollingRowWriter(output_level, data_file_path_factory);
    ScopeGuard write_guard([&]() -> void {
        (void)rolling_writer->Close();
        rolling_writer->Abort();
        for (const auto& reader : reader_holders) {
            reader->Close();
        }
    });

    for (const auto& section : sections) {
        // prepare loser tree sort merge reader
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortMergeReader> sort_merge_reader,
                               merge_file_split_read_->CreateSortMergeReaderForSection(
                                   section, partition_, /*deletion_file_map=*/{},
                                   /*predicate=*/nullptr, data_file_path_factory, drop_delete));

        // consumer batch size is WriteBatchSize
        auto async_key_value_producer_consumer =
            std::make_unique<AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch>>(
                std::move(sort_merge_reader), create_consumer,
                std::min(options_.GetWriteBatchSize(), MAX_PROJECTION_BATCH_SIZE),
                /*projection_thread_num=*/1, pool_);

        // read KeyValueBatch from SortMergeReader and write to RollingWriter
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(KeyValueBatch key_value_batch,
                                   async_key_value_producer_consumer->NextBatch());
            if (key_value_batch.batch == nullptr) {
                break;
            }
            PAIMON_RETURN_NOT_OK(rolling_writer->Write(std::move(key_value_batch)));
        }
        reader_holders.push_back(std::move(async_key_value_producer_consumer));
    }
    PAIMON_RETURN_NOT_OK(rolling_writer->Close());
    write_guard.Release();

    auto before = AbstractCompactRewriter::ExtractFilesFromSections(sections);
    NotifyRewriteCompactBefore(before);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> after,
                           rolling_writer->GetResult());

    after = NotifyRewriteCompactAfter(after);
    return CompactResult(before, after);
}

}  // namespace paimon
