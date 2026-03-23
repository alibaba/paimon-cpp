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

#include "paimon/core/operation/append_only_file_store_write.h"

#include <atomic>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/core/append/append_only_writer.h"
#include "paimon/core/append/bucketed_append_compact_manager.h"
#include "paimon/core/compact/noop_compact_manager.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_file_writer.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/operation/restore_files.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/executor.h"
#include "paimon/logging.h"
#include "paimon/read_context.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class DataFilePathFactory;
class MemoryPool;
class SchemaManager;

AppendOnlyFileStoreWrite::AppendOnlyFileStoreWrite(
    const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager, const std::string& commit_user,
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema,
    const std::shared_ptr<BucketedDvMaintainer::Factory>& dv_maintainer_factory,
    const CoreOptions& options, bool ignore_previous_files, bool is_streaming_mode,
    bool ignore_num_bucket_check, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool)
    : AbstractFileStoreWrite(file_store_path_factory, snapshot_manager, schema_manager, commit_user,
                             root_path, table_schema, schema, write_schema, partition_schema,
                             dv_maintainer_factory, options, ignore_previous_files,
                             is_streaming_mode, ignore_num_bucket_check, executor, pool),
      logger_(Logger::GetLogger("AppendOnlyFileStoreWrite")) {
    write_cols_ = write_schema->field_names();
    auto schemas = BlobUtils::SeparateBlobSchema(schema_);
    if (schemas.blob_schema && schemas.blob_schema->num_fields() > 0) {
        with_blob_ = true;
    }
    // optimize write_cols to null in following cases:
    // 1. write_schema contains all columns
    // 2. TODO(xinyu.lxy) write_schema contains all columns and append _ROW_ID & _SEQUENCE_NUMBER
    // cols
    if (schema->Equals(write_schema)) {
        write_cols_ = std::nullopt;
    }
}

AppendOnlyFileStoreWrite::~AppendOnlyFileStoreWrite() = default;

Result<std::unique_ptr<FileStoreScan>> AppendOnlyFileStoreWrite::CreateFileStoreScan(
    const std::shared_ptr<ScanFilter>& scan_filter) const {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(options_.GetFileSystem(), options_.GetManifestFormat(),
                             options_.GetManifestCompression(), file_store_path_factory_, pool_));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(options_.GetFileSystem(), options_.GetManifestFormat(),
                             options_.GetManifestCompression(), file_store_path_factory_,
                             options_.GetManifestTargetFileSize(), pool_, options_,
                             partition_schema_));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan,
                           AppendOnlyFileStoreScan::Create(
                               snapshot_manager_, schema_manager_, manifest_list, manifest_file,
                               table_schema_, schema_, scan_filter, options_, executor_, pool_));
    return scan;
}

Result<std::vector<std::shared_ptr<DataFileMeta>>> AppendOnlyFileStoreWrite::CompactRewrite(
    const BinaryRow& partition, int32_t bucket, DeletionVector::Factory dv_factory,
    const std::vector<std::shared_ptr<DataFileMeta>>& to_compact,
    const std::shared_ptr<std::atomic_bool>& cancel_flag) {
    if (to_compact.empty()) {
        return std::vector<std::shared_ptr<DataFileMeta>>{};
    }

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                           CreateFilesReader(partition, bucket, dv_factory, to_compact));
    auto rewriter =
        std::make_unique<RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
            options_.GetTargetFileSize(/*has_primary_key=*/false),
            GetDataFileWriterCreator(partition, bucket, write_schema_, write_cols_, to_compact));

    ScopeGuard reader_guard([&]() {
        if (reader) {
            reader->Close();
        }
    });

    ScopeGuard rewriter_guard([&]() {
        if (rewriter) {
            (void)rewriter->Close();
        }
    });

    while (true) {
        if (cancel_flag->load(std::memory_order_relaxed)) {
            return Status::Cancelled("Compaction cancelled while rewriting files.");
        }
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
        if (!struct_array) {
            return Status::Invalid(
                "cannot cast array to StructArray in CompleteRowKindBatchReader");
        }
        PAIMON_ASSIGN_OR_RAISE(struct_array, ArrowUtils::RemoveFieldFromStructArray(
                                                 struct_array, SpecialFields::ValueKind().Name()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportArray(*struct_array, c_array.get(), c_schema.get()));
        ArrowSchemaRelease(c_schema.get());
        ScopeGuard guard([array = c_array.get()]() { ArrowArrayRelease(array); });
        PAIMON_RETURN_NOT_OK(rewriter->Write(c_array.get()));
        guard.Release();
    }
    rewriter_guard.Release();
    PAIMON_RETURN_NOT_OK(rewriter->Close());
    return rewriter->GetResult();
}

Result<std::shared_ptr<BatchWriter>> AppendOnlyFileStoreWrite::CreateWriter(
    const BinaryRow& partition, int32_t bucket,
    const std::vector<std::shared_ptr<DataFileMeta>>& restore_data_files,
    int64_t restore_max_seq_number, const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer) {
    PAIMON_LOG_DEBUG(logger_, "Creating append only writer for partition %s, bucket %d",
                     partition.ToString().c_str(), bucket);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                           file_store_path_factory_->CreateDataFilePathFactory(partition, bucket));

    std::shared_ptr<CompactManager> compact_manager;
    auto schemas = BlobUtils::SeparateBlobSchema(write_schema_);
    if (options_.WriteOnly() || options_.DataEvolutionEnabled() || options_.GetBucket() == -1 ||
        with_blob_) {
        compact_manager = std::make_shared<NoopCompactManager>();
    } else {
        auto dv_factory =
            [dv_maintainer](
                const std::string& file_name) -> Result<std::shared_ptr<DeletionVector>> {
            if (dv_maintainer) {
                return dv_maintainer->DeletionVectorOf(file_name).value_or(
                    std::shared_ptr<DeletionVector>());
            }
            return std::shared_ptr<DeletionVector>();
        };
        auto cancel_flag = std::make_shared<std::atomic_bool>(false);

        auto rewriter = [this, partition, bucket, dv_factory,
                         cancel_flag](const std::vector<std::shared_ptr<DataFileMeta>>& to_compact)
            -> Result<std::vector<std::shared_ptr<DataFileMeta>>> {
            return CompactRewrite(partition, bucket, dv_factory, to_compact, cancel_flag);
        };

        compact_manager = std::make_shared<BucketedAppendCompactManager>(
            compact_executor_, restore_data_files, dv_maintainer,
            options_.GetCompactionMinFileNum(),
            options_.GetTargetFileSize(/*has_primary_key=*/false),
            options_.GetCompactionFileSize(/*has_primary_key=*/false),
            options_.CompactionForceRewriteAllFiles(), rewriter,
            compaction_metrics_->CreateReporter(partition, bucket), cancel_flag);
    }

    auto writer = std::make_shared<AppendOnlyWriter>(
        options_, table_schema_->Id(), write_schema_, write_cols_, restore_max_seq_number,
        data_file_path_factory, compact_manager, pool_);
    return std::shared_ptr<BatchWriter>(writer);
}

AppendOnlyFileStoreWrite::SingleFileWriterCreator
AppendOnlyFileStoreWrite::GetDataFileWriterCreator(
    const BinaryRow& partition, int32_t bucket, const std::shared_ptr<arrow::Schema>& schema,
    const std::optional<std::vector<std::string>>& write_cols,
    const std::vector<std::shared_ptr<DataFileMeta>>& to_compact) const {
    return
        [this, partition, bucket, schema, write_cols, to_compact]()
            -> Result<
                std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>> {
            ::ArrowSchema arrow_schema;
            ScopeGuard guard([&arrow_schema]() { ArrowSchemaRelease(&arrow_schema); });
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &arrow_schema));
            auto format = options_.GetFileFormat();
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<WriterBuilder> writer_builder,
                format->CreateWriterBuilder(&arrow_schema, options_.GetWriteBatchSize()));
            writer_builder->WithMemoryPool(pool_);

            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &arrow_schema));
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatStatsExtractor> stats_extractor,
                                   format->CreateStatsExtractor(&arrow_schema));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                file_store_path_factory_->CreateDataFilePathFactory(partition, bucket));
            auto writer = std::make_unique<DataFileWriter>(
                options_.GetFileCompression(), std::function<Status(ArrowArray*, ArrowArray*)>(),
                table_schema_->Id(),
                std::make_shared<LongCounter>(to_compact[0]->min_sequence_number),
                FileSource::Compact(), stats_extractor, data_file_path_factory->IsExternalPath(),
                write_cols, pool_);
            PAIMON_RETURN_NOT_OK(writer->Init(options_.GetFileSystem(),
                                              data_file_path_factory->NewPath(), writer_builder));
            return writer;
        };
}

Result<std::unique_ptr<BatchReader>> AppendOnlyFileStoreWrite::CreateFilesReader(
    const BinaryRow& partition, int32_t bucket, DeletionVector::Factory dv_factory,
    const std::vector<std::shared_ptr<DataFileMeta>>& files) const {
    ReadContextBuilder context_builder(root_path_);
    context_builder.EnablePrefetch(true).SetPrefetchMaxParallelNum(1);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
    std::map<std::string, std::string> map = options_.ToMap();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> internal_read_context,
                           InternalReadContext::Create(read_context, table_schema_, map));
    auto read = std::make_unique<RawFileSplitRead>(file_store_path_factory_, internal_read_context,
                                                   pool_, compact_executor_);

    return read->CreateReader(partition, bucket, files, dv_factory);
}

}  // namespace paimon
