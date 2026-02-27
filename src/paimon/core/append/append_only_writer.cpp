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
#include <string>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/type.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/long_counter.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_file_writer.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/io/rolling_blob_file_writer.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/writer_builder.h"
#include "paimon/macros.h"
#include "paimon/metrics.h"
#include "paimon/record_batch.h"

namespace paimon {

class MemoryPool;
class FormatStatsExtractor;

AppendOnlyWriter::AppendOnlyWriter(const CoreOptions& options, int64_t schema_id,
                                   const std::shared_ptr<arrow::Schema>& write_schema,
                                   const std::optional<std::vector<std::string>>& write_cols,
                                   int64_t max_sequence_number,
                                   const std::shared_ptr<DataFilePathFactory>& path_factory,
                                   const std::shared_ptr<MemoryPool>& memory_pool)
    : options_(options),
      schema_id_(schema_id),
      write_schema_(write_schema),
      write_cols_(write_cols),
      seq_num_counter_(std::make_shared<LongCounter>(max_sequence_number + 1)),
      path_factory_(path_factory),
      memory_pool_(memory_pool),
      metrics_(std::make_shared<MetricsImpl>()) {}

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
    return writer_->Write(batch->GetData());
}

Result<CommitIncrement> AppendOnlyWriter::PrepareCommit(bool wait_compaction) {
    PAIMON_RETURN_NOT_OK(Flush());
    return DrainIncrement();
}

Result<CommitIncrement> AppendOnlyWriter::DrainIncrement() {
    DataIncrement data_increment(std::move(new_files_), std::move(deleted_files_), {});
    CompactIncrement compact_increment({}, {}, {});
    new_files_.clear();
    deleted_files_.clear();
    return CommitIncrement(data_increment, compact_increment);
}

Status AppendOnlyWriter::Flush() {
    if (writer_) {
        PAIMON_RETURN_NOT_OK(writer_->Close());
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> flushed_files,
                               writer_->GetResult());
        new_files_.insert(new_files_.end(), flushed_files.begin(), flushed_files.end());
        metrics_->Merge(writer_->GetMetrics());
        writer_.reset();
    }
    return Status::OK();
}

AppendOnlyWriter::RollingFileWriterResult AppendOnlyWriter::CreateRollingRowWriter() const {
    auto schemas = BlobUtils::SeparateBlobSchema(write_schema_);
    if (schemas.blob_schema && schemas.blob_schema->num_fields() > 0) {
        return CreateRollingBlobWriter(schemas);
    } else {
        return std::make_unique<RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
            options_.GetTargetFileSize(/*has_primary_key=*/false),
            GetDataFileWriterCreator(write_schema_, write_cols_));
    }
}

AppendOnlyWriter::SingleFileWriterCreator AppendOnlyWriter::GetDataFileWriterCreator(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::optional<std::vector<std::string>>& write_cols) const {
    return
        [this, schema, write_cols]()
            -> Result<
                std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>> {
            ::ArrowSchema arrow_schema;
            ScopeGuard guard([&arrow_schema]() { ArrowSchemaRelease(&arrow_schema); });
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &arrow_schema));
            auto format = options_.GetWriteFileFormat();
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<WriterBuilder> writer_builder,
                format->CreateWriterBuilder(&arrow_schema, options_.GetWriteBatchSize()));
            writer_builder->WithMemoryPool(memory_pool_);

            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &arrow_schema));
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatStatsExtractor> stats_extractor,
                                   format->CreateStatsExtractor(&arrow_schema));
            auto writer = std::make_unique<DataFileWriter>(
                options_.GetFileCompression(), std::function<Status(ArrowArray*, ArrowArray*)>(),
                schema_id_, seq_num_counter_, FileSource::Append(), stats_extractor,
                path_factory_->IsExternalPath(), write_cols, memory_pool_);
            PAIMON_RETURN_NOT_OK(
                writer->Init(options_.GetFileSystem(), path_factory_->NewPath(), writer_builder));
            return writer;
        };
}

AppendOnlyWriter::SingleFileWriterCreator AppendOnlyWriter::GetBlobFileWriterCreator(
    const std::shared_ptr<WriterBuilder>& writer_builder,
    const std::shared_ptr<FormatStatsExtractor>& stats_extractor,
    const std::optional<std::vector<std::string>>& write_cols) const {
    return
        [this, writer_builder, stats_extractor, write_cols]()
            -> Result<
                std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>> {
            auto writer = std::make_unique<DataFileWriter>(
                /*compression=*/"none", std::function<Status(ArrowArray*, ArrowArray*)>(),
                schema_id_, seq_num_counter_, FileSource::Append(), stats_extractor,
                path_factory_->IsExternalPath(), write_cols, memory_pool_);
            PAIMON_RETURN_NOT_OK(writer->Init(options_.GetFileSystem(),
                                              path_factory_->NewBlobPath(), writer_builder));
            return writer;
        };
}

AppendOnlyWriter::RollingFileWriterResult AppendOnlyWriter::CreateRollingBlobWriter(
    const BlobUtils::SeparatedSchemas& schemas) const {
    if (schemas.blob_schema->num_fields() > RollingBlobFileWriter::EXPECTED_BLOB_FIELD_COUNT) {
        return Status::Invalid("Limit exactly one blob field in one paimon table yet.");
    }
    // use a specialized writer that writes blob data to a separate rolling file.
    ::ArrowSchema arrow_schema;
    ScopeGuard guard([&arrow_schema]() { ArrowSchemaRelease(&arrow_schema); });
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schemas.blob_schema, &arrow_schema));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileFormat> format,
                           FileFormatFactory::Get("blob", options_.ToMap()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<WriterBuilder> writer_builder,
        format->CreateWriterBuilder(&arrow_schema, options_.GetWriteBatchSize()));
    writer_builder->WithMemoryPool(memory_pool_);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schemas.blob_schema, &arrow_schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatStatsExtractor> stats_extractor,
                           format->CreateStatsExtractor(&arrow_schema));

    auto single_blob_file_writer_creator = GetBlobFileWriterCreator(
        writer_builder, stats_extractor, schemas.blob_schema->field_names());
    auto rolling_blob_file_writer_creator = [this, single_blob_file_writer_creator]()
        -> Result<
            std::unique_ptr<RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>> {
        return std::make_unique<RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
            options_.GetBlobTargetFileSize(), single_blob_file_writer_creator);
    };
    return std::make_unique<RollingBlobFileWriter>(
        options_.GetTargetFileSize(/*has_primary_key=*/false),
        GetDataFileWriterCreator(schemas.main_schema, schemas.main_schema->field_names()),
        rolling_blob_file_writer_creator, arrow::struct_(write_schema_->fields()));
}

Status AppendOnlyWriter::Close() {
    if (writer_) {
        writer_->Abort();
        writer_.reset();
    }
    return Status::OK();
}

}  // namespace paimon
