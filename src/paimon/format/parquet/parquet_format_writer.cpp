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

#include "paimon/format/parquet/parquet_format_writer.h"

#include <string>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_output_stream_impl.h"
#include "paimon/metrics.h"
#include "parquet/arrow/writer.h"
#include "parquet/properties.h"

namespace arrow {
class MemoryPool;
class Schema;
}  // namespace arrow
namespace paimon {
class OutputStream;
}  // namespace paimon
struct ArrowArray;

namespace paimon::parquet {

Result<std::unique_ptr<ParquetFormatWriter>> ParquetFormatWriter::Create(
    const std::shared_ptr<OutputStream>& output_stream,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<::parquet::WriterProperties>& writer_properties, uint64_t max_memory_use,
    const std::shared_ptr<arrow::MemoryPool>& pool) {
    auto out = std::make_shared<ParquetOutputStreamImpl>(output_stream);
    ::parquet::ArrowWriterProperties::Builder arrow_properties_builder;
    auto arrow_writer_properties =
        arrow_properties_builder.enable_deprecated_int96_timestamps()->build();
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::unique_ptr<::parquet::arrow::FileWriter> file_writer,
        ::parquet::arrow::FileWriter::Open(*schema, pool.get(), out, writer_properties,
                                           arrow_writer_properties));
    return std::unique_ptr<ParquetFormatWriter>(
        new ParquetFormatWriter(std::move(file_writer), out, schema, max_memory_use, pool));
}

Status ParquetFormatWriter::AddBatch(ArrowArray* batch) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<::arrow::RecordBatch> record_batch,
                                      arrow::ImportRecordBatch(batch, schema_));
    if (static_cast<uint64_t>(pool_->bytes_allocated()) > max_memory_use_) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(writer_->NewBufferedRowGroup());
    }
    PAIMON_RETURN_NOT_OK_FROM_ARROW(writer_->WriteRecordBatch(*record_batch));
    total_records_written_ += (*record_batch).num_rows();
    return Status::OK();
}

Status ParquetFormatWriter::Flush() {
    metrics_->SetCounter(ParquetMetrics::WRITE_RECORD_COUNT, total_records_written_);
    return Status::OK();
}

Status ParquetFormatWriter::Finish() {
    PAIMON_RETURN_NOT_OK(Flush());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(writer_->Close());
    return Status::OK();
}

Result<bool> ParquetFormatWriter::ReachTargetSize(bool suggested_check, int64_t target_size) const {
    if (suggested_check) {
        PAIMON_ASSIGN_OR_RAISE(const uint64_t length, GetEstimateLength());
        return length >= static_cast<uint64_t>(target_size);
    }
    return false;
}

Result<uint64_t> ParquetFormatWriter::GetEstimateLength() const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(int64_t written_bytes, out_->Tell());
    return writer_->GetBufferedSize() + written_bytes;
}

ParquetFormatWriter::ParquetFormatWriter(std::unique_ptr<::parquet::arrow::FileWriter> writer,
                                         const std::shared_ptr<ParquetOutputStreamImpl>& out,
                                         const std::shared_ptr<arrow::Schema>& schema,
                                         uint64_t max_memory_use,
                                         const std::shared_ptr<arrow::MemoryPool>& pool)
    : pool_(pool),
      out_(out),
      writer_(std::move(writer)),
      schema_(schema),
      metrics_(std::make_shared<MetricsImpl>()),
      max_memory_use_(max_memory_use) {}

}  // namespace paimon::parquet
