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

#pragma once

#include <cstdint>
#include <memory>

#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/parquet/parquet_output_stream_impl.h"
#include "paimon/fs/file_system.h"
#include "paimon/metrics.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_writer.h"

namespace arrow {
class MemoryPool;
class Schema;
}  // namespace arrow
namespace paimon {
class Metrics;
class OutputStream;

namespace parquet {
class ParquetOutputStreamImpl;
}  // namespace parquet
}  // namespace paimon
namespace parquet {
class WriterProperties;
}  // namespace parquet
struct ArrowArray;

namespace paimon::parquet {

class ParquetFormatWriter : public FormatWriter {
 public:
    static Result<std::unique_ptr<ParquetFormatWriter>> Create(
        const std::shared_ptr<OutputStream>& output_stream,
        const std::shared_ptr<arrow::Schema>& schema,
        const std::shared_ptr<::parquet::WriterProperties>& writer_properties,
        uint64_t max_memory_use, const std::shared_ptr<arrow::MemoryPool>& pool);

    Status AddBatch(ArrowArray* batch) override;

    Status Flush() override;

    Status Finish() override;

    Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size) const override;

    std::shared_ptr<Metrics> GetWriterMetrics() const override {
        return metrics_;
    }

 private:
    ParquetFormatWriter(std::unique_ptr<::parquet::arrow::FileWriter> writer,
                        const std::shared_ptr<ParquetOutputStreamImpl>& out,
                        const std::shared_ptr<arrow::Schema>& schema, uint64_t max_memory_use,
                        const std::shared_ptr<arrow::MemoryPool>& pool);

    Result<uint64_t> GetEstimateLength() const;

    std::shared_ptr<arrow::MemoryPool> pool_;
    std::shared_ptr<ParquetOutputStreamImpl> out_;
    std::unique_ptr<::parquet::arrow::FileWriter> writer_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<Metrics> metrics_;
    int64_t total_records_written_ = 0;
    uint64_t max_memory_use_;
};

}  // namespace paimon::parquet
