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
#include <map>
#include <memory>
#include <string>

#include "arrow/api.h"
#include "orc/Common.hh"
#include "orc/OrcFile.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/Writer.hh"
#include "paimon/format/format_writer.h"
#include "paimon/format/orc/orc_memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
class Schema;
}  // namespace arrow
namespace paimon {
class MemoryPool;
namespace orc {
class OrcMemoryPool;
}  // namespace orc
}  // namespace paimon
struct ArrowArray;

namespace paimon::orc {
/// A `FormatWriter` implementation that writes data in ORC format.
class OrcFormatWriter : public FormatWriter {
 public:
    static Result<std::unique_ptr<OrcFormatWriter>> Create(
        std::unique_ptr<::orc::OutputStream>&& output_stream, const arrow::Schema& schema,
        const std::map<std::string, std::string>& options, const std::string& compression,
        int32_t batch_size, const std::shared_ptr<MemoryPool>& pool);

    Status AddBatch(ArrowArray* batch) override;

    Status Flush() override;

    Status Finish() override;

    Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size) const override;

    std::shared_ptr<Metrics> GetWriterMetrics() const override;

    Status UpdateSchema(::ArrowSchema* schema) override;

 private:
    OrcFormatWriter(const std::shared_ptr<OrcMemoryPool>& orc_memory_pool,
                    std::unique_ptr<::orc::OutputStream>&& output_stream,
                    std::unique_ptr<::orc::WriterMetrics>&& writer_metrics,
                    std::unique_ptr<::orc::Writer>&& writer,
                    std::unique_ptr<::orc::ColumnVectorBatch>&& orc_batch,
                    std::unique_ptr<::orc::Type>&& orc_type,
                    const ::orc::WriterOptions& writer_options,
                    const std::shared_ptr<arrow::DataType>& data_type);

    Result<uint64_t> GetEstimateLength() const;
    Status ExpandBatch(uint64_t expect_size);

    static Result<::orc::WriterOptions> PrepareWriterOptions(
        const std::map<std::string, std::string>& options, const std::string& file_compression,
        const std::shared_ptr<arrow::DataType>& data_type);
    static Result<::orc::CompressionKind> ToOrcCompressionKind(const std::string& file_compression);

 private:
    std::shared_ptr<OrcMemoryPool> orc_memory_pool_;
    std::unique_ptr<::orc::OutputStream> output_stream_;
    std::unique_ptr<::orc::WriterMetrics> writer_metrics_;
    std::unique_ptr<::orc::Writer> writer_;
    std::unique_ptr<::orc::ColumnVectorBatch> orc_batch_;
    std::unique_ptr<::orc::Type> orc_type_;
    ::orc::WriterOptions writer_options_;
    std::shared_ptr<arrow::DataType> data_type_;
    std::shared_ptr<Metrics> metrics_;
};
}  // namespace paimon::orc
