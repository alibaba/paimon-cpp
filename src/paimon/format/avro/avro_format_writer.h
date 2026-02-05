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

#include <cstddef>
#include <cstdint>
#include <memory>

#include "arrow/api.h"
#include "avro/DataFile.hh"
#include "avro/ValidSchema.hh"
#include "paimon/format/avro/avro_direct_encoder.h"
#include "paimon/format/avro/avro_output_stream_impl.h"
#include "paimon/format/format_writer.h"
#include "paimon/metrics.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
class Schema;
}  // namespace arrow
namespace avro {
class GenericDatum;
}  // namespace avro
namespace paimon {
class Metrics;
}  // namespace paimon
struct ArrowArray;

namespace paimon::avro {

/// A `FormatWriter` implementation that writes data in Avro format.
class AvroFormatWriter : public FormatWriter {
 public:
    static Result<std::unique_ptr<AvroFormatWriter>> Create(
        std::unique_ptr<AvroOutputStreamImpl> out, const std::shared_ptr<arrow::Schema>& schema,
        const ::avro::Codec codec);

    Status AddBatch(ArrowArray* batch) override;

    Status Flush() override;

    Status Finish() override;

    Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size) const override;

    std::shared_ptr<Metrics> GetWriterMetrics() const override {
        return metrics_;
    }

 private:
    static constexpr size_t DEFAULT_SYNC_INTERVAL = 64 * 1024;

    AvroFormatWriter(std::unique_ptr<::avro::DataFileWriterBase>&& file_writer,
                     const ::avro::ValidSchema& avro_schema,
                     const std::shared_ptr<arrow::DataType>& data_type,
                     AvroOutputStreamImpl* avro_output_stream);

    std::unique_ptr<::avro::DataFileWriterBase> writer_;
    ::avro::ValidSchema avro_schema_;
    std::shared_ptr<arrow::DataType> data_type_;
    std::shared_ptr<Metrics> metrics_;
    AvroOutputStreamImpl* avro_output_stream_;
    // Encode context for reusing scratch buffers
    AvroDirectEncoder::EncodeContext encode_ctx_;
};

}  // namespace paimon::avro
