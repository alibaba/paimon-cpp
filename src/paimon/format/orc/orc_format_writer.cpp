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

#include "paimon/format/orc/orc_format_writer.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <exception>
#include <optional>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "orc/Common.hh"
#include "orc/OrcFile.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/Writer.hh"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/format/orc/orc_adapter.h"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/format/orc/orc_memory_pool.h"
#include "paimon/format/orc/orc_metrics.h"
#include "paimon/macros.h"

namespace paimon {
class MemoryPool;
}  // namespace paimon
struct ArrowArray;

namespace paimon::orc {

OrcFormatWriter::OrcFormatWriter(const std::shared_ptr<OrcMemoryPool>& orc_memory_pool,
                                 std::unique_ptr<::orc::OutputStream>&& output_stream,
                                 std::unique_ptr<::orc::WriterMetrics>&& writer_metrics,
                                 std::unique_ptr<::orc::Writer>&& writer,
                                 std::unique_ptr<::orc::ColumnVectorBatch>&& orc_batch,
                                 std::unique_ptr<::orc::Type>&& orc_type,
                                 const ::orc::WriterOptions& writer_options,
                                 const std::shared_ptr<arrow::DataType>& data_type)
    : orc_memory_pool_(orc_memory_pool),
      output_stream_(std::move(output_stream)),
      writer_metrics_(std::move(writer_metrics)),
      writer_(std::move(writer)),
      orc_batch_(std::move(orc_batch)),
      orc_type_(std::move(orc_type)),
      writer_options_(writer_options),
      data_type_(data_type),
      metrics_(std::make_shared<MetricsImpl>()) {}

Result<std::unique_ptr<OrcFormatWriter>> OrcFormatWriter::Create(
    std::unique_ptr<::orc::OutputStream>&& output_stream, const arrow::Schema& schema,
    const std::map<std::string, std::string>& options, const std::string& compression,
    int32_t batch_size, const std::shared_ptr<MemoryPool>& pool) {
    assert(output_stream);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::orc::Type> orc_type, OrcAdapter::GetOrcType(schema));
    auto data_type = arrow::struct_(schema.fields());
    try {
        PAIMON_ASSIGN_OR_RAISE(::orc::WriterOptions writer_options,
                               PrepareWriterOptions(options, compression, data_type));
        std::shared_ptr<OrcMemoryPool> orc_memory_pool;
        if (pool) {
            orc_memory_pool = std::make_shared<OrcMemoryPool>(pool);
            writer_options.setMemoryPool(orc_memory_pool.get());
        }

        std::unique_ptr<::orc::WriterMetrics> writer_metrics;
        PAIMON_ASSIGN_OR_RAISE(
            bool write_enable_metrics,
            OptionsUtils::GetValueFromMap<bool>(options, ORC_WRITE_ENABLE_METRICS, false));
        if (write_enable_metrics) {
            writer_metrics = std::make_unique<::orc::WriterMetrics>();
            writer_options.setWriterMetrics(writer_metrics.get());
        }
        std::unique_ptr<::orc::Writer> writer =
            ::orc::createWriter(*orc_type, output_stream.get(), writer_options);
        assert(writer);
        std::unique_ptr<::orc::ColumnVectorBatch> orc_batch = writer->createRowBatch(batch_size);
        return std::unique_ptr<OrcFormatWriter>(new OrcFormatWriter(
            orc_memory_pool, std::move(output_stream), std::move(writer_metrics), std::move(writer),
            std::move(orc_batch), std::move(orc_type), writer_options, data_type));
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("create orc format writer failed for file {}, with {} error",
                        output_stream->getName(), e.what()));
    } catch (...) {
        return Status::UnknownError(
            fmt::format("create orc format writer failed for file {}, with unknown error",
                        output_stream->getName()));
    }
}

Status OrcFormatWriter::ExpandBatch(uint64_t expect_size) {
    try {
        orc_batch_.reset(writer_->createRowBatch(expect_size).release());
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("expand orc batch to {} failed for file {}, with {} error", expect_size,
                        output_stream_->getName(), e.what()));
    } catch (...) {
        return Status::UnknownError(
            fmt::format("expand orc batch to {} failed for file {}, with unknown error",
                        expect_size, output_stream_->getName()));
    }
}

Status OrcFormatWriter::AddBatch(ArrowArray* batch) {
    assert(batch);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(batch, data_type_));
    if (PAIMON_UNLIKELY(static_cast<uint64_t>(arrow_array->length()) > orc_batch_->capacity)) {
        PAIMON_RETURN_NOT_OK(ExpandBatch(arrow_array->length()));
    }
    PAIMON_RETURN_NOT_OK(OrcAdapter::WriteBatch(arrow_array, orc_batch_.get()));
    assert(orc_batch_->numElements == static_cast<uint64_t>(arrow_array->length()));
    PAIMON_RETURN_NOT_OK(Flush());
    return Status::OK();
}

Status OrcFormatWriter::Flush() {
    try {
        if (orc_batch_->numElements > 0) {
            writer_->add(*orc_batch_);
        }
        orc_batch_->clear();
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("orc format writer flush failed for file {}, with {} error",
                        output_stream_->getName(), e.what()));
    } catch (...) {
        return Status::UnknownError(
            fmt::format("orc format writer flush failed for file {}, with unknown error",
                        output_stream_->getName()));
    }
    return Status::OK();
}

Status OrcFormatWriter::Finish() {
    PAIMON_RETURN_NOT_OK(Flush());
    try {
        metrics_ = GetWriterMetrics();
        orc_batch_.reset();
        writer_->close();
        writer_.reset();
        writer_metrics_.reset();
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("orc format writer finish failed for file {}, with {} error",
                        output_stream_->getName(), e.what()));
    } catch (...) {
        return Status::UnknownError(
            fmt::format("orc format writer finish failed for file {}, with unknown error",
                        output_stream_->getName()));
    }
    return Status::OK();
}

Result<bool> OrcFormatWriter::ReachTargetSize(bool suggested_check, int64_t target_size) const {
    if (suggested_check) {
        PAIMON_ASSIGN_OR_RAISE(uint64_t length, GetEstimateLength());
        return length >= static_cast<uint64_t>(target_size);
    }
    return false;
}

Result<uint64_t> OrcFormatWriter::GetEstimateLength() const {
    try {
        return output_stream_->getLength() + writer_options_.getStripeSize();
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format(
            "orc format writer get estimated file size failed for file {}, with {} error",
            output_stream_->getName(), e.what()));
    } catch (...) {
        return Status::UnknownError(fmt::format(
            "orc format writer get estimated file size failed for file {}, with unknown error",
            output_stream_->getName()));
    }
}

std::shared_ptr<Metrics> OrcFormatWriter::GetWriterMetrics() const {
    if (writer_metrics_) {
        metrics_->SetCounter(OrcMetrics::WRITE_IO_COUNT, writer_metrics_->IOCount);
    }
    return metrics_;
}

Result<::orc::WriterOptions> OrcFormatWriter::PrepareWriterOptions(
    const std::map<std::string, std::string>& options, const std::string& file_compression,
    const std::shared_ptr<arrow::DataType>& data_type) {
    if (ArrowSchemaValidator::ContainTimestampWithTimezone(*data_type)) {
        PAIMON_ASSIGN_OR_RAISE(bool ltz_legacy, OptionsUtils::GetValueFromMap<bool>(
                                                    options, ORC_TIMESTAMP_LTZ_LEGACY_TYPE, true));
        if (ltz_legacy) {
            return Status::Invalid(
                "invalid config, do not support writing timestamp with timezone in legacy format "
                "for orc");
        }
    }
    ::orc::WriterOptions writer_options;
    PAIMON_ASSIGN_OR_RAISE(size_t stripe_size, OptionsUtils::GetValueFromMap<size_t>(
                                                   options, ORC_STRIPE_SIZE, DEFAULT_STRIPE_SIZE));
    writer_options.setStripeSize(stripe_size);
    PAIMON_ASSIGN_OR_RAISE(::orc::CompressionKind compression,
                           ToOrcCompressionKind(StringUtils::ToLowerCase(file_compression)));
    writer_options.setCompression(compression);
    PAIMON_ASSIGN_OR_RAISE(size_t compression_block_size, OptionsUtils::GetValueFromMap<size_t>(
                                                              options, ORC_COMPRESSION_BLOCK_SIZE,
                                                              DEFAULT_COMPRESSION_BLOCK_SIZE));
    writer_options.setCompressionBlockSize(compression_block_size);
    PAIMON_ASSIGN_OR_RAISE(
        double dictionary_key_threshold,
        OptionsUtils::GetValueFromMap<double>(options, ORC_DICTIONARY_KEY_SIZE_THRESHOLD,
                                              DEFAULT_DICTIONARY_KEY_SIZE_THRESHOLD));
    writer_options.setDictionaryKeySizeThreshold(dictionary_key_threshold);
    // always use tight numeric vector
    writer_options.setUseTightNumericVector(true);
    PAIMON_ASSIGN_OR_RAISE(size_t row_index_stride,
                           OptionsUtils::GetValueFromMap<size_t>(options, ORC_ROW_INDEX_STRIDE,
                                                                 DEFAULT_ROW_INDEX_STRIDE));
    writer_options.setRowIndexStride(row_index_stride);
    // In order to avoid issue like https://github.com/alibaba/paimon-cpp/issues/42, we explicitly
    // set GMT timezone.
    writer_options.setTimezoneName("GMT");
    return writer_options;
}

Result<::orc::CompressionKind> OrcFormatWriter::ToOrcCompressionKind(
    const std::string& file_compression) {
    if (file_compression == "zstd") {
        return ::orc::CompressionKind_ZSTD;
    } else if (file_compression == "lz4") {
        return ::orc::CompressionKind_LZ4;
    } else if (file_compression == "snappy") {
        return ::orc::CompressionKind_SNAPPY;
    } else if (file_compression == "zlib") {
        return ::orc::CompressionKind_ZLIB;
    } else if (file_compression == "lzo") {
        return ::orc::CompressionKind_LZO;
    } else {
        return Status::Invalid("unknown compression " + file_compression);
    }
}

}  // namespace paimon::orc
