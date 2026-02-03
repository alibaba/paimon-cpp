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

#include "paimon/format/avro/avro_format_writer.h"

#include <cassert>
#include <exception>
#include <memory>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "avro/Compiler.hh"  // IWYU pragma: keep
#include "avro/DataFile.hh"
#include "avro/Exception.hh"
#include "avro/Generic.hh"   // IWYU pragma: keep
#include "avro/Specific.hh"  // IWYU pragma: keep
#include "avro/ValidSchema.hh"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/avro/avro_schema_converter.h"

namespace arrow {
class Array;
}  // namespace arrow
struct ArrowArray;

namespace paimon::avro {

AvroFormatWriter::AvroFormatWriter(std::unique_ptr<::avro::DataFileWriterBase>&& file_writer,
                                   const ::avro::ValidSchema& avro_schema,
                                   const std::shared_ptr<arrow::DataType>& data_type,
                                   AvroOutputStreamImpl* avro_output_stream)
    : writer_(std::move(file_writer)),
      avro_schema_(avro_schema),
      data_type_(data_type),
      metrics_(std::make_shared<MetricsImpl>()),
      avro_output_stream_(avro_output_stream) {}

Result<std::unique_ptr<AvroFormatWriter>> AvroFormatWriter::Create(
    std::unique_ptr<AvroOutputStreamImpl> out, const std::shared_ptr<arrow::Schema>& schema,
    const ::avro::Codec codec) {
    try {
        PAIMON_ASSIGN_OR_RAISE(::avro::ValidSchema avro_schema,
                               AvroSchemaConverter::ArrowSchemaToAvroSchema(schema));
        AvroOutputStreamImpl* avro_output_stream = out.get();
        auto writer = std::make_unique<::avro::DataFileWriterBase>(std::move(out), avro_schema,
                                                                   DEFAULT_SYNC_INTERVAL, codec);
        auto data_type = arrow::struct_(schema->fields());
        return std::unique_ptr<AvroFormatWriter>(
            new AvroFormatWriter(std::move(writer), avro_schema, data_type, avro_output_stream));
    } catch (const ::avro::Exception& e) {
        return Status::Invalid(fmt::format("avro format writer create failed. {}", e.what()));
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("avro format writer create failed: {}", e.what()));
    } catch (...) {
        return Status::Invalid("avro format writer create failed: unknown exception");
    }
}

Status AvroFormatWriter::Flush() {
    try {
        writer_->flush();
    } catch (const ::avro::Exception& e) {
        return Status::Invalid(fmt::format("avro writer flush failed. {}", e.what()));
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("avro writer flush failed: {}", e.what()));
    } catch (...) {
        return Status::Invalid("avro writer flush failed: unknown exception");
    }

    return Status::OK();
}

Status AvroFormatWriter::Finish() {
    try {
        avro_output_stream_->FlushBuffer();  // we need flush buffer before close writer
        writer_->close();
    } catch (const ::avro::Exception& e) {
        return Status::Invalid(fmt::format("avro writer close failed. {}", e.what()));
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("avro writer close failed: {}", e.what()));
    } catch (...) {
        return Status::Invalid("avro writer close failed: unknown exception");
    }
    return Status::OK();
}

Result<bool> AvroFormatWriter::ReachTargetSize(bool suggested_check, int64_t target_size) const {
    if (suggested_check) {
        uint64_t current_size = writer_->getCurrentBlockStart();
        return current_size >= static_cast<uint64_t>(target_size);
    }
    return false;
}

Status AvroFormatWriter::AddBatch(ArrowArray* batch) {
    assert(batch);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(batch, data_type_));
    try {
        for (int64_t row_index = 0; row_index < arrow_array->length(); ++row_index) {
            writer_->syncIfNeeded();
            PAIMON_RETURN_NOT_OK(AvroDirectEncoder::EncodeArrowToAvro(
                avro_schema_.root(), *arrow_array, row_index, &writer_->encoder(), &encode_ctx_));
            writer_->incr();
        }
    } catch (const ::avro::Exception& e) {
        return Status::Invalid(fmt::format("avro writer add batch failed. {}", e.what()));
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("avro writer add batch failed: {}", e.what()));
    } catch (...) {
        return Status::Invalid("avro writer add batch failed: unknown exception");
    }
    PAIMON_RETURN_NOT_OK(Flush());
    return Status::OK();
}

}  // namespace paimon::avro
