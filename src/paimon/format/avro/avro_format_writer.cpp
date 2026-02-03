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
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "avro/Compiler.hh"  // IWYU pragma: keep
#include "avro/DataFile.hh"
#include "avro/Exception.hh"
#include "avro/Generic.hh"  // IWYU pragma: keep
#include "avro/GenericDatum.hh"
#include "avro/Specific.hh"  // IWYU pragma: keep
#include "avro/ValidSchema.hh"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/avro/avro_adaptor.h"
#include "paimon/format/avro/avro_schema_converter.h"

namespace arrow {
class Array;
}  // namespace arrow
struct ArrowArray;

namespace paimon::avro {

AvroFormatWriter::AvroFormatWriter(
    const std::shared_ptr<::avro::DataFileWriter<::avro::GenericDatum>>& file_writer,
    const ::avro::ValidSchema& avro_schema, const std::shared_ptr<arrow::DataType>& data_type,
    std::unique_ptr<AvroAdaptor> adaptor, AvroOutputStreamImpl* avro_output_stream)
    : writer_(file_writer),
      avro_schema_(avro_schema),
      data_type_(data_type),
      adaptor_(std::move(adaptor)),
      avro_output_stream_(avro_output_stream) {}

Result<std::unique_ptr<AvroFormatWriter>> AvroFormatWriter::Create(
    std::unique_ptr<AvroOutputStreamImpl> out, const std::shared_ptr<arrow::Schema>& schema,
    const ::avro::Codec codec) {
    try {
        PAIMON_ASSIGN_OR_RAISE(::avro::ValidSchema avro_schema,
                               AvroSchemaConverter::ArrowSchemaToAvroSchema(schema));
        AvroOutputStreamImpl* avro_output_stream = out.get();
        auto writer = std::make_shared<::avro::DataFileWriter<::avro::GenericDatum>>(
            std::move(out), avro_schema, DEFAULT_SYNC_INTERVAL, codec);
        auto data_type = arrow::struct_(schema->fields());
        auto adaptor = std::make_unique<AvroAdaptor>(data_type);
        return std::unique_ptr<AvroFormatWriter>(new AvroFormatWriter(
            writer, avro_schema, data_type, std::move(adaptor), avro_output_stream));
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
    return Status::NotImplemented("not support yet");
}

Status AvroFormatWriter::AddBatch(ArrowArray* batch) {
    assert(batch);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(batch, data_type_));
    PAIMON_ASSIGN_OR_RAISE(std::vector<::avro::GenericDatum> datums,
                           adaptor_->ConvertArrayToGenericDatums(arrow_array, avro_schema_));
    try {
        for (const auto& datum : datums) {
            writer_->write(datum);
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
