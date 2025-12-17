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

#include "paimon/format/avro/avro_file_batch_reader.h"

#include <memory>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "avro/Generic.hh"
#include "avro/GenericDatum.hh"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/avro/avro_schema_converter.h"
#include "paimon/reader/batch_reader.h"

namespace paimon::avro {

AvroFileBatchReader::AvroFileBatchReader(
    std::unique_ptr<::avro::DataFileReader<::avro::GenericDatum>>&& reader,
    std::unique_ptr<AvroRecordConverter>&& record_converter, int32_t batch_size)
    : reader_(std::move(reader)),
      record_converter_(std::move(record_converter)),
      batch_size_(batch_size) {}

AvroFileBatchReader::~AvroFileBatchReader() {
    DoClose();
}

void AvroFileBatchReader::DoClose() {
    if (!close_) {
        reader_->close();
        close_ = true;
    }
}

Result<std::unique_ptr<AvroFileBatchReader>> AvroFileBatchReader::Create(
    std::unique_ptr<::avro::DataFileReader<::avro::GenericDatum>>&& reader, int32_t batch_size,
    const std::shared_ptr<MemoryPool>& pool) {
    if (batch_size <= 0) {
        return Status::Invalid(
            fmt::format("invalid batch size {}, must be larger than 0", batch_size));
    }
    const auto& avro_read_schema = reader->readerSchema();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<::arrow::DataType> arrow_data_type,
                           AvroSchemaConverter::AvroSchemaToArrowDataType(avro_read_schema));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<AvroRecordConverter> record_converter,
                           AvroRecordConverter::Create(arrow_data_type, pool));
    return std::unique_ptr<AvroFileBatchReader>(
        new AvroFileBatchReader(std::move(reader), std::move(record_converter), batch_size));
}

Result<BatchReader::ReadBatch> AvroFileBatchReader::NextBatch() {
    std::vector<::avro::GenericDatum> datums;
    datums.reserve(batch_size_);
    try {
        for (int32_t i = 0; i < batch_size_; i++) {
            ::avro::GenericDatum datum(reader_->readerSchema());
            if (!reader_->read(datum)) {
                // reach eof
                break;
            }
            if (datum.type() != ::avro::AVRO_RECORD) {
                return Status::Invalid(
                    fmt::format("avro reader next batch failed. invalid datum type: {}",
                                ::avro::toString(datum.type())));
            }
            datums.emplace_back(datum);
        }
        if (datums.empty()) {
            return BatchReader::MakeEofBatch();
        }
        // TODO(jinli.zjw) when support SetReadSchema(), may need convert file timestamp (milli) to
        // target read type timestamp(second)
        return record_converter_->NextBatch(datums);
    } catch (const ::avro::Exception& e) {
        return Status::Invalid(fmt::format("avro reader next batch failed. {}", e.what()));
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("avro reader next batch failed. {}", e.what()));
    } catch (...) {
        return Status::Invalid("avro reader next batch failed. unknown error");
    }
}

Status AvroFileBatchReader::SetReadSchema(::ArrowSchema* read_schema,
                                          const std::shared_ptr<Predicate>& predicate,
                                          const std::optional<RoaringBitmap32>& selection_bitmap) {
    assert(false);
    return Status::NotImplemented("avro reader not support set read schema");
}

Result<std::unique_ptr<::ArrowSchema>> AvroFileBatchReader::GetFileSchema() const {
    assert(reader_);
    try {
        const auto& avro_file_schema = reader_->dataSchema();
        bool nullable = false;
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::DataType> arrow_file_type,
            AvroSchemaConverter::GetArrowType(avro_file_schema.root(), &nullable));
        auto c_schema = std::make_unique<::ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*arrow_file_type, c_schema.get()));
        return c_schema;
    } catch (const ::avro::Exception& e) {
        return Status::Invalid(fmt::format("get file schema failed. {}", e.what()));
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("get file schema batch failed. {}", e.what()));
    } catch (...) {
        return Status::Invalid("get file schema failed. unknown error");
    }
}

}  // namespace paimon::avro
