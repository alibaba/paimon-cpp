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

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/avro/avro_input_stream_impl.h"
#include "paimon/format/avro/avro_schema_converter.h"
#include "paimon/reader/batch_reader.h"

namespace paimon::avro {

AvroFileBatchReader::AvroFileBatchReader(const std::shared_ptr<InputStream>& input_stream,
                                         const std::shared_ptr<::arrow::DataType>& file_data_type,
                                         std::unique_ptr<::avro::DataFileReaderBase>&& reader,
                                         std::unique_ptr<arrow::ArrayBuilder>&& array_builder,
                                         std::unique_ptr<arrow::MemoryPool>&& arrow_pool,
                                         int32_t batch_size,
                                         const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      arrow_pool_(std::move(arrow_pool)),
      input_stream_(input_stream),
      file_data_type_(file_data_type),
      reader_(std::move(reader)),
      array_builder_(std::move(array_builder)),
      batch_size_(batch_size),
      metrics_(std::make_shared<MetricsImpl>()) {}

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
    const std::shared_ptr<InputStream>& input_stream, int32_t batch_size,
    const std::shared_ptr<MemoryPool>& pool) {
    if (batch_size <= 0) {
        return Status::Invalid(
            fmt::format("invalid batch size {}, must be larger than 0", batch_size));
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::avro::DataFileReaderBase> reader,
                           CreateDataFileReader(input_stream, pool));
    const auto& avro_file_schema = reader->dataSchema();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<::arrow::DataType> file_data_type,
                           AvroSchemaConverter::AvroSchemaToArrowDataType(avro_file_schema));
    auto arrow_pool = GetArrowPool(pool);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> array_builder,
                                      arrow::MakeBuilder(file_data_type, arrow_pool.get()));
    return std::unique_ptr<AvroFileBatchReader>(
        new AvroFileBatchReader(input_stream, file_data_type, std::move(reader),
                                std::move(array_builder), std::move(arrow_pool), batch_size, pool));
}

Result<std::unique_ptr<::avro::DataFileReaderBase>> AvroFileBatchReader::CreateDataFileReader(
    const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_RETURN_NOT_OK(input_stream->Seek(0, SeekOrigin::FS_SEEK_SET));
    try {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::avro::InputStream> in,
                               AvroInputStreamImpl::Create(input_stream, BUFFER_SIZE, pool));
        auto reader = std::make_unique<::avro::DataFileReaderBase>(std::move(in));
        reader->init();
        return reader;
    } catch (const ::avro::Exception& e) {
        return Status::Invalid(fmt::format("build avro reader failed. {}", e.what()));
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("build avro reader failed. {}", e.what()));
    } catch (...) {
        return Status::Invalid("build avro reader failed. unknown error");
    }
}

Result<BatchReader::ReadBatch> AvroFileBatchReader::NextBatch() {
    if (next_row_to_read_ == std::numeric_limits<uint64_t>::max()) {
        next_row_to_read_ = 0;
    }
    try {
        while (array_builder_->length() < batch_size_) {
            if (!reader_->hasMore()) {
                break;
            }
            reader_->decr();
            PAIMON_RETURN_NOT_OK(AvroDirectDecoder::DecodeAvroToBuilder(
                reader_->dataSchema().root(), read_fields_projection_, &reader_->decoder(),
                array_builder_.get(), &decode_context_));
        }
        previous_first_row_ = next_row_to_read_;
        next_row_to_read_ += array_builder_->length();
        if (array_builder_->length() == 0) {
            return BatchReader::MakeEofBatch();
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          array_builder_->Finish());
        std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));
        return make_pair(std::move(c_array), std::move(c_schema));
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
    if (!read_schema) {
        return Status::Invalid("SetReadSchema failed: read schema cannot be nullptr");
    }
    // TODO(menglingda.mld): support predicate
    if (selection_bitmap) {
        // TODO(menglingda.mld): support bitmap
    }
    previous_first_row_ = std::numeric_limits<uint64_t>::max();
    next_row_to_read_ = std::numeric_limits<uint64_t>::max();
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_read_schema,
                                      arrow::ImportSchema(read_schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> file_schema,
                           ArrowUtils::DataTypeToSchema(file_data_type_));
    PAIMON_ASSIGN_OR_RAISE(read_fields_projection_,
                           CalculateReadFieldsProjection(file_schema, arrow_read_schema->fields()));
    array_builder_->Reset();
    std::shared_ptr<::arrow::DataType> read_data_type = arrow::struct_(arrow_read_schema->fields());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(array_builder_,
                                      arrow::MakeBuilder(read_data_type, arrow_pool_.get()));
    return Status::OK();
}

Result<std::set<size_t>> AvroFileBatchReader::CalculateReadFieldsProjection(
    const std::shared_ptr<::arrow::Schema>& file_schema, const arrow::FieldVector& read_fields) {
    std::set<size_t> projection_set;
    auto projection = ArrowUtils::CreateProjection(file_schema, read_fields);
    int32_t prev_index = -1;
    for (auto& index : projection) {
        if (index <= prev_index) {
            return Status::Invalid(
                "SetReadSchema failed: read schema fields order is different from file schema");
        }
        prev_index = index;
        projection_set.insert(index);
    }
    return projection_set;
}

Result<std::unique_ptr<::ArrowSchema>> AvroFileBatchReader::GetFileSchema() const {
    assert(reader_);
    auto c_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*file_data_type_, c_schema.get()));
    return c_schema;
}

}  // namespace paimon::avro
