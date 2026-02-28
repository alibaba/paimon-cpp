/*
 * Copyright 2026-present Alibaba Inc.
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

#include "paimon/common/file_index/rangebitmap/range_bitmap_file_index.h"

#include <arrow/c/bridge.h>
#include <arrow/type.h>

#include <limits>

#include "paimon/common/file_index/rangebitmap/range_bitmap.h"
#include "paimon/common/options/memory_size.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

RangeBitmapFileIndex::RangeBitmapFileIndex(const std::map<std::string, std::string>& options)
    : options_(options) {}

Result<std::shared_ptr<FileIndexReader>> RangeBitmapFileIndex::CreateReader(
    ::ArrowSchema* const arrow_schema, const int32_t start, const int32_t length,
    const std::shared_ptr<InputStream>& input_stream,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema_ptr,
                                      arrow::ImportSchema(arrow_schema));
    if (arrow_schema_ptr->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for RangeBitmapFileIndexReader, supposed to have single field.");
    }
    const auto arrow_type = arrow_schema_ptr->field(0)->type();
    return RangeBitmapFileIndexReader::Create(arrow_type, start, length, input_stream, pool);
}

Result<std::shared_ptr<FileIndexWriter>> RangeBitmapFileIndex::CreateWriter(
    ::ArrowSchema* arrow_schema, const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema_ptr,
                                      arrow::ImportSchema(arrow_schema));
    if (arrow_schema_ptr->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for RangeBitmapFileIndexWriter, supposed to have single field.");
    }
    const auto arrow_field = arrow_schema_ptr->field(0);
    return RangeBitmapFileIndexWriter::Create(arrow_schema_ptr, arrow_field->name(), options_,
                                              pool);
}

Result<std::shared_ptr<RangeBitmapFileIndexWriter>> RangeBitmapFileIndexWriter::Create(
    const std::shared_ptr<arrow::Schema>& arrow_schema, const std::string& field_name,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool) {
    const auto field = arrow_schema->GetFieldByName(field_name);
    if (!field) {
        return Status::Invalid(fmt::format("Field not found in schema: {}", field_name));
    }
    PAIMON_ASSIGN_OR_RAISE(FieldType field_type,
                           FieldTypeUtils::ConvertToFieldType(field->type()->id()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeyFactory> shared_key_factory,
                           KeyFactory::Create(field_type));
    PAIMON_ASSIGN_OR_RAISE(int64_t parsed_chunk_size,
                           MemorySize::ParseBytes(RangeBitmapFileIndexWriter::DEFAULT_CHUNK_SIZE));
    if (const auto chunk_size_it = options.find(RangeBitmapFileIndex::CHUNK_SIZE);
        chunk_size_it != options.end()) {
        PAIMON_ASSIGN_OR_RAISE(parsed_chunk_size, MemorySize::ParseBytes(chunk_size_it->second));
    }
    if (parsed_chunk_size > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid("Chunk size must be less than 4GB");
    }
    auto appender_ptr =
        std::make_unique<RangeBitmap::Appender>(pool, shared_key_factory, parsed_chunk_size);
    return std::make_shared<RangeBitmapFileIndexWriter>(
        field->type(), options, pool, shared_key_factory, std::move(appender_ptr));
}

Status RangeBitmapFileIndexWriter::AddBatch(::ArrowArray* batch) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(batch, arrow_type_));
    PAIMON_ASSIGN_OR_RAISE(std::vector<Literal> array_values,
                           LiteralConverter::ConvertLiteralsFromArray(*array, true));
    for (const auto& literal : array_values) {
        appender_->Append(literal);
    }
    return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<Bytes>> RangeBitmapFileIndexWriter::SerializedBytes() const {
    return appender_->Serialize();
}

RangeBitmapFileIndexWriter::RangeBitmapFileIndexWriter(
    const std::shared_ptr<arrow::DataType>& arrow_type,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<KeyFactory>& key_factory, std::unique_ptr<RangeBitmap::Appender> appender)
    : arrow_type_(arrow_type),
      options_(options),
      pool_(pool),
      key_factory_(key_factory),
      appender_(std::move(appender)) {}

Result<std::shared_ptr<RangeBitmapFileIndexReader>> RangeBitmapFileIndexReader::Create(
    const std::shared_ptr<arrow::DataType>& arrow_type, const int32_t start, const int32_t length,
    const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool) {
    if (!arrow_type || !input_stream || !pool) {
        return Status::Invalid("RangeBitmapFileIndexReader::Create: null argument");
    }
    PAIMON_ASSIGN_OR_RAISE(FieldType field_type,
                           FieldTypeUtils::ConvertToFieldType(arrow_type->id()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RangeBitmap> range_bitmap,
                           RangeBitmap::Create(input_stream, start, field_type, pool));
    return std::shared_ptr<RangeBitmapFileIndexReader>(
        new RangeBitmapFileIndexReader(std::move(range_bitmap)));
}

RangeBitmapFileIndexReader::RangeBitmapFileIndexReader(std::unique_ptr<RangeBitmap> range_bitmap)
    : range_bitmap_(std::move(range_bitmap)) {}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitEqual(
    const Literal& literal) {
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), literal]() -> Result<RoaringBitmap32> {
            if (!self->range_bitmap_) return RoaringBitmap32();
            return self->range_bitmap_->Eq(literal);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitNotEqual(
    const Literal& literal) {
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), literal]() -> Result<RoaringBitmap32> {
            if (!self->range_bitmap_) return RoaringBitmap32();
            return self->range_bitmap_->Neq(literal);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), literals]() -> Result<RoaringBitmap32> {
            if (!self->range_bitmap_) return RoaringBitmap32();
            return self->range_bitmap_->In(literals);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), literals]() -> Result<RoaringBitmap32> {
            if (!self->range_bitmap_) return RoaringBitmap32();
            return self->range_bitmap_->NotIn(literals);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitIsNull() {
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this()]() -> Result<RoaringBitmap32> {
            if (!self->range_bitmap_) return RoaringBitmap32();
            return self->range_bitmap_->IsNull();
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitIsNotNull() {
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this()]() -> Result<RoaringBitmap32> {
            if (!self->range_bitmap_) return RoaringBitmap32();
            return self->range_bitmap_->IsNotNull();
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitGreaterThan(
    const Literal& literal) {
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), literal]() -> Result<RoaringBitmap32> {
            if (!self->range_bitmap_) return RoaringBitmap32();
            return self->range_bitmap_->Gt(literal);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitLessThan(
    const Literal& literal) {
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), literal]() -> Result<RoaringBitmap32> {
            if (!self->range_bitmap_) return RoaringBitmap32();
            return self->range_bitmap_->Lt(literal);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), literal]() -> Result<RoaringBitmap32> {
            if (!self->range_bitmap_) return RoaringBitmap32();
            return self->range_bitmap_->Gte(literal);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), literal]() -> Result<RoaringBitmap32> {
            if (!self->range_bitmap_) return RoaringBitmap32();
            return self->range_bitmap_->Lte(literal);
        });
}

}  // namespace paimon
