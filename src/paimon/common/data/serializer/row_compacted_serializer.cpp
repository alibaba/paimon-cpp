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
#include "paimon/common/data/serializer/row_compacted_serializer.h"

#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_map.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/data/serializer/binary_serializer_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/utils/fields_comparator.h"

namespace paimon {
Result<std::unique_ptr<RowCompactedSerializer>> RowCompactedSerializer::Create(
    const std::shared_ptr<arrow::Schema>& schema, const std::shared_ptr<MemoryPool>& pool) {
    std::vector<InternalRow::FieldGetterFunc> getters(schema->num_fields());
    std::vector<RowCompactedSerializer::FieldWriter> writers(schema->num_fields());
    std::vector<RowCompactedSerializer::FieldReader> readers(schema->num_fields());
    for (int32_t i = 0; i < schema->num_fields(); i++) {
        auto field_type = schema->field(i)->type();
        // TODO(xinyu.lxy): check if we can enable use view
        PAIMON_ASSIGN_OR_RAISE(getters[i],
                               InternalRow::CreateFieldGetter(i, field_type, /*use_view=*/false));
        PAIMON_ASSIGN_OR_RAISE(writers[i], CreateFieldWriter(field_type, pool));
        PAIMON_ASSIGN_OR_RAISE(readers[i], CreateFieldReader(field_type, pool));
    }
    return std::unique_ptr<RowCompactedSerializer>(new RowCompactedSerializer(
        schema, std::move(getters), std::move(writers), std::move(readers), pool));
}

Result<MemorySlice::SliceComparator> RowCompactedSerializer::CreateSliceComparator(
    const std::shared_ptr<arrow::Schema>& schema, const std::shared_ptr<MemoryPool>& pool) {
    int32_t bit_set_in_bytes = RowCompactedSerializer::CalculateBitSetInBytes(schema->num_fields());
    auto row_reader1 = std::make_shared<RowReader>(bit_set_in_bytes, pool);
    auto row_reader2 = std::make_shared<RowReader>(bit_set_in_bytes, pool);
    std::vector<RowCompactedSerializer::FieldReader> readers(schema->num_fields());
    std::vector<FieldsComparator::VariantComparatorFunc> comparators(schema->num_fields());
    for (int32_t i = 0; i < schema->num_fields(); i++) {
        auto field_type = schema->field(i)->type();
        PAIMON_ASSIGN_OR_RAISE(readers[i], CreateFieldReader(field_type, pool));
        PAIMON_ASSIGN_OR_RAISE(comparators[i],
                               FieldsComparator::CompareVariant(i, field_type, /*use_view=*/false));
    }
    auto comparator = [row_reader1, row_reader2, readers, comparators](
                          const std::shared_ptr<MemorySlice>& slice1,
                          const std::shared_ptr<MemorySlice>& slice2) -> Result<int32_t> {
        row_reader1->PointTo(*slice1->GetSegment(), slice1->Offset());
        row_reader2->PointTo(*slice2->GetSegment(), slice2->Offset());
        for (int32_t i = 0; i < static_cast<int32_t>(readers.size()); i++) {
            bool is_null1 = row_reader1->IsNullAt(i);
            bool is_null2 = row_reader2->IsNullAt(i);
            if (!is_null1 || !is_null2) {
                if (is_null1) {
                    return -1;
                } else if (is_null2) {
                    return 1;
                } else {
                    PAIMON_ASSIGN_OR_RAISE(VariantType field1, readers[i](i, row_reader1.get()));
                    PAIMON_ASSIGN_OR_RAISE(VariantType field2, readers[i](i, row_reader2.get()));
                    int32_t comp = comparators[i](field1, field2);
                    if (comp != 0) {
                        return comp;
                    }
                }
            }
        }
        return 0;
    };
    return std::function<Result<int32_t>(const std::shared_ptr<MemorySlice>&,
                                         const std::shared_ptr<MemorySlice>&)>(comparator);
}

Result<std::shared_ptr<Bytes>> RowCompactedSerializer::SerializeToBytes(const InternalRow& row) {
    if (!row_writer_) {
        row_writer_ = std::make_unique<RowWriter>(CalculateBitSetInBytes(getters_.size()), pool_);
    }
    row_writer_->Reset();
    PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind, row.GetRowKind());
    row_writer_->WriteRowKind(*row_kind);
    for (size_t i = 0; i < getters_.size(); i++) {
        VariantType field = getters_[i](row);
        PAIMON_RETURN_NOT_OK(writers_[i](i, field, row_writer_.get()));
    }
    return row_writer_->CopyBuffer();
}

Result<std::unique_ptr<InternalRow>> RowCompactedSerializer::Deserialize(
    const std::shared_ptr<Bytes>& bytes) {
    if (!row_reader_) {
        row_reader_ = std::make_unique<RowReader>(CalculateBitSetInBytes(getters_.size()), pool_);
    }
    row_reader_->PointTo(bytes);
    auto row = std::make_unique<GenericRow>(getters_.size());
    PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind, row_reader_->ReadRowKind());
    row->SetRowKind(row_kind);
    for (size_t i = 0; i < readers_.size(); i++) {
        PAIMON_ASSIGN_OR_RAISE(VariantType field, readers_[i](i, row_reader_.get()));
        row->SetField(i, field);
    }
    return row;
}

RowCompactedSerializer::RowCompactedSerializer(
    const std::shared_ptr<arrow::Schema>& schema,
    std::vector<InternalRow::FieldGetterFunc>&& getters,
    std::vector<RowCompactedSerializer::FieldWriter>&& writers,
    std::vector<RowCompactedSerializer::FieldReader>&& readers,
    const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      schema_(schema),
      getters_(std::move(getters)),
      writers_(std::move(writers)),
      readers_(std::move(readers)) {}

Result<RowCompactedSerializer::FieldReader> RowCompactedSerializer::CreateFieldReader(
    const std::shared_ptr<arrow::DataType>& field_type, const std::shared_ptr<MemoryPool>& pool) {
    arrow::Type::type type = field_type->id();
    RowCompactedSerializer::FieldReader field_reader;
    switch (type) {
        case arrow::Type::type::BOOL: {
            field_reader = [](int32_t pos, RowReader* reader) -> Result<VariantType> {
                return VariantType(reader->ReadValue<bool>());
            };
            break;
        }
        case arrow::Type::type::INT8: {
            field_reader = [](int32_t pos, RowReader* reader) -> Result<VariantType> {
                return VariantType(reader->ReadValue<char>());
            };
            break;
        }
        case arrow::Type::type::INT16: {
            field_reader = [](int32_t pos, RowReader* reader) -> Result<VariantType> {
                return VariantType(reader->ReadValue<int16_t>());
            };
            break;
        }
        case arrow::Type::type::INT32:
        case arrow::Type::type::DATE32: {
            field_reader = [](int32_t pos, RowReader* reader) -> Result<VariantType> {
                return VariantType(reader->ReadValue<int32_t>());
            };
            break;
        }
        case arrow::Type::type::INT64: {
            field_reader = [](int32_t pos, RowReader* reader) -> Result<VariantType> {
                return VariantType(reader->ReadValue<int64_t>());
            };
            break;
        }
        case arrow::Type::type::FLOAT: {
            field_reader = [](int32_t pos, RowReader* reader) -> Result<VariantType> {
                return VariantType(reader->ReadValue<float>());
            };
            break;
        }
        case arrow::Type::type::DOUBLE: {
            field_reader = [](int32_t pos, RowReader* reader) -> Result<VariantType> {
                return VariantType(reader->ReadValue<double>());
            };
            break;
        }
        case arrow::Type::type::STRING: {
            field_reader = [](int32_t pos, RowReader* reader) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(VariantType value, reader->ReadString());
                return value;
            };
            break;
        }
        case arrow::Type::type::BINARY: {
            field_reader = [](int32_t pos, RowReader* reader) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(VariantType value, reader->ReadBinary());
                return value;
            };
            break;
        }
        case arrow::Type::type::TIMESTAMP: {
            auto timestamp_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(field_type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(timestamp_type);
            field_reader = [precision](int32_t pos, RowReader* reader) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(VariantType value, reader->ReadTimestamp(precision));
                return value;
            };
            break;
        }
        case arrow::Type::type::DECIMAL: {
            auto* decimal_type =
                arrow::internal::checked_cast<arrow::Decimal128Type*>(field_type.get());
            assert(decimal_type);
            auto precision = decimal_type->precision();
            auto scale = decimal_type->scale();
            field_reader = [precision, scale](int32_t pos,
                                              RowReader* reader) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(VariantType value, reader->ReadDecimal(precision, scale));
                return value;
            };
            break;
        }
        case arrow::Type::type::LIST: {
            field_reader = [](int32_t pos, RowReader* reader) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(VariantType value, reader->ReadArray());
                return value;
            };
            break;
        }
        case arrow::Type::type::MAP: {
            field_reader = [](int32_t pos, RowReader* reader) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(VariantType value, reader->ReadMap());
                return value;
            };
            break;
        }
        case arrow::Type::type::STRUCT: {
            auto* struct_type = arrow::internal::checked_cast<arrow::StructType*>(field_type.get());
            assert(struct_type);
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<RowCompactedSerializer> serializer,
                RowCompactedSerializer::Create(arrow::schema(struct_type->fields()), pool));
            field_reader = [serializer](int32_t pos, RowReader* reader) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(VariantType value, reader->ReadRow(serializer));
                return value;
            };
            break;
        }
        default:
            return Status::Invalid(
                fmt::format("type {} not support in FieldReader in row compacted serializer",
                            field_type->ToString()));
    }

    RowCompactedSerializer::FieldReader ret =
        [field_reader](int32_t pos, RowReader* reader) -> Result<VariantType> {
        if (reader->IsNullAt(pos)) {
            return VariantType(NullType());
        }
        return field_reader(pos, reader);
    };
    return ret;
}

Result<RowCompactedSerializer::FieldWriter> RowCompactedSerializer::CreateFieldWriter(
    const std::shared_ptr<arrow::DataType>& field_type, const std::shared_ptr<MemoryPool>& pool) {
    arrow::Type::type type = field_type->id();
    RowCompactedSerializer::FieldWriter field_writer;
    switch (type) {
        case arrow::Type::type::BOOL: {
            field_writer = [](int32_t pos, const VariantType& field, RowWriter* writer) -> Status {
                return writer->WriteValue<bool>(DataDefine::GetVariantValue<bool>(field));
            };
            break;
        }
        case arrow::Type::type::INT8: {
            field_writer = [](int32_t pos, const VariantType& field, RowWriter* writer) -> Status {
                return writer->WriteValue<char>(DataDefine::GetVariantValue<char>(field));
            };
            break;
        }
        case arrow::Type::type::INT16: {
            field_writer = [](int32_t pos, const VariantType& field, RowWriter* writer) -> Status {
                return writer->WriteValue<int16_t>(DataDefine::GetVariantValue<int16_t>(field));
            };
            break;
        }
        case arrow::Type::type::INT32:
        case arrow::Type::type::DATE32: {
            field_writer = [](int32_t pos, const VariantType& field, RowWriter* writer) -> Status {
                return writer->WriteValue<int32_t>(DataDefine::GetVariantValue<int32_t>(field));
            };
            break;
        }
        case arrow::Type::type::INT64: {
            field_writer = [](int32_t pos, const VariantType& field, RowWriter* writer) -> Status {
                return writer->WriteValue<int64_t>(DataDefine::GetVariantValue<int64_t>(field));
            };
            break;
        }
        case arrow::Type::type::FLOAT: {
            field_writer = [](int32_t pos, const VariantType& field, RowWriter* writer) -> Status {
                return writer->WriteValue<float>(DataDefine::GetVariantValue<float>(field));
            };
            break;
        }
        case arrow::Type::type::DOUBLE: {
            field_writer = [](int32_t pos, const VariantType& field, RowWriter* writer) -> Status {
                return writer->WriteValue<double>(DataDefine::GetVariantValue<double>(field));
            };
            break;
        }
        case arrow::Type::type::STRING: {
            field_writer = [](int32_t pos, const VariantType& field, RowWriter* writer) -> Status {
                const auto* view = DataDefine::GetVariantPtr<std::string_view>(field);
                if (view) {
                    // TODO(xinyu.lxy): remove copy from view
                    return writer->WriteString(
                        BinaryString::FromString(std::string(*view), GetDefaultPool().get()));
                }
                return writer->WriteString(DataDefine::GetVariantValue<BinaryString>(field));
            };
            break;
        }
        case arrow::Type::type::BINARY: {
            field_writer = [](int32_t pos, const VariantType& field, RowWriter* writer) -> Status {
                const auto* view = DataDefine::GetVariantPtr<std::string_view>(field);
                if (view) {
                    auto bytes =
                        std::make_shared<Bytes>(std::string(*view), GetDefaultPool().get());
                    return writer->WriteBinary(bytes.get());
                }
                return writer->WriteBinary(
                    DataDefine::GetVariantValue<std::shared_ptr<Bytes>>(field).get());
            };
            break;
        }
        case arrow::Type::type::TIMESTAMP: {
            auto timestamp_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(field_type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(timestamp_type);
            field_writer = [precision](int32_t pos, const VariantType& field,
                                       RowWriter* writer) -> Status {
                return writer->WriteTimestamp(DataDefine::GetVariantValue<Timestamp>(field),
                                              precision);
            };
            break;
        }
        case arrow::Type::type::DECIMAL: {
            auto* decimal_type =
                arrow::internal::checked_cast<arrow::Decimal128Type*>(field_type.get());
            assert(decimal_type);
            auto precision = decimal_type->precision();
            field_writer = [precision](int32_t pos, const VariantType& field,
                                       RowWriter* writer) -> Status {
                return writer->WriteDecimal(DataDefine::GetVariantValue<Decimal>(field), precision);
            };
            break;
        }
        case arrow::Type::type::LIST: {
            field_writer = [field_type](int32_t pos, const VariantType& field,
                                        RowWriter* writer) -> Status {
                return writer->WriteArray(
                    DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(field), field_type);
            };
            break;
        }
        case arrow::Type::type::MAP: {
            field_writer = [field_type](int32_t pos, const VariantType& field,
                                        RowWriter* writer) -> Status {
                return writer->WriteMap(
                    DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(field), field_type);
            };
            break;
        }
        case arrow::Type::type::STRUCT: {
            auto struct_type = arrow::internal::checked_pointer_cast<arrow::StructType>(field_type);
            assert(struct_type);
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<RowCompactedSerializer> serializer,
                RowCompactedSerializer::Create(arrow::schema(struct_type->fields()), pool));
            field_writer = [serializer](int32_t pos, const VariantType& field,
                                        RowWriter* writer) -> Status {
                return writer->WriteRow(
                    DataDefine::GetVariantValue<std::shared_ptr<InternalRow>>(field), serializer);
            };
            break;
        }
        default:
            return Status::Invalid(
                fmt::format("type {} not support in FieldWriter in row compacted serializer",
                            field_type->ToString()));
    }

    RowCompactedSerializer::FieldWriter ret = [field_writer](int32_t pos, const VariantType& field,
                                                             RowWriter* writer) -> Status {
        if (DataDefine::IsVariantNull(field)) {
            writer->SetNullAt(pos);
            return Status::OK();
        }
        return field_writer(pos, field, writer);
    };
    return ret;
}

RowCompactedSerializer::RowWriter::RowWriter(int32_t header_size_in_bytes,
                                             const std::shared_ptr<MemoryPool>& pool)
    : header_size_in_bytes_(header_size_in_bytes), pool_(pool) {
    int32_t initial_capacity = std::max(64, header_size_in_bytes);
    SetBuffer(std::make_shared<Bytes>(initial_capacity, pool_.get()));
    position_ = header_size_in_bytes_;
}

Status RowCompactedSerializer::RowWriter::WriteDecimal(const Decimal& value, int32_t precision) {
    if (Decimal::IsCompact(precision)) {
        return WriteValue<int64_t>(value.ToUnscaledLong());
    } else {
        auto value_bytes = value.ToUnscaledBytes();
        return WriteBinary(&value_bytes);
    }
}

Status RowCompactedSerializer::RowWriter::WriteTimestamp(const Timestamp& value,
                                                         int32_t precision) {
    if (Timestamp::IsCompact(precision)) {
        return WriteValue<int64_t>(value.GetMillisecond());
    } else {
        PAIMON_RETURN_NOT_OK(WriteValue<int64_t>(value.GetMillisecond()));
        return WriteUnsignedInt(value.GetNanoOfMillisecond());
    }
}

Status RowCompactedSerializer::RowWriter::WriteRow(
    const std::shared_ptr<InternalRow>& value,
    const std::shared_ptr<RowCompactedSerializer>& serializer) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, serializer->SerializeToBytes(*value));
    return WriteBinary(bytes.get());
}

Status RowCompactedSerializer::RowWriter::WriteArray(const std::shared_ptr<InternalArray>& value,
                                                     const std::shared_ptr<arrow::DataType>& type) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BinaryArray> binary_array,
                           BinarySerializerUtils::WriteBinaryArray(value, type, pool_.get()));
    return WriteSegments(binary_array->GetSegments(), binary_array->GetOffset(),
                         binary_array->GetSizeInBytes());
}

Status RowCompactedSerializer::RowWriter::WriteMap(const std::shared_ptr<InternalMap>& value,
                                                   const std::shared_ptr<arrow::DataType>& type) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BinaryMap> binary_map,
                           BinarySerializerUtils::WriteBinaryMap(value, type, pool_.get()));
    return WriteSegments(binary_map->GetSegments(), binary_map->GetOffset(),
                         binary_map->GetSizeInBytes());
}

std::shared_ptr<Bytes> RowCompactedSerializer::RowWriter::CopyBuffer() const {
    return Bytes::CopyOf(*buffer_, position_, pool_.get());
}

Status RowCompactedSerializer::RowWriter::WriteUnsignedInt(int32_t value) {
    EnsureCapacity(VarLengthIntUtils::kMaxVarIntSize);
    PAIMON_ASSIGN_OR_RAISE(int32_t len,
                           VarLengthIntUtils::EncodeInt(position_, value, buffer_.get()));
    position_ += len;
    return Status::OK();
}

Status RowCompactedSerializer::RowWriter::WriteSegments(const std::vector<MemorySegment>& segments,
                                                        int32_t off, int32_t len) {
    PAIMON_RETURN_NOT_OK(WriteUnsignedInt(len));
    EnsureCapacity(len);
    MemorySegmentUtils::CopyToBytes(segments, off, buffer_.get(), position_, len);
    position_ += len;
    return Status::OK();
}

void RowCompactedSerializer::RowWriter::EnsureCapacity(int32_t size) {
    if (static_cast<int32_t>(buffer_->size()) - position_ < size) {
        Grow(size);
    }
}

void RowCompactedSerializer::RowWriter::Grow(int32_t min_capacity_add) {
    auto current_len = static_cast<int32_t>(buffer_->size());
    int32_t new_len = std::max(current_len * 2, current_len + min_capacity_add);

    auto new_buffer = std::make_shared<Bytes>(new_len, pool_.get());
    memcpy(new_buffer->data(), buffer_->data(), current_len);
    SetBuffer(std::move(new_buffer));
}

void RowCompactedSerializer::RowWriter::SetBuffer(std::shared_ptr<Bytes> new_buffer) {
    buffer_ = std::move(new_buffer);
    segment_ = MemorySegment::Wrap(buffer_);
}

void RowCompactedSerializer::RowReader::PointTo(const std::shared_ptr<Bytes>& bytes) {
    MemorySegment seg = MemorySegment::Wrap(bytes);
    PointTo(seg, 0);
}

void RowCompactedSerializer::RowReader::PointTo(const MemorySegment& segment, int32_t offset) {
    segment_ = segment;
    segments_ = {segment};
    offset_ = offset;
    position_ = offset + header_size_in_bytes_;
}

Result<const RowKind*> RowCompactedSerializer::RowReader::ReadRowKind() const {
    char b = segment_.Get(offset_);
    return RowKind::FromByteValue(static_cast<int8_t>(b));
}

Result<BinaryString> RowCompactedSerializer::RowReader::ReadString() {
    PAIMON_ASSIGN_OR_RAISE(int32_t length, ReadUnsignedInt());
    BinaryString str = BinaryString::FromAddress(segments_, position_, length);
    position_ += length;
    return str;
}

Result<std::shared_ptr<Bytes>> RowCompactedSerializer::RowReader::ReadBinary() {
    PAIMON_ASSIGN_OR_RAISE(int32_t length, ReadUnsignedInt());
    auto bytes = std::make_shared<Bytes>(length, pool_.get());
    segment_.Get(position_, bytes.get(), /*offset=*/0, length);
    position_ += length;
    return bytes;
}

Result<Decimal> RowCompactedSerializer::RowReader::ReadDecimal(int32_t precision, int32_t scale) {
    if (Decimal::IsCompact(precision)) {
        return Decimal::FromUnscaledLong(ReadValue<int64_t>(), precision, scale);
    } else {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, ReadBinary());
        return Decimal::FromUnscaledBytes(precision, scale, bytes.get());
    }
}

Result<Timestamp> RowCompactedSerializer::RowReader::ReadTimestamp(int32_t precision) {
    if (Timestamp::IsCompact(precision)) {
        return Timestamp::FromEpochMillis(ReadValue<int64_t>());
    }
    auto milliseconds = ReadValue<int64_t>();
    PAIMON_ASSIGN_OR_RAISE(int32_t nanos_of_millisecond, ReadUnsignedInt());
    return Timestamp::FromEpochMillis(milliseconds, nanos_of_millisecond);
}

Result<std::shared_ptr<InternalArray>> RowCompactedSerializer::RowReader::ReadArray() {
    auto value = std::make_shared<BinaryArray>();
    PAIMON_ASSIGN_OR_RAISE(int32_t length, ReadUnsignedInt());
    value->PointTo(segments_, position_, length);
    position_ += length;
    return value;
}

Result<std::shared_ptr<InternalMap>> RowCompactedSerializer::RowReader::ReadMap() {
    auto value = std::make_shared<BinaryMap>();
    PAIMON_ASSIGN_OR_RAISE(int32_t length, ReadUnsignedInt());
    value->PointTo(segments_, position_, length);
    position_ += length;
    return value;
}

Result<std::shared_ptr<InternalRow>> RowCompactedSerializer::RowReader::ReadRow(
    const std::shared_ptr<RowCompactedSerializer>& serializer) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, ReadBinary());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalRow> result, serializer->Deserialize(bytes));
    return result;
}

}  // namespace paimon
