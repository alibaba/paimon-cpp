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

#include "paimon/common/data/serializer/binary_serializer_utils.h"

#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/utils/date_time_utils.h"
namespace paimon {
Result<std::shared_ptr<BinaryArray>> BinarySerializerUtils::WriteBinaryArray(
    const std::shared_ptr<InternalArray>& value, const std::shared_ptr<arrow::DataType>& type,
    MemoryPool* pool) {
    if (auto binary_array = std::dynamic_pointer_cast<BinaryArray>(value)) {
        return binary_array;
    }
    auto binary_array = std::make_shared<BinaryArray>();
    auto list_type = std::dynamic_pointer_cast<arrow::ListType>(type);
    assert(list_type);
    auto value_type = list_type->value_type();
    // TODO(xinyu.lxy): reuse BinaryWriter
    BinaryArrayWriter binary_writer(binary_array.get(), value->Size(),
                                    BinaryArrayWriter::GetElementSize(value_type->id()), pool);
    for (int32_t i = 0; i < value->Size(); i++) {
        PAIMON_RETURN_NOT_OK(WriteBinaryData(value_type, value.get(), i, &binary_writer, pool));
    }
    binary_writer.Complete();
    return binary_array;
}

Result<std::shared_ptr<BinaryMap>> BinarySerializerUtils::WriteBinaryMap(
    const std::shared_ptr<InternalMap>& value, const std::shared_ptr<arrow::DataType>& type,
    MemoryPool* pool) {
    if (auto binary_map = std::dynamic_pointer_cast<BinaryMap>(value)) {
        return binary_map;
    }
    auto map_type = std::dynamic_pointer_cast<arrow::MapType>(type);
    assert(map_type);
    auto key_type = map_type->key_type();
    auto value_type = map_type->item_type();
    auto key_array = value->KeyArray();
    auto value_array = value->ValueArray();
    assert(key_array && value_array);
    assert(key_array->Size() == value_array->Size());
    BinaryArray binary_key_array;
    BinaryArrayWriter binary_key_writer(&binary_key_array, key_array->Size(),
                                        BinaryArrayWriter::GetElementSize(key_type->id()), pool);
    BinaryArray binary_value_array;
    BinaryArrayWriter binary_value_writer(&binary_value_array, value_array->Size(),
                                          BinaryArrayWriter::GetElementSize(value_type->id()),
                                          pool);
    for (int32_t i = 0; i < key_array->Size(); i++) {
        PAIMON_RETURN_NOT_OK(
            WriteBinaryData(key_type, key_array.get(), i, &binary_key_writer, pool));
        PAIMON_RETURN_NOT_OK(
            WriteBinaryData(value_type, value_array.get(), i, &binary_value_writer, pool));
    }
    binary_key_writer.Complete();
    binary_value_writer.Complete();
    return BinaryMap::ValueOf(binary_key_array, binary_value_array, pool);
}

Result<std::shared_ptr<BinaryRow>> BinarySerializerUtils::WriteBinaryRow(
    const std::shared_ptr<InternalRow>& value, const std::shared_ptr<arrow::DataType>& type,
    MemoryPool* pool) {
    if (auto binary_row = std::dynamic_pointer_cast<BinaryRow>(value)) {
        return binary_row;
    }

    auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(type);
    assert(struct_type);
    auto field_count = struct_type->num_fields();
    auto binary_row = std::make_shared<BinaryRow>(field_count);
    BinaryRowWriter binary_writer(binary_row.get(), /*initial_size=*/1024, pool);
    for (int32_t i = 0; i < field_count; i++) {
        PAIMON_RETURN_NOT_OK(
            WriteBinaryData(struct_type->field(i)->type(), value.get(), i, &binary_writer, pool));
    }
    binary_writer.Complete();
    return binary_row;
}

Status BinarySerializerUtils::WriteBinaryData(const std::shared_ptr<arrow::DataType>& type,
                                              const DataGetters* getter, int32_t pos,
                                              BinaryWriter* writer, MemoryPool* pool) {
    assert(getter && writer && pool);
    arrow::Type::type type_id = type->id();
    auto array_writer = dynamic_cast<BinaryArrayWriter*>(writer);
    if (getter->IsNullAt(pos)) {
        // compatible with Java Paimon
        if (array_writer) {
            array_writer->SetNullAt(pos, type_id);
            return Status::OK();
        } else if (type_id != arrow::Type::type::DECIMAL &&
                   type_id != arrow::Type::type::TIMESTAMP) {
            // if row writer, exclude decimal and timestamp when set null
            writer->SetNullAt(pos);
            return Status::OK();
        }
    }
    switch (type_id) {
        case arrow::Type::type::BOOL: {
            writer->WriteBoolean(pos, getter->GetBoolean(pos));
            break;
        }
        case arrow::Type::type::INT8: {
            writer->WriteByte(pos, getter->GetByte(pos));
            break;
        }
        case arrow::Type::type::INT16: {
            writer->WriteShort(pos, getter->GetShort(pos));
            break;
        }
        case arrow::Type::type::DATE32: {
            writer->WriteInt(pos, getter->GetDate(pos));
            break;
        }
        case arrow::Type::type::INT32: {
            writer->WriteInt(pos, getter->GetInt(pos));
            break;
        }
        case arrow::Type::type::INT64: {
            writer->WriteLong(pos, getter->GetLong(pos));
            break;
        }
        case arrow::Type::type::FLOAT: {
            writer->WriteFloat(pos, getter->GetFloat(pos));
            break;
        }
        case arrow::Type::type::DOUBLE: {
            writer->WriteDouble(pos, getter->GetDouble(pos));
            break;
        }
        case arrow::Type::type::STRING: {
            writer->WriteString(pos, getter->GetString(pos));
            break;
        }
        case arrow::Type::type::BINARY: {
            writer->WriteBinary(pos, *(getter->GetBinary(pos)));
            break;
        }
        case arrow::Type::type::TIMESTAMP: {
            auto timestamp_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(type);
            assert(timestamp_type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(timestamp_type);
            if (getter->IsNullAt(pos)) {
                // compatible with Java Paimon
                if (!Timestamp::IsCompact(precision)) {
                    writer->WriteTimestamp(pos, std::nullopt, precision);
                } else {
                    writer->SetNullAt(pos);
                }
            } else {
                writer->WriteTimestamp(pos, getter->GetTimestamp(pos, precision), precision);
            }
            break;
        }
        case arrow::Type::type::DECIMAL: {
            auto* decimal_type = arrow::internal::checked_cast<arrow::Decimal128Type*>(type.get());
            assert(decimal_type);
            auto precision = decimal_type->precision();
            auto scale = decimal_type->scale();
            if (getter->IsNullAt(pos)) {
                // compatible with Java Paimon
                if (!Decimal::IsCompact(precision)) {
                    writer->WriteDecimal(pos, std::nullopt, precision);
                } else {
                    writer->SetNullAt(pos);
                }
            } else {
                writer->WriteDecimal(pos, getter->GetDecimal(pos, precision, scale), precision);
            }
            break;
        }
        case arrow::Type::type::LIST: {
            auto internal_array = getter->GetArray(pos);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BinaryArray> binary_array,
                                   WriteBinaryArray(internal_array, type, pool));
            writer->WriteArray(pos, *binary_array);
            break;
        }
        case arrow::Type::type::MAP: {
            auto internal_map = getter->GetMap(pos);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BinaryMap> binary_map,
                                   WriteBinaryMap(internal_map, type, pool));
            writer->WriteMap(pos, *binary_map);
            break;
        }
        case arrow::Type::type::STRUCT: {
            auto internal_row = getter->GetRow(pos, type->num_fields());
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BinaryRow> binary_row,
                                   WriteBinaryRow(internal_row, type, pool));
            writer->WriteRow(pos, *binary_row);
            break;
        }
        default:
            return Status::Invalid(
                fmt::format("type {} not support in WriteBinaryData in binary serializer utils",
                            type->ToString()));
    }
    return Status::OK();
}
}  // namespace paimon
