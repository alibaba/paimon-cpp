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

#include "paimon/common/predicate/literal_converter.h"

#include <optional>

#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_decimal.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_primitive.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {
Result<Literal> LiteralConverter::ConvertLiteralsFromString(const FieldType& type,
                                                            const std::string& value_str) {
    switch (type) {
        case FieldType::BOOLEAN: {
            auto value = StringUtils::StringToValue<bool>(value_str);
            if (value == std::nullopt) {
                return Status::Invalid(fmt::format("cannot convert {} to BOOLEAN type", value_str));
            }
            return Literal(value.value());
        }
        case FieldType::TINYINT: {
            auto value = StringUtils::StringToValue<int8_t>(value_str);
            if (value == std::nullopt) {
                return Status::Invalid(fmt::format("cannot convert {} to TINYINT type", value_str));
            }
            return Literal(value.value());
        }
        case FieldType::SMALLINT: {
            auto value = StringUtils::StringToValue<int16_t>(value_str);
            if (value == std::nullopt) {
                return Status::Invalid(
                    fmt::format("cannot convert {} to SMALLINT type", value_str));
            }
            return Literal(value.value());
        }
        case FieldType::INT: {
            auto value = StringUtils::StringToValue<int32_t>(value_str);
            if (value == std::nullopt) {
                return Status::Invalid(fmt::format("cannot convert {} to INT type", value_str));
            }
            return Literal(value.value());
        }
        case FieldType::BIGINT: {
            auto value = StringUtils::StringToValue<int64_t>(value_str);
            if (value == std::nullopt) {
                return Status::Invalid(fmt::format("cannot convert {} to BIGINT type", value_str));
            }
            return Literal(value.value());
        }
        case FieldType::FLOAT: {
            auto value = StringUtils::StringToValue<float>(value_str);
            if (value == std::nullopt) {
                return Status::Invalid(fmt::format("cannot convert {} to FLOAT type", value_str));
            }
            return Literal(value.value());
        }
        case FieldType::DOUBLE: {
            auto value = StringUtils::StringToValue<double>(value_str);
            if (value == std::nullopt) {
                return Status::Invalid(fmt::format("cannot convert {} to DOUBLE type", value_str));
            }
            return Literal(value.value());
        }
        case FieldType::STRING:
        case FieldType::BINARY:
            return Literal(type, value_str.data(), value_str.size());
        default:
            return Status::Invalid(
                fmt::format("Do not support type {}", FieldTypeUtils::FieldTypeToString(type)));
    }
}

Result<Literal> LiteralConverter::ConvertLiteralsFromRow(
    const std::shared_ptr<arrow::Schema>& schema, const InternalRow& row, int32_t field_idx,
    const FieldType& type) {
    if (row.IsNullAt(field_idx)) {
        return Literal(type);
    }
    switch (type) {
        case FieldType::BOOLEAN:
            return Literal(row.GetBoolean(field_idx));
        case FieldType::TINYINT:
            return Literal(static_cast<int8_t>(row.GetByte(field_idx)));
        case FieldType::SMALLINT:
            return Literal(row.GetShort(field_idx));
        case FieldType::INT:
            return Literal(row.GetInt(field_idx));
        case FieldType::BIGINT:
            return Literal(row.GetLong(field_idx));
        case FieldType::FLOAT:
            return Literal(row.GetFloat(field_idx));
        case FieldType::DOUBLE:
            return Literal(row.GetDouble(field_idx));
        case FieldType::STRING: {
            std::string field = row.GetString(field_idx).ToString();
            return Literal(type, field.data(), field.size());
        }
        case FieldType::BINARY: {
            auto field = row.GetBinary(field_idx);
            return Literal(type, field->data(), field->size());
        }
        case FieldType::TIMESTAMP: {
            auto timestamp_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(
                schema->field(field_idx)->type());
            if (!timestamp_type) {
                return Status::Invalid(
                    fmt::format("Convert literal from row not valid for schema {}, field_idx {}",
                                schema->ToString(), field_idx));
            }
            int32_t precision = DateTimeUtils::GetPrecisionFromType(timestamp_type);
            Timestamp field = row.GetTimestamp(field_idx, precision);
            return Literal(field);
        }
        case FieldType::DECIMAL: {
            auto* decimal_type = arrow::internal::checked_cast<arrow::Decimal128Type*>(
                schema->field(field_idx)->type().get());
            if (!decimal_type) {
                return Status::Invalid(
                    fmt::format("Convert literal from row not valid for schema {}, field_idx {}",
                                schema->ToString(), field_idx));
            }
            auto precision = decimal_type->precision();
            auto scale = decimal_type->scale();
            Decimal field = row.GetDecimal(field_idx, precision, scale);
            return Literal(field);
        }
        case FieldType::DATE:
            return Literal(FieldType::DATE, row.GetInt(field_idx));
        case FieldType::ARRAY:
        case FieldType::MAP:
        case FieldType::STRUCT:
        default:
            return Status::Invalid(fmt::format("Convert literal from row not valid for {}",
                                               FieldTypeUtils::FieldTypeToString(type)));
    }
}

Result<std::vector<Literal>> LiteralConverter::ConvertLiteralsFromArray(const arrow::Array& array,
                                                                        bool own_data) {
    const auto kind = array.type_id();
    switch (kind) {
        case arrow::Type::type::BOOL:
            return GetLiteralFromGenericArray<arrow::BooleanType>(array, FieldType::BOOLEAN);
        case arrow::Type::type::INT8:
            return GetLiteralFromGenericArray<arrow::Int8Type>(array, FieldType::TINYINT);
        case arrow::Type::type::INT16:
            return GetLiteralFromGenericArray<arrow::Int16Type>(array, FieldType::SMALLINT);
        case arrow::Type::type::INT32:
            return GetLiteralFromGenericArray<arrow::Int32Type>(array, FieldType::INT);
        case arrow::Type::type::INT64:
            return GetLiteralFromGenericArray<arrow::Int64Type>(array, FieldType::BIGINT);
        case arrow::Type::type::FLOAT:
            return GetLiteralFromGenericArray<arrow::FloatType>(array, FieldType::FLOAT);
        case arrow::Type::type::DOUBLE:
            return GetLiteralFromGenericArray<arrow::DoubleType>(array, FieldType::DOUBLE);
        case arrow::Type::type::STRING:
            return GetLiteralFromStringArray<arrow::StringType>(array, FieldType::STRING, own_data);
        case arrow::Type::type::BINARY:
            return GetLiteralFromStringArray<arrow::BinaryType>(array, FieldType::BINARY, own_data);
        case arrow::Type::type::TIMESTAMP:
            return GetLiteralFromTimestampArray(array);
        case arrow::Type::type::DECIMAL128:
            return GetLiteralFromDecimalArray(array);
        case arrow::Type::type::DATE32:
            return GetLiteralFromDateArray(array);
        case arrow::Type::type::DICTIONARY: {
            const auto& dict_array =
                arrow::internal::checked_cast<const arrow::DictionaryArray&>(array);
            auto* dict_type =
                arrow::internal::checked_cast<arrow::DictionaryType*>(dict_array.type().get());
            auto value_type_id = dict_type->value_type()->id();
            auto index_type_id = dict_type->index_type()->id();
            if (value_type_id == arrow::Type::type::STRING &&
                index_type_id == arrow::Type::type::INT32) {
                return GetLiteralFromDictionaryArray<arrow::StringArray, arrow::Int32Array>(
                    dict_array, FieldType::STRING, own_data);
            } else if (value_type_id == arrow::Type::type::LARGE_STRING &&
                       index_type_id == arrow::Type::type::INT64) {
                return GetLiteralFromDictionaryArray<arrow::LargeStringArray, arrow::Int64Array>(
                    dict_array, FieldType::STRING, own_data);
            } else {
                return Status::Invalid(
                    "only support [STRING, INT32] or [LARGE_STRING, INT64] for DictionaryArray");
            }
        }
        default:
            return Status::Invalid(
                fmt::format("Not support literal on arrow {} type", array.type()->ToString()));
    }
}

std::vector<Literal> LiteralConverter::GetLiteralFromDecimalArray(const arrow::Array& array) {
    using ArrayType = typename arrow::TypeTraits<arrow::Decimal128Type>::ArrayType;
    const auto& array_(arrow::internal::checked_cast<const ArrayType&>(array));
    auto* arrow_type = arrow::internal::checked_cast<arrow::Decimal128Type*>(array.type().get());
    int32_t precision = arrow_type->precision();
    int32_t scale = arrow_type->scale();
    std::vector<Literal> literals;
    literals.reserve(array_.length());
    for (int64_t i = 0; i < array_.length(); i++) {
        if (array_.IsNull(i)) {
            literals.emplace_back(FieldType::DECIMAL);
        } else {
            const arrow::Decimal128 decimal(array_.GetValue(i));
            auto value =
                static_cast<Decimal::int128_t>(decimal.high_bits()) << 64 | decimal.low_bits();
            literals.emplace_back(Decimal(precision, scale, value));
        }
    }
    return literals;
}

std::vector<Literal> LiteralConverter::GetLiteralFromDateArray(const arrow::Array& array) {
    using ArrayType = typename arrow::TypeTraits<arrow::Date32Type>::ArrayType;
    const auto& array_(arrow::internal::checked_cast<const ArrayType&>(array));
    std::vector<Literal> literals;
    literals.reserve(array_.length());
    for (int64_t i = 0; i < array_.length(); i++) {
        if (array_.IsNull(i)) {
            literals.emplace_back(FieldType::DATE);
        } else {
            literals.emplace_back(FieldType::DATE, array_.Value(i));
        }
    }
    return literals;
}

std::vector<Literal> LiteralConverter::GetLiteralFromTimestampArray(const arrow::Array& array) {
    using ArrayType = typename arrow::TypeTraits<arrow::TimestampType>::ArrayType;
    const auto& array_(arrow::internal::checked_cast<const ArrayType&>(array));
    auto timestamp_type =
        arrow::internal::checked_pointer_cast<arrow::TimestampType>(array_.type());
    assert(timestamp_type);
    DateTimeUtils::TimeType time_type = DateTimeUtils::GetTimeTypeFromArrowType(timestamp_type);
    std::vector<Literal> literals;
    literals.reserve(array_.length());
    for (int64_t i = 0; i < array_.length(); i++) {
        if (array_.IsNull(i)) {
            literals.emplace_back(FieldType::TIMESTAMP);
        } else {
            int64_t data = array_.Value(i);
            auto [milli, nano] = DateTimeUtils::TimestampConverter(
                data, time_type, DateTimeUtils::TimeType::MILLISECOND,
                DateTimeUtils::TimeType::NANOSECOND);
            literals.emplace_back(Timestamp(milli, nano));
        }
    }
    return literals;
}
}  // namespace paimon
