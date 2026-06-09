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

#include "paimon/common/data/internal_row.h"

#include <cassert>
#include <utility>
#include <variant>

#include "arrow/api.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/status.h"

namespace paimon {
Result<InternalRow::FieldGetterFunc> InternalRow::CreateFieldGetter(
    int32_t field_idx, const std::shared_ptr<arrow::DataType>& field_type, bool use_view) {
    arrow::Type::type type = field_type->id();
    InternalRow::FieldGetterFunc field_getter;
    switch (type) {
        case arrow::Type::type::BOOL: {
            field_getter = [field_idx](const InternalRow& row) -> VariantType {
                return row.GetBoolean(field_idx);
            };
            break;
        }
        case arrow::Type::type::INT8: {
            field_getter = [field_idx](const InternalRow& row) -> VariantType {
                return row.GetByte(field_idx);
            };
            break;
        }
        case arrow::Type::type::INT16: {
            field_getter = [field_idx](const InternalRow& row) -> VariantType {
                return row.GetShort(field_idx);
            };
            break;
        }
        case arrow::Type::type::INT32: {
            field_getter = [field_idx](const InternalRow& row) -> VariantType {
                return row.GetInt(field_idx);
            };
            break;
        }
        case arrow::Type::type::INT64: {
            field_getter = [field_idx](const InternalRow& row) -> VariantType {
                return row.GetLong(field_idx);
            };
            break;
        }
        case arrow::Type::type::FLOAT: {
            field_getter = [field_idx](const InternalRow& row) -> VariantType {
                return row.GetFloat(field_idx);
            };
            break;
        }
        case arrow::Type::type::DOUBLE: {
            field_getter = [field_idx](const InternalRow& row) -> VariantType {
                return row.GetDouble(field_idx);
            };
            break;
        }
        case arrow::Type::type::DATE32: {
            field_getter = [field_idx](const InternalRow& row) -> VariantType {
                return row.GetDate(field_idx);
            };
            break;
        }
        case arrow::Type::type::STRING: {
            field_getter = [field_idx, use_view](const InternalRow& row) -> VariantType {
                if (use_view) {
                    return row.GetStringView(field_idx);
                } else {
                    return row.GetString(field_idx);
                }
            };
            break;
        }
        case arrow::Type::type::BINARY: {
            field_getter = [field_idx, use_view](const InternalRow& row) -> VariantType {
                if (use_view) {
                    return row.GetStringView(field_idx);
                } else {
                    return row.GetBinary(field_idx);
                }
            };
            break;
        }
        case arrow::Type::type::TIMESTAMP: {
            auto timestamp_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(field_type);
            assert(timestamp_type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(timestamp_type);
            field_getter = [field_idx, precision](const InternalRow& row) -> VariantType {
                return row.GetTimestamp(field_idx, precision);
            };
            break;
        }
        case arrow::Type::type::DECIMAL128: {
            auto* decimal_type =
                arrow::internal::checked_cast<arrow::Decimal128Type*>(field_type.get());
            assert(decimal_type);
            auto precision = decimal_type->precision();
            auto scale = decimal_type->scale();
            field_getter = [field_idx, precision, scale](const InternalRow& row) -> VariantType {
                return row.GetDecimal(field_idx, precision, scale);
            };
            break;
        }
        case arrow::Type::type::STRUCT: {
            auto* struct_type = arrow::internal::checked_cast<arrow::StructType*>(field_type.get());
            assert(struct_type);
            auto num_fields = struct_type->num_fields();
            field_getter = [field_idx, num_fields](const InternalRow& row) -> VariantType {
                return row.GetRow(field_idx, num_fields);
            };
            break;
        }
        case arrow::Type::type::LIST: {
            field_getter = [field_idx](const InternalRow& row) -> VariantType {
                return row.GetArray(field_idx);
            };
            break;
        }
        case arrow::Type::type::MAP: {
            field_getter = [field_idx](const InternalRow& row) -> VariantType {
                return row.GetMap(field_idx);
            };
            break;
        }
        default:
            return Status::Invalid(
                fmt::format("type {} not support in data getter", field_type->ToString()));
    }
    InternalRow::FieldGetterFunc ret = [field_idx,
                                        field_getter](const InternalRow& row) -> VariantType {
        if (row.IsNullAt(field_idx)) {
            return NullType();
        }
        return field_getter(row);
    };
    return ret;
}

}  // namespace paimon
