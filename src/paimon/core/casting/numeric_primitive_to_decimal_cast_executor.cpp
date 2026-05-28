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

#include "paimon/core/casting/numeric_primitive_to_decimal_cast_executor.h"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
}  // namespace arrow

namespace paimon {

template <typename SrcType>
Result<Literal> NumericPrimitiveToDecimalCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) {
    auto* decimal_type = arrow::internal::checked_cast<arrow::Decimal128Type*>(target_type.get());
    assert(decimal_type);
    if (literal.IsNull()) {
        return Literal(FieldType::DECIMAL);
    }
    auto src_value = literal.GetValue<SrcType>();
    if constexpr (std::is_same_v<SrcType, float> || std::is_same_v<SrcType, double>) {
        if (src_value == INFINITY || src_value == -INFINITY || std::isnan(src_value)) {
            return Status::Invalid(fmt::format("Cannot cast {} to decimal", src_value));
        }
        auto decimal_result = arrow::Decimal128::FromReal(src_value, decimal_type->precision(),
                                                          decimal_type->scale());
        if (decimal_result.ok()) {
            return Literal{Decimal(decimal_type->precision(), decimal_type->scale(),
                                   static_cast<Decimal::int128_t>(
                                       static_cast<Decimal::uint128_t>(static_cast<uint64_t>(
                                           decimal_result.ValueUnsafe().high_bits()))
                                           << 64 |
                                       decimal_result.ValueUnsafe().low_bits()))};
        }
    } else {
        auto scaled_decimal = DecimalUtils::RescaleDecimalWithOverflowCheck(
            arrow::Decimal128(src_value), /*src_scale=*/0, decimal_type->precision(),
            decimal_type->scale());
        if (scaled_decimal != std::nullopt) {
            return Literal(Decimal(decimal_type->precision(), decimal_type->scale(),
                                   static_cast<Decimal::int128_t>(
                                       static_cast<Decimal::uint128_t>(static_cast<uint64_t>(
                                           scaled_decimal.value().high_bits()))
                                           << 64 |
                                       scaled_decimal.value().low_bits())));
        }
    }
    return Literal(FieldType::DECIMAL);
}

Result<Literal> NumericPrimitiveToDecimalCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    PAIMON_RETURN_NOT_OK(DecimalUtils::CheckDecimalType(*target_type));
    FieldType src_type = literal.GetType();
    if (src_type == FieldType::TINYINT) {
        return Cast<int8_t>(literal, target_type);
    } else if (src_type == FieldType::SMALLINT) {
        return Cast<int16_t>(literal, target_type);
    } else if (src_type == FieldType::INT) {
        return Cast<int32_t>(literal, target_type);
    } else if (src_type == FieldType::BIGINT) {
        return Cast<int64_t>(literal, target_type);
    } else if (src_type == FieldType::FLOAT) {
        return Cast<float>(literal, target_type);
    } else if (src_type == FieldType::DOUBLE) {
        return Cast<double>(literal, target_type);
    }
    return Status::Invalid(fmt::format(
        "cast literal in NumericPrimitiveToDecimalCastExecutor failed: cannot find cast "
        "function from {} to {}",
        FieldTypeUtils::FieldTypeToString(src_type), target_type->ToString()));
}

template <typename SrcType>
Result<std::shared_ptr<arrow::Array>> NumericPrimitiveToDecimalCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) {
    using SrcValueType = typename arrow::NumericArray<SrcType>::value_type;
    auto* typed_array = arrow::internal::checked_cast<arrow::NumericArray<SrcType>*>(array.get());
    assert(typed_array);
    auto* decimal_type = arrow::internal::checked_cast<arrow::Decimal128Type*>(target_type.get());
    auto decimal_builder = std::make_shared<arrow::Decimal128Builder>(target_type, pool);
    for (int64_t i = 0; i < typed_array->length(); ++i) {
        if (typed_array->IsNull(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(decimal_builder->AppendNull());
        } else {
            SrcValueType src_value = typed_array->Value(i);
            if constexpr (std::is_same_v<SrcType, arrow::FloatType> ||
                          std::is_same_v<SrcType, arrow::DoubleType>) {
                if (src_value == INFINITY || src_value == -INFINITY || std::isnan(src_value)) {
                    return Status::Invalid(fmt::format("Cannot cast {} to decimal", src_value));
                }
                auto decimal_result = arrow::Decimal128::FromReal(
                    src_value, decimal_type->precision(), decimal_type->scale());
                if (!decimal_result.ok()) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(decimal_builder->AppendNull());
                } else {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(
                        decimal_builder->Append(decimal_result.ValueUnsafe()));
                }
            } else {
                auto scaled_decimal = DecimalUtils::RescaleDecimalWithOverflowCheck(
                    arrow::Decimal128(src_value), /*src_scale=*/0, decimal_type->precision(),
                    decimal_type->scale());
                if (scaled_decimal == std::nullopt) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(decimal_builder->AppendNull());
                } else {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(
                        decimal_builder->Append(scaled_decimal.value()));
                }
            }
        }
    }
    std::shared_ptr<arrow::Array> casted_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(decimal_builder->Finish(&casted_array));
    return casted_array;
}

Result<std::shared_ptr<arrow::Array>> NumericPrimitiveToDecimalCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    PAIMON_RETURN_NOT_OK(DecimalUtils::CheckDecimalType(*target_type));
    auto src_type_id = array->type()->id();
    if (src_type_id == arrow::Type::type::INT8) {
        return Cast<arrow::Int8Type>(array, target_type, pool);
    } else if (src_type_id == arrow::Type::type::INT16) {
        return Cast<arrow::Int16Type>(array, target_type, pool);
    } else if (src_type_id == arrow::Type::type::INT32) {
        return Cast<arrow::Int32Type>(array, target_type, pool);
    } else if (src_type_id == arrow::Type::type::INT64) {
        return Cast<arrow::Int64Type>(array, target_type, pool);
    } else if (src_type_id == arrow::Type::type::FLOAT) {
        return Cast<arrow::FloatType>(array, target_type, pool);
    } else if (src_type_id == arrow::Type::type::DOUBLE) {
        return Cast<arrow::DoubleType>(array, target_type, pool);
    }
    return Status::Invalid(
        fmt::format("cast array in NumericPrimitiveToDecimalCastExecutor failed: cannot cast "
                    "from {} to decimal",
                    array->type()->ToString()));
}

}  // namespace paimon
