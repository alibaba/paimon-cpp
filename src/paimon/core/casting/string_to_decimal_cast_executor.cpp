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

#include "paimon/core/casting/string_to_decimal_cast_executor.h"

#include <cassert>
#include <utility>

#include "arrow/array/array_binary.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {

Result<std::optional<arrow::Decimal128>> StringToDecimalCastExecutor::StringToDecimal(
    const std::string& str_value, int32_t target_precision, int32_t target_scale) {
    int32_t src_precision = 0, src_scale = 0;
    arrow::Decimal128 src_decimal;
    // parse invalid string
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::Decimal128::FromString(str_value, &src_decimal, &src_precision, &src_scale));
    if (src_precision > Decimal::MAX_PRECISION) {
        return std::optional<arrow::Decimal128>();
    }
    return DecimalUtils::RescaleDecimalWithOverflowCheck(src_decimal, src_scale, target_precision,
                                                         target_scale);
}

Result<Literal> StringToDecimalCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(literal.GetType() == FieldType::STRING);
    PAIMON_RETURN_NOT_OK(DecimalUtils::CheckDecimalType(*target_type));
    auto* decimal_type = arrow::internal::checked_cast<arrow::Decimal128Type*>(target_type.get());
    assert(decimal_type);
    if (literal.IsNull()) {
        return Literal(FieldType::DECIMAL);
    }
    auto src_value = literal.GetValue<std::string>();
    PAIMON_ASSIGN_OR_RAISE(
        std::optional<arrow::Decimal128> scaled_decimal,
        StringToDecimal(src_value, decimal_type->precision(), decimal_type->scale()));
    if (scaled_decimal == std::nullopt) {
        return Literal(FieldType::DECIMAL);
    }
    return Literal(Decimal(
        decimal_type->precision(), decimal_type->scale(),
        static_cast<Decimal::int128_t>(static_cast<Decimal::uint128_t>(static_cast<uint64_t>(
                                           scaled_decimal.value().high_bits()))
                                           << 64 |
                                       scaled_decimal.value().low_bits())));
}

Result<std::shared_ptr<arrow::Array>> StringToDecimalCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    PAIMON_RETURN_NOT_OK(DecimalUtils::CheckDecimalType(*target_type));
    auto* string_array = arrow::internal::checked_cast<arrow::StringArray*>(array.get());
    assert(string_array);
    auto* decimal_type = arrow::internal::checked_cast<arrow::Decimal128Type*>(target_type.get());
    auto decimal_builder = std::make_shared<arrow::Decimal128Builder>(target_type, pool);
    for (int64_t i = 0; i < string_array->length(); ++i) {
        if (string_array->IsNull(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(decimal_builder->AppendNull());
        } else {
            std::string src_value = string_array->GetString(i);
            PAIMON_ASSIGN_OR_RAISE(
                std::optional<arrow::Decimal128> scaled_decimal,
                StringToDecimal(src_value, decimal_type->precision(), decimal_type->scale()));
            if (scaled_decimal == std::nullopt) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(decimal_builder->AppendNull());
            } else {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(decimal_builder->Append(scaled_decimal.value()));
            }
        }
    }
    std::shared_ptr<arrow::Array> casted_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(decimal_builder->Finish(&casted_array));
    return casted_array;
}

}  // namespace paimon
