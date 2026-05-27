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

#include "paimon/core/casting/decimal_to_decimal_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <optional>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/array/array_decimal.h"
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
}  // namespace arrow

namespace paimon {
Result<Literal> DecimalToDecimalCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(literal.GetType() == FieldType::DECIMAL);
    PAIMON_RETURN_NOT_OK(DecimalUtils::CheckDecimalType(*target_type));
    if (literal.IsNull()) {
        return Literal(FieldType::DECIMAL);
    }
    auto* target_decimal_type =
        arrow::internal::checked_cast<arrow::Decimal128Type*>(target_type.get());
    assert(target_decimal_type);
    auto src_value = literal.GetValue<Decimal>();
    arrow::Decimal128 src_decimal(src_value.HighBits(), src_value.LowBits());
    auto scaled_decimal = DecimalUtils::RescaleDecimalWithOverflowCheck(
        src_decimal, src_value.Scale(), target_decimal_type->precision(),
        target_decimal_type->scale());
    if (scaled_decimal == std::nullopt) {
        return Literal(FieldType::DECIMAL);
    }
    return Literal(Decimal(
        target_decimal_type->precision(), target_decimal_type->scale(),
        static_cast<Decimal::int128_t>(static_cast<Decimal::uint128_t>(static_cast<uint64_t>(
                                           scaled_decimal.value().high_bits()))
                                           << 64 |
                                       scaled_decimal.value().low_bits())));
}

Result<std::shared_ptr<arrow::Array>> DecimalToDecimalCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    PAIMON_RETURN_NOT_OK(DecimalUtils::CheckDecimalType(*target_type));
    auto* src_array = arrow::internal::checked_cast<arrow::Decimal128Array*>(array.get());
    assert(src_array);
    auto* src_decimal_type =
        arrow::internal::checked_cast<arrow::Decimal128Type*>(array->type().get());
    assert(src_decimal_type);
    auto* target_decimal_type =
        arrow::internal::checked_cast<arrow::Decimal128Type*>(target_type.get());
    assert(target_decimal_type);
    auto decimal_builder = std::make_shared<arrow::Decimal128Builder>(target_type, pool);
    for (int64_t i = 0; i < src_array->length(); ++i) {
        if (src_array->IsNull(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(decimal_builder->AppendNull());
        } else {
            arrow::Decimal128 src_value(src_array->GetValue(i));
            auto scaled_decimal = DecimalUtils::RescaleDecimalWithOverflowCheck(
                src_value, src_decimal_type->scale(), target_decimal_type->precision(),
                target_decimal_type->scale());
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
