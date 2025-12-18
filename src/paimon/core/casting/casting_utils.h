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

#pragma once

#include <memory>
#include <string>

#include "arrow/compute/api.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/predicate/literal.h"

namespace paimon {
class CastingUtils {
 public:
    CastingUtils() = delete;
    ~CastingUtils() = delete;

    // make sure src and casted literals are both integer (i.e., TINYINT, SMALLINT, INT, BIGINT)
    static bool IsIntegerLiteralCastedOverflow(const Literal& src_literal,
                                               const Literal& casted_literal) {
        return GetLongValueFromLiteral(src_literal) != GetLongValueFromLiteral(casted_literal);
    }

    static Result<std::shared_ptr<arrow::Array>> Cast(
        const std::shared_ptr<arrow::Array>& src_array,
        const std::shared_ptr<arrow::DataType>& target_type,
        const arrow::compute::CastOptions& options, arrow::MemoryPool* pool);

    template <typename SrcScalar, typename SrcDataType, typename TargetScalar,
              typename TargetDataType>
    static Result<Literal> Cast(const Literal& literal,
                                const std::shared_ptr<arrow::DataType>& src_type,
                                const std::shared_ptr<arrow::DataType>& target_type,
                                const arrow::compute::CastOptions& options) {
        PAIMON_ASSIGN_OR_RAISE(FieldType target_field_type,
                               FieldTypeUtils::ConvertToFieldType(target_type->id()));
        if (literal.IsNull()) {
            return Literal(target_field_type);
        }
        auto src_value = literal.GetValue<SrcDataType>();
        if (!arrow::compute::CanCast(*src_type, *target_type)) {
            return Status::Invalid(fmt::format("cast literal failed: cannot cast from {} to {}",
                                               src_type->ToString(), target_type->ToString()));
        }
        std::shared_ptr<arrow::Scalar> src_scalar;
        if constexpr (std::is_same_v<SrcDataType, Decimal>) {
            src_scalar = std::make_shared<SrcScalar>(
                arrow::Decimal128(src_value.HighBits(), src_value.LowBits()), src_type);
        } else {
            src_scalar = std::make_shared<SrcScalar>(src_value);
        }
        arrow::TypeHolder type_holder(target_type.get());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            arrow::Datum casted_result,
            arrow::compute::Cast(arrow::Datum(src_scalar), type_holder, options));
        auto* casted_scalar =
            arrow::internal::checked_cast<TargetScalar*>(casted_result.scalar().get());
        if (!casted_scalar) {
            return Status::Invalid(fmt::format("cast literal failed: cannot cast to {} scalar",
                                               target_type->ToString()));
        }
        if constexpr (std::is_same_v<TargetDataType, std::string>) {
            std::string_view casted_data = casted_scalar->view();
            return Literal(FieldType::STRING, casted_data.data(), casted_data.size());
        } else {
            const auto* casted_data = static_cast<const TargetDataType*>(casted_scalar->data());
            assert(casted_data);
            return Literal(*casted_data);
        }
    }

    // src_array is local time (no tz), target type is utc (with tz)
    static Result<std::shared_ptr<arrow::Array>> TimestampToTimestampWithTimezone(
        const std::shared_ptr<arrow::Array>& src_array,
        const std::shared_ptr<arrow::TimestampType>& target_type, arrow::MemoryPool* pool);

    // src_array is utc (with tz) , target type is local time (no tz),
    static Result<std::shared_ptr<arrow::Array>> TimestampWithTimezoneToTimestamp(
        const std::shared_ptr<arrow::Array>& src_array,
        const std::shared_ptr<arrow::TimestampType>& target_type, arrow::MemoryPool* pool);

 private:
    static int64_t GetLongValueFromLiteral(const Literal& literal);
};
}  // namespace paimon
