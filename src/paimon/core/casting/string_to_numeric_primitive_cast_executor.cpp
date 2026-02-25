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

#include "paimon/core/casting/string_to_numeric_primitive_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "arrow/compute/cast.h"
#include "arrow/type.h"
#include "arrow/util/value_parsing.h"
#include "fmt/format.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
StringToNumericPrimitiveCastExecutor::StringToNumericPrimitiveCastExecutor() {
    literal_cast_executor_map_ = {
        {FieldType::TINYINT,
         [&](const Literal& literal) { return CastLiteral<int8_t>(literal, FieldType::TINYINT); }},
        {FieldType::SMALLINT,
         [&](const Literal& literal) {
             return CastLiteral<int16_t>(literal, FieldType::SMALLINT);
         }},
        {FieldType::INT,
         [&](const Literal& literal) { return CastLiteral<int32_t>(literal, FieldType::INT); }},
        {FieldType::BIGINT,
         [&](const Literal& literal) { return CastLiteral<int64_t>(literal, FieldType::BIGINT); }},
        {FieldType::FLOAT,
         [&](const Literal& literal) { return CastLiteral<float>(literal, FieldType::FLOAT); }},
        {FieldType::DOUBLE,
         [&](const Literal& literal) { return CastLiteral<double>(literal, FieldType::DOUBLE); }}};
}

template <typename TargetType>
Result<Literal> StringToNumericPrimitiveCastExecutor::CastLiteral(const Literal& literal,
                                                                  const FieldType& target_type) {
    if (literal.IsNull()) {
        return Literal(target_type);
    }
    auto value = literal.GetValue<std::string>();
    if constexpr (std::is_same_v<TargetType, float> || std::is_same_v<TargetType, double>) {
        // use arrow::internal::StringToFloat Func to handle overflow, e.g., 1.7976931348623157e309
        // is supposed to return infinity
        TargetType out;
        bool success = arrow::internal::StringToFloat(value.data(), value.size(), '.', &out);
        if (!success) {
            return Status::Invalid(
                fmt::format("cast literal in StringToNumericPrimitiveCastExecutor failed: cannot "
                            "cast '{}' from STRING to {}",
                            value, FieldTypeUtils::FieldTypeToString(target_type)));
        }
        return Literal(out);
    } else {
        std::optional<TargetType> casted_value = StringUtils::StringToValue<TargetType>(value);
        if (!casted_value) {
            return Status::Invalid(
                fmt::format("cast literal in StringToNumericPrimitiveCastExecutor failed: cannot "
                            "cast '{}' from STRING to {}",
                            value, FieldTypeUtils::FieldTypeToString(target_type)));
        }
        return Literal(casted_value.value());
    }
}

Result<Literal> StringToNumericPrimitiveCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(literal.GetType() == FieldType::STRING);
    PAIMON_ASSIGN_OR_RAISE(FieldType target_field_type,
                           FieldTypeUtils::ConvertToFieldType(target_type->id()));
    auto iter = literal_cast_executor_map_.find(target_field_type);
    if (iter == literal_cast_executor_map_.end()) {
        return Status::Invalid(fmt::format(
            "cast literal in StringToNumericPrimitiveCastExecutor failed: cannot find cast "
            "function from STRING to {}",
            FieldTypeUtils::FieldTypeToString(target_field_type)));
    }
    return iter->second(literal);
}

Result<std::shared_ptr<arrow::Array>> StringToNumericPrimitiveCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    return CastingUtils::Cast(array, target_type, options, pool);
}

}  // namespace paimon
