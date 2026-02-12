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

#include "paimon/core/casting/string_to_boolean_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "arrow/array/array_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
Result<Literal> StringToBooleanCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(literal.GetType() == FieldType::STRING);
    PAIMON_ASSIGN_OR_RAISE(FieldType target_field_type,
                           FieldTypeUtils::ConvertToFieldType(target_type->id()));
    assert(target_field_type == FieldType::BOOLEAN);
    if (literal.IsNull()) {
        return Literal(target_field_type);
    }
    auto value = literal.GetValue<std::string>();
    std::optional<bool> bool_value = StringUtils::StringToValue<bool>(value);
    if (bool_value == std::nullopt) {
        return Status::Invalid(fmt::format(
            "StringToBooleanCastExecutor cast failed: STRING '{}' cannot cast to BOOLEAN", value));
    }
    return Literal(bool_value.value());
}

Result<std::shared_ptr<arrow::Array>> StringToBooleanCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    auto* string_array = arrow::internal::checked_cast<arrow::StringArray*>(array.get());
    assert(string_array);
    auto bool_builder = std::make_shared<arrow::BooleanBuilder>(pool);
    for (int64_t i = 0; i < string_array->length(); ++i) {
        if (string_array->IsNull(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(bool_builder->AppendNull());
        } else {
            std::optional<bool> bool_value =
                StringUtils::StringToValue<bool>(string_array->GetString(i));
            if (bool_value == std::nullopt) {
                return Status::Invalid(fmt::format(
                    "StringToBooleanCastExecutor cast failed: STRING '{}' cannot cast to BOOLEAN",
                    string_array->GetString(i)));
            }
            PAIMON_RETURN_NOT_OK_FROM_ARROW(bool_builder->Append(bool_value.value()));
        }
    }
    std::shared_ptr<arrow::Array> casted_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(bool_builder->Finish(&casted_array));
    return casted_array;
}

}  // namespace paimon
