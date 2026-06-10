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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/result.h"

namespace paimon {

/// Extract the paimon field ID from an Arrow field's metadata ("paimon.id").
/// Returns -1 if the metadata key is not present.
inline int32_t GetPaimonFieldId(const std::shared_ptr<arrow::Field>& field) {
    if (!field || !field->HasMetadata() || !field->metadata()) {
        return -1;
    }
    auto result = field->metadata()->Get(DataField::FIELD_ID);
    if (!result.ok()) {
        return -1;
    }
    std::optional<int32_t> field_id = StringUtils::StringToValue<int32_t>(result.ValueUnsafe());
    return field_id.value_or(-1);
}

/// Find a child field in a STRUCT DataType by paimon field ID.
/// Returns nullptr if no child has the given ID.
inline std::shared_ptr<arrow::Field> FindFieldByPaimonId(
    const std::shared_ptr<arrow::DataType>& struct_type, int32_t field_id) {
    if (!struct_type || struct_type->id() != arrow::Type::STRUCT) {
        return nullptr;
    }
    for (const auto& child : struct_type->fields()) {
        if (GetPaimonFieldId(child) == field_id) {
            return child;
        }
    }
    return nullptr;
}

/// Recursively prune `data_type` so that only the sub-fields requested by
/// `read_type` are retained. Matching is done by paimon field ID to support
/// schema evolution (field renames).
///
/// Supported nesting: STRUCT, LIST (element recurse), MAP (key/value recurse).
/// For atomic types, `data_type` is returned as-is.
///
/// Returns std::nullopt when all sub-fields of a STRUCT are pruned away
/// (caller should skip this field entirely, mirroring Java's null return).
Result<std::optional<std::shared_ptr<arrow::DataType>>> PruneDataType(
    const std::shared_ptr<arrow::DataType>& read_type,
    const std::shared_ptr<arrow::DataType>& data_type);

/// Prune a StructArray so that only the sub-fields present in `target_type`
/// are kept. Used as a fallback when the format reader returns more columns
/// than requested.
Result<std::shared_ptr<arrow::Array>> PruneArray(
    const std::shared_ptr<arrow::Array>& array,
    const std::shared_ptr<arrow::DataType>& target_type);

}  // namespace paimon
