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

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/result.h"

namespace paimon {

/// Utility class for nested column pruning and map key selection.
class PAIMON_EXPORT NestedProjectionUtils {
 public:
    NestedProjectionUtils() = delete;

    /// Extract the paimon field ID from an Arrow field's metadata ("paimon.id").
    /// Returns -1 if the metadata key is not present.
    static int32_t GetPaimonFieldId(const std::shared_ptr<arrow::Field>& field) {
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
    static std::shared_ptr<arrow::Field> FindFieldByPaimonId(
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
    static Result<std::optional<std::shared_ptr<arrow::DataType>>> PruneDataType(
        const std::shared_ptr<arrow::DataType>& read_type,
        const std::shared_ptr<arrow::DataType>& data_type);

    /// Parse the "paimon.map.selected-keys" metadata from an Arrow field.
    /// Returns an empty set if the metadata key is absent or the field is not a MAP.
    /// The metadata value must be a JSON array of strings, e.g. '["key1","key2"]'.
    static std::set<std::string> GetMapSelectedKeys(const std::shared_ptr<arrow::Field>& field);

    /// Filter a MapArray so that only entries whose key is in `selected_keys` are kept.
    /// Only supports string-keyed maps. Returns the original array unchanged if
    /// `selected_keys` is empty.
    static Result<std::shared_ptr<arrow::Array>> FilterMapArrayBySelectedKeys(
        const std::shared_ptr<arrow::Array>& map_array, const std::set<std::string>& selected_keys);
};

}  // namespace paimon
