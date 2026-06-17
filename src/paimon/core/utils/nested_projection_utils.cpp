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

#include "paimon/core/utils/nested_projection_utils.h"

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/status.h"

namespace paimon {

std::shared_ptr<arrow::Field> NestedProjectionUtils::FindFieldByName(
    const arrow::FieldVector& fields, const std::string& name) {
    for (const auto& field : fields) {
        if (field->name() == name) {
            return field;
        }
    }
    return nullptr;
}

Result<bool> NestedProjectionUtils::HasNestedSubfieldProjectionType(
    const std::shared_ptr<arrow::DataType>& file_type,
    const std::shared_ptr<arrow::DataType>& read_type) {
    if (file_type->id() != read_type->id()) {
        return false;
    }

    switch (file_type->id()) {
        case arrow::Type::STRUCT: {
            auto file_struct = std::static_pointer_cast<arrow::StructType>(file_type);
            auto read_struct = std::static_pointer_cast<arrow::StructType>(read_type);
            if (read_struct->num_fields() != file_struct->num_fields()) {
                return true;
            }
            for (const auto& read_child : read_struct->fields()) {
                auto file_child = FindFieldByName(file_struct->fields(), read_child->name());
                if (!file_child) {
                    return true;
                }
                PAIMON_ASSIGN_OR_RAISE(
                    bool child_has_nested_projection,
                    HasNestedSubfieldProjectionType(file_child->type(), read_child->type()));
                if (child_has_nested_projection) {
                    return true;
                }
            }
            return false;
        }
        case arrow::Type::LIST: {
            auto file_list = std::static_pointer_cast<arrow::ListType>(file_type);
            auto read_list = std::static_pointer_cast<arrow::ListType>(read_type);
            return HasNestedSubfieldProjectionType(file_list->value_type(),
                                                   read_list->value_type());
        }
        case arrow::Type::MAP: {
            auto file_map = std::static_pointer_cast<arrow::MapType>(file_type);
            auto read_map = std::static_pointer_cast<arrow::MapType>(read_type);
            PAIMON_ASSIGN_OR_RAISE(
                bool key_has_nested_projection,
                HasNestedSubfieldProjectionType(file_map->key_type(), read_map->key_type()));
            if (key_has_nested_projection) {
                return true;
            }
            return HasNestedSubfieldProjectionType(file_map->item_type(), read_map->item_type());
        }
        default:
            return false;
    }
}

Result<std::optional<std::shared_ptr<arrow::DataType>>> NestedProjectionUtils::PruneDataType(
    const std::shared_ptr<arrow::DataType>& read_type,
    const std::shared_ptr<arrow::DataType>& data_type) {
    // Identical types need no pruning.
    if (read_type->Equals(data_type)) {
        return std::optional<std::shared_ptr<arrow::DataType>>(data_type);
    }

    switch (read_type->id()) {
        case arrow::Type::STRUCT: {
            arrow::FieldVector pruned_fields;
            for (const auto& read_child : read_type->fields()) {
                int32_t read_child_id = GetPaimonFieldId(read_child);
                std::shared_ptr<arrow::Field> data_child =
                    FindFieldByPaimonId(data_type, read_child_id);
                if (!data_child) {
                    // Schema Evolution: field not present in data, skip.
                    continue;
                }
                PAIMON_ASSIGN_OR_RAISE(
                    std::optional<std::shared_ptr<arrow::DataType>> pruned_child_type,
                    PruneDataType(read_child->type(), data_child->type()));
                if (!pruned_child_type.has_value()) {
                    // All sub-fields of this child were pruned away; skip it.
                    continue;
                }
                pruned_fields.push_back(data_child->WithType(pruned_child_type.value()));
            }
            if (pruned_fields.empty()) {
                // All fields pruned — return nullopt so the caller can skip this field.
                return std::optional<std::shared_ptr<arrow::DataType>>(std::nullopt);
            }
            return std::optional<std::shared_ptr<arrow::DataType>>(arrow::struct_(pruned_fields));
        }

        case arrow::Type::LIST: {
            const auto& read_list = static_cast<const arrow::ListType&>(*read_type);
            const auto& data_list = static_cast<const arrow::ListType&>(*data_type);
            PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<arrow::DataType>> pruned_elem,
                                   PruneDataType(read_list.value_type(), data_list.value_type()));
            if (!pruned_elem.has_value()) {
                return std::optional<std::shared_ptr<arrow::DataType>>(std::nullopt);
            }
            std::shared_ptr<arrow::DataType> result_type = arrow::list(arrow::field(
                data_list.value_field()->name(), pruned_elem.value(),
                data_list.value_field()->nullable(), data_list.value_field()->metadata()));
            return std::optional<std::shared_ptr<arrow::DataType>>(std::move(result_type));
        }

        case arrow::Type::MAP: {
            const auto& read_map = static_cast<const arrow::MapType&>(*read_type);
            const auto& data_map = static_cast<const arrow::MapType&>(*data_type);
            PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<arrow::DataType>> pruned_key,
                                   PruneDataType(read_map.key_type(), data_map.key_type()));
            PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<arrow::DataType>> pruned_value,
                                   PruneDataType(read_map.item_type(), data_map.item_type()));
            if (!pruned_key.has_value() || !pruned_value.has_value()) {
                return std::optional<std::shared_ptr<arrow::DataType>>(std::nullopt);
            }
            std::shared_ptr<arrow::DataType> result_type = arrow::map(
                pruned_key.value(), pruned_value.value(), data_map.key_field()->nullable());
            return std::optional<std::shared_ptr<arrow::DataType>>(std::move(result_type));
        }

        default:
            // Atomic type: return data_type as-is (type evolution is handled
            // separately by CastExecutor).
            return std::optional<std::shared_ptr<arrow::DataType>>(data_type);
    }
}

Result<bool> NestedProjectionUtils::HasNestedSubfieldProjection(
    const std::shared_ptr<arrow::Schema>& file_schema,
    const std::shared_ptr<arrow::Schema>& read_schema) {
    for (const auto& read_field : read_schema->fields()) {
        auto file_field = file_schema->GetFieldByName(read_field->name());
        if (!file_field) {
            continue;
        }
        if (read_field->type()->id() == arrow::Type::STRUCT ||
            read_field->type()->id() == arrow::Type::LIST ||
            read_field->type()->id() == arrow::Type::MAP) {
            PAIMON_ASSIGN_OR_RAISE(
                bool has_nested_projection,
                HasNestedSubfieldProjectionType(file_field->type(), read_field->type()));
            if (has_nested_projection) {
                return true;
            }
        }
    }
    return false;
}

// Map selected-keys support

std::set<std::string> NestedProjectionUtils::GetMapSelectedKeys(
    const std::shared_ptr<arrow::Field>& field) {
    std::set<std::string> result;
    if (!field || !field->HasMetadata() || !field->metadata()) {
        return result;
    }
    auto get_result = field->metadata()->Get(DataField::MAP_SELECTED_KEYS);
    if (!get_result.ok()) {
        return result;
    }
    std::string value = get_result.ValueUnsafe();
    StringUtils::Trim(&value);
    if (value.empty()) {
        // Metadata is explicitly present but empty: treat as "filter all keys".
        result.insert("");
        return result;
    }

    auto tokens = StringUtils::Split(value, ",", /*ignore_empty=*/true);
    for (auto& token : tokens) {
        StringUtils::Trim(&token);
        if (!token.empty()) {
            result.insert(token);
        }
    }
    return result;
}

Result<std::shared_ptr<arrow::Array>> NestedProjectionUtils::FilterMapArrayBySelectedKeys(
    const std::shared_ptr<arrow::Array>& array, const std::set<std::string>& selected_keys) {
    if (selected_keys.empty() || !array || array->length() == 0) {
        return array;
    }
    bool filter_all_keys = selected_keys.count("") > 0;

    auto map_array = std::static_pointer_cast<arrow::MapArray>(array);
    auto map_type = std::static_pointer_cast<arrow::MapType>(array->type());

    if (map_type->key_type()->id() != arrow::Type::STRING) {
        return Status::Invalid(
            fmt::format("FilterMapArrayBySelectedKeys only supports string keys, got {}",
                        map_type->key_type()->ToString()));
    }

    auto keys_array = std::static_pointer_cast<arrow::StringArray>(map_array->keys());
    auto values_array = map_array->items();
    int64_t total_entries = keys_array->length();
    int64_t num_maps = map_array->length();

    // Mark which flat entries to keep
    std::vector<bool> keep(total_entries, false);
    int64_t kept_count = 0;
    for (int64_t i = 0; i < total_entries; ++i) {
        if (filter_all_keys) {
            continue;
        }
        if (!keys_array->IsNull(i)) {
            std::string_view key_view = keys_array->GetView(i);
            std::string key_str(key_view.data(), key_view.size());
            if (selected_keys.count(key_str) > 0) {
                keep[i] = true;
                ++kept_count;
            }
        }
    }

    if (kept_count == total_entries) {
        return array;
    }

    // Collect kept slices as contiguous runs to build filtered key/value arrays
    // via Slice + Concatenate (avoids arrow::compute::Take dependency).
    arrow::ArrayVector key_slices;
    arrow::ArrayVector value_slices;
    key_slices.reserve(kept_count);
    value_slices.reserve(kept_count);

    std::vector<int32_t> new_offsets;
    new_offsets.reserve(num_maps + 1);
    int32_t running_offset = 0;

    for (int64_t map_idx = 0; map_idx < num_maps; ++map_idx) {
        new_offsets.push_back(running_offset);
        if (map_array->IsNull(map_idx)) {
            continue;
        }
        int64_t start = map_array->value_offset(map_idx);
        int64_t end = map_array->value_offset(map_idx + 1);
        // Collect contiguous runs of kept entries within this map
        int64_t run_start = -1;
        for (int64_t entry_idx = start; entry_idx <= end; ++entry_idx) {
            bool should_keep = (entry_idx < end) && keep[entry_idx];
            if (should_keep && run_start < 0) {
                run_start = entry_idx;
            } else if (!should_keep && run_start >= 0) {
                int64_t run_len = entry_idx - run_start;
                key_slices.push_back(keys_array->Slice(run_start, run_len));
                value_slices.push_back(values_array->Slice(run_start, run_len));
                running_offset += static_cast<int32_t>(run_len);
                run_start = -1;
            }
        }
    }
    new_offsets.push_back(running_offset);

    // Build filtered key/value arrays
    std::shared_ptr<arrow::Array> filtered_keys;
    std::shared_ptr<arrow::Array> filtered_values;
    if (key_slices.empty()) {
        // All entries filtered out — create empty arrays
        filtered_keys = keys_array->Slice(0, 0);
        filtered_values = values_array->Slice(0, 0);
    } else if (key_slices.size() == 1) {
        filtered_keys = key_slices[0];
        filtered_values = value_slices[0];
    } else {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(filtered_keys, arrow::Concatenate(key_slices));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(filtered_values, arrow::Concatenate(value_slices));
    }

    // Build new offsets array
    arrow::Int32Builder offset_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        offset_builder.Reserve(static_cast<int64_t>(new_offsets.size())));
    for (int32_t offset : new_offsets) {
        offset_builder.UnsafeAppend(offset);
    }
    std::shared_ptr<arrow::Array> new_offsets_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(offset_builder.Finish(&new_offsets_array));

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> result_map,
        arrow::MapArray::FromArrays(new_offsets_array, filtered_keys, filtered_values,
                                    arrow::default_memory_pool(), map_array->null_bitmap()));
    return result_map;
}

}  // namespace paimon
