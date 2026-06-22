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
#include <unordered_set>
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
            std::optional<std::shared_ptr<arrow::DataType>> pruned_elem = std::nullopt;
            PAIMON_ASSIGN_OR_RAISE(pruned_elem,
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
            std::optional<std::shared_ptr<arrow::DataType>> pruned_key = std::nullopt;
            PAIMON_ASSIGN_OR_RAISE(pruned_key,
                                   PruneDataType(read_map.key_type(), data_map.key_type()));
            std::optional<std::shared_ptr<arrow::DataType>> pruned_value = std::nullopt;
            PAIMON_ASSIGN_OR_RAISE(pruned_value,
                                   PruneDataType(read_map.item_type(), data_map.item_type()));
            if (!pruned_key.has_value() || !pruned_value.has_value()) {
                return std::optional<std::shared_ptr<arrow::DataType>>(std::nullopt);
            }
            std::shared_ptr<arrow::Field> pruned_item_field =
                data_map.item_field()->WithType(pruned_value.value());
            std::shared_ptr<arrow::DataType> result_type =
                arrow::map(pruned_key.value(), pruned_item_field, data_map.keys_sorted());
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

Result<std::vector<std::string>> NestedProjectionUtils::GetMapSelectedKeys(
    const std::shared_ptr<arrow::Field>& field) {
    std::vector<std::string> result;
    if (!field || !field->HasMetadata() || !field->metadata()) {
        return result;
    }
    auto get_result = field->metadata()->Get(DataField::MAP_SELECTED_KEYS);
    if (!get_result.ok()) {
        return result;
    }
    std::string value = get_result.ValueUnsafe();
    if (value.empty()) {
        // Metadata is explicitly present but empty: select the empty-string key.
        result.push_back("");
        return result;
    }

    auto tokens = StringUtils::Split(value, ",", /*ignore_empty=*/false);
    std::unordered_set<std::string> deduplicated;
    deduplicated.reserve(tokens.size());
    for (auto& token : tokens) {
        if (!deduplicated.insert(token).second) {
            return Status::Invalid(fmt::format("Duplicate selected key '{}' in {} metadata", token,
                                               DataField::MAP_SELECTED_KEYS));
        }
        result.push_back(token);
    }
    return result;
}

Result<std::shared_ptr<arrow::Array>> NestedProjectionUtils::FilterMapArrayBySelectedKeys(
    const std::shared_ptr<arrow::Array>& array, const std::vector<std::string>& selected_keys,
    arrow::MemoryPool* pool) {
    if (selected_keys.empty() || !array || array->length() == 0) {
        return array;
    }
    if (pool == nullptr) {
        return Status::Invalid("FilterMapArrayBySelectedKeys requires a non-null memory pool");
    }

    if (array->type_id() != arrow::Type::MAP) {
        return Status::Invalid(fmt::format(
            "FilterMapArrayBySelectedKeys requires map array, got {}", array->type()->ToString()));
    }

    auto map_array = std::static_pointer_cast<arrow::MapArray>(array);
    auto map_type = std::static_pointer_cast<arrow::MapType>(array->type());
    assert(map_array && map_type);

    auto key_array = map_array->keys();
    std::shared_ptr<arrow::StringArray> string_keys;
    std::shared_ptr<arrow::DictionaryArray> dict_keys;
    std::shared_ptr<arrow::StringArray> dict_values;
    std::shared_ptr<arrow::LargeStringArray> dict_large_values;
    if (key_array->type_id() == arrow::Type::STRING) {
        string_keys = std::static_pointer_cast<arrow::StringArray>(key_array);
    } else if (key_array->type_id() == arrow::Type::DICTIONARY) {
        auto dict_type = std::static_pointer_cast<arrow::DictionaryType>(key_array->type());
        if (dict_type->value_type()->id() != arrow::Type::STRING &&
            dict_type->value_type()->id() != arrow::Type::LARGE_STRING) {
            return Status::Invalid(
                fmt::format("FilterMapArrayBySelectedKeys only supports string keys or "
                            "dictionary<string|large_string> keys, got {}",
                            key_array->type()->ToString()));
        }
        dict_keys = std::static_pointer_cast<arrow::DictionaryArray>(key_array);
        if (dict_type->value_type()->id() == arrow::Type::STRING) {
            dict_values = std::static_pointer_cast<arrow::StringArray>(dict_keys->dictionary());
        } else {
            dict_large_values =
                std::static_pointer_cast<arrow::LargeStringArray>(dict_keys->dictionary());
        }
    } else {
        return Status::Invalid(
            fmt::format("FilterMapArrayBySelectedKeys only supports string keys or "
                        "dictionary<string|large_string> keys, got {}",
                        key_array->type()->ToString()));
    }

    auto values_array = map_array->items();
    int64_t num_maps = map_array->length();

    std::unordered_set<std::string> deduplicated;
    deduplicated.reserve(selected_keys.size());
    for (const auto& selected_key : selected_keys) {
        if (!deduplicated.insert(selected_key).second) {
            return Status::Invalid(fmt::format("Duplicate selected key '{}' in {} metadata",
                                               selected_key, DataField::MAP_SELECTED_KEYS));
        }
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> key_builder_u,
                                      arrow::MakeBuilder(arrow::utf8(), pool));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> value_builder_u,
                                      arrow::MakeBuilder(values_array->type(), pool));
    arrow::MapBuilder map_builder(pool, std::move(key_builder_u), std::move(value_builder_u));
    auto* key_builder = static_cast<arrow::StringBuilder*>(map_builder.key_builder());
    auto* value_builder = map_builder.item_builder();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder.Reserve(num_maps));

    for (int64_t map_idx = 0; map_idx < num_maps; ++map_idx) {
        if (map_array->IsNull(map_idx)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder.AppendNull());
            continue;
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder.Append());
        int64_t start = map_array->value_offset(map_idx);
        int64_t end = map_array->value_offset(map_idx + 1);

        // Keep selected keys in the exact selected_keys order.
        for (const auto& selected_key : selected_keys) {
            for (int64_t entry_idx = start; entry_idx < end; ++entry_idx) {
                std::string_view key_view;
                if (string_keys) {
                    if (string_keys->IsNull(entry_idx)) {
                        continue;
                    }
                    key_view = string_keys->GetView(entry_idx);
                } else {
                    if (dict_keys->IsNull(entry_idx)) {
                        continue;
                    }
                    int64_t dict_idx = dict_keys->GetValueIndex(entry_idx);
                    if (dict_values) {
                        if (dict_values->IsNull(dict_idx)) {
                            continue;
                        }
                        key_view = dict_values->GetView(dict_idx);
                    } else {
                        if (dict_large_values->IsNull(dict_idx)) {
                            continue;
                        }
                        key_view = dict_large_values->GetView(dict_idx);
                    }
                }
                if (key_view == selected_key) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Append(
                        key_view.data(), static_cast<int32_t>(key_view.size())));
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(
                        value_builder->AppendArraySlice(*values_array->data(), entry_idx, 1));
                }
            }
        }
    }

    std::shared_ptr<arrow::Array> result_map;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder.Finish(&result_map));
    return result_map;
}

}  // namespace paimon
