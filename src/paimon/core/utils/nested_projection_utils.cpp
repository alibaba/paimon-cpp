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

int32_t NestedProjectionUtils::GetPaimonFieldId(const std::shared_ptr<arrow::Field>& field) {
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

std::shared_ptr<arrow::Field> NestedProjectionUtils::FindFieldByPaimonId(
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

Result<bool> NestedProjectionUtils::HasNestedSubfieldProjectionType(
    const std::shared_ptr<arrow::DataType>& file_type,
    const std::shared_ptr<arrow::DataType>& read_type) {
    switch (file_type->id()) {
        case arrow::Type::STRUCT: {
            if (read_type->id() != arrow::Type::STRUCT) {
                return Status::Invalid(fmt::format(
                    "HasNestedSubfieldProjectionType requires same nested type kind, but file "
                    "type is {} and read type is {}",
                    file_type->ToString(), read_type->ToString()));
            }
            auto file_struct = std::static_pointer_cast<arrow::StructType>(file_type);
            auto read_struct = std::static_pointer_cast<arrow::StructType>(read_type);
            bool field_count_diff = read_struct->num_fields() != file_struct->num_fields();
            for (const auto& read_child : read_struct->fields()) {
                auto file_child = FindFieldByName(file_struct->fields(), read_child->name());
                if (!file_child) {
                    return Status::Invalid(fmt::format(
                        "HasNestedSubfieldProjectionType found requested struct child '{}' "
                        "missing in file type {}",
                        read_child->name(), file_type->ToString()));
                }
                PAIMON_ASSIGN_OR_RAISE(
                    bool child_has_nested_projection,
                    HasNestedSubfieldProjectionType(file_child->type(), read_child->type()));
                if (child_has_nested_projection) {
                    return true;
                }
            }
            return field_count_diff;
        }
        case arrow::Type::LIST: {
            if (read_type->id() != arrow::Type::LIST) {
                return Status::Invalid(fmt::format(
                    "HasNestedSubfieldProjectionType requires same nested type kind, but file "
                    "type is {} and read type is {}",
                    file_type->ToString(), read_type->ToString()));
            }
            auto file_list = std::static_pointer_cast<arrow::ListType>(file_type);
            auto read_list = std::static_pointer_cast<arrow::ListType>(read_type);
            return HasNestedSubfieldProjectionType(file_list->value_type(),
                                                   read_list->value_type());
        }
        case arrow::Type::MAP: {
            if (read_type->id() != arrow::Type::MAP) {
                return Status::Invalid(fmt::format(
                    "HasNestedSubfieldProjectionType requires same nested type kind, but file "
                    "type is {} and read type is {}",
                    file_type->ToString(), read_type->ToString()));
            }
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
                if (read_child_id < 0) {
                    return Status::Invalid(fmt::format(
                        "PruneDataType requires paimon.id for nested struct field '{}', but it "
                        "is missing or invalid",
                        read_child->name()));
                }
                std::shared_ptr<arrow::Field> data_child =
                    FindFieldByPaimonId(data_type, read_child_id);
                if (!data_child) {
                    return Status::Invalid(fmt::format(
                        "PruneDataType does not support schema evolution inside struct: nested "
                        "field '{}' (id={}) does not exist in data type {}",
                        read_child->name(), read_child_id, data_type->ToString()));
                }
                if (read_child->name() != data_child->name()) {
                    return Status::Invalid(fmt::format(
                        "PruneDataType does not support schema evolution inside struct: nested "
                        "field id {} name mismatch: read '{}' vs data '{}'",
                        read_child_id, read_child->name(), data_child->name()));
                }
                if (read_child->type()->id() != data_child->type()->id()) {
                    return Status::Invalid(fmt::format(
                        "PruneDataType nested field type mismatch for '{}': read {} vs data {}",
                        read_child->name(), read_child->type()->ToString(),
                        data_child->type()->ToString()));
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
            // Keep behavior aligned with format readers: partial projection inside
            // LIST is unsupported and must fail fast.
            return Status::Invalid(fmt::format(
                "PruneDataType does not support partial projection inside list: src {} vs target {}",
                data_type->ToString(), read_type->ToString()));
        }

        case arrow::Type::MAP: {
            // Keep behavior aligned with format readers: partial projection inside
            // MAP is unsupported and must fail fast.
            return Status::Invalid(fmt::format(
                "PruneDataType does not support partial projection inside map: src {} vs target {}",
                data_type->ToString(), read_type->ToString()));
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
            return Status::Invalid(fmt::format(
                "HasNestedSubfieldProjection found read field '{}' missing in file schema {}",
                read_field->name(), file_schema->ToString()));
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

namespace {

struct MapKeyAccessor {
    std::shared_ptr<arrow::StringArray> string_keys;
    std::shared_ptr<arrow::DictionaryArray> dict_keys;
    std::shared_ptr<arrow::StringArray> dict_values;
    std::shared_ptr<arrow::LargeStringArray> dict_large_values;
};

Result<MapKeyAccessor> BuildMapKeyAccessor(const std::shared_ptr<arrow::Array>& key_array) {
    MapKeyAccessor accessor;
    if (key_array->type_id() == arrow::Type::STRING) {
        accessor.string_keys = std::static_pointer_cast<arrow::StringArray>(key_array);
        return accessor;
    }
    if (key_array->type_id() == arrow::Type::DICTIONARY) {
        auto dict_type = std::static_pointer_cast<arrow::DictionaryType>(key_array->type());
        if (dict_type->value_type()->id() != arrow::Type::STRING &&
            dict_type->value_type()->id() != arrow::Type::LARGE_STRING) {
            return Status::Invalid(
                fmt::format("FilterMapArrayBySelectedKeys only supports string keys or "
                            "dictionary<string|large_string> keys, got {}",
                            key_array->type()->ToString()));
        }
        accessor.dict_keys = std::static_pointer_cast<arrow::DictionaryArray>(key_array);
        if (dict_type->value_type()->id() == arrow::Type::STRING) {
            accessor.dict_values =
                std::static_pointer_cast<arrow::StringArray>(accessor.dict_keys->dictionary());
        } else {
            accessor.dict_large_values = std::static_pointer_cast<arrow::LargeStringArray>(
                accessor.dict_keys->dictionary());
        }
        return accessor;
    }
    return Status::Invalid(fmt::format(
        "FilterMapArrayBySelectedKeys only supports string keys or "
        "dictionary<string|large_string> keys, got {}",
        key_array->type()->ToString()));
}

Result<std::string_view> GetMapKeyViewAt(const MapKeyAccessor& accessor, int64_t entry_idx) {
    if (accessor.string_keys) {
        if (accessor.string_keys->IsNull(entry_idx)) {
            return Status::Invalid("FilterMapArrayBySelectedKeys found null map key at entry " +
                                   std::to_string(entry_idx));
        }
        return accessor.string_keys->GetView(entry_idx);
    }

    if (accessor.dict_keys->IsNull(entry_idx)) {
        return Status::Invalid("FilterMapArrayBySelectedKeys found null map key at entry " +
                               std::to_string(entry_idx));
    }
    int64_t dict_idx = accessor.dict_keys->GetValueIndex(entry_idx);
    if (accessor.dict_values) {
        if (accessor.dict_values->IsNull(dict_idx)) {
            return Status::Invalid(
                "FilterMapArrayBySelectedKeys found null dictionary map key at dictionary index " +
                std::to_string(dict_idx));
        }
        return accessor.dict_values->GetView(dict_idx);
    }

    if (accessor.dict_large_values->IsNull(dict_idx)) {
        return Status::Invalid(
            "FilterMapArrayBySelectedKeys found null dictionary map key at dictionary index " +
            std::to_string(dict_idx));
    }
    return accessor.dict_large_values->GetView(dict_idx);
}

}  // namespace

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
    PAIMON_ASSIGN_OR_RAISE(MapKeyAccessor key_accessor, BuildMapKeyAccessor(key_array));

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
                PAIMON_ASSIGN_OR_RAISE(std::string_view key_view,
                                       GetMapKeyViewAt(key_accessor, entry_idx));
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
