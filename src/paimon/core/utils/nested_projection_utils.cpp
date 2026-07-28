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
#include "arrow/array/util.h"
#include "arrow/compute/cast.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/variant_access_utils.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/casting/casting_utils.h"
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

Result<int32_t> NestedProjectionUtils::GetPaimonFieldId(
    const std::shared_ptr<arrow::Field>& field) {
    if (!field->HasMetadata() || !field->metadata()) {
        return Status::Invalid(fmt::format(
            "GetPaimonFieldId failed, do not exist metadata in field {}", field->name()));
    }
    auto result = field->metadata()->Get(DataField::FIELD_ID);
    if (!result.ok()) {
        return Status::Invalid(
            fmt::format("GetPaimonFieldId failed, cannot find field_id in metadata in field {}",
                        field->name()));
    }
    std::optional<int32_t> field_id = StringUtils::StringToValue<int32_t>(result.ValueUnsafe());
    if (!field_id) {
        return Status::Invalid(
            fmt::format("GetPaimonFieldId failed, cannot find convert field_id {} to int32",
                        result.ValueUnsafe()));
    }
    return field_id.value();
}

Result<std::shared_ptr<arrow::Field>> NestedProjectionUtils::FindFieldByPaimonId(
    const std::shared_ptr<arrow::DataType>& struct_type, int32_t field_id) {
    for (const auto& child : struct_type->fields()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t paimon_field_id, GetPaimonFieldId(child));
        if (paimon_field_id == field_id) {
            return child;
        }
    }
    return Status::Invalid(
        fmt::format("cannot find field {} in struct type {}", field_id, struct_type->ToString()));
}

Result<bool> NestedProjectionUtils::HasNestedSubfieldProjectionType(
    const std::shared_ptr<arrow::DataType>& file_type,
    const std::shared_ptr<arrow::DataType>& read_type) {
    switch (file_type->id()) {
        case arrow::Type::STRUCT: {
            if (VariantAccessUtils::IsVariantAccessType(read_type)) {
                // A variant-access projection is resolved by the variant read plans, not by
                // nested subfield projection.
                return false;
            }
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

namespace {

// Structural equality that also compares paimon field IDs on STRUCT children, so a
// drop+add of a same-name/same-type field (new ID) is not treated as a no-op.
Result<bool> EqualWithFieldIds(const std::shared_ptr<arrow::DataType>& a,
                               const std::shared_ptr<arrow::DataType>& b) {
    if (a->id() != b->id() || a->num_fields() != b->num_fields()) {
        return false;
    }
    if (a->num_fields() == 0) {
        return a->Equals(*b);
    }
    for (int32_t i = 0; i < a->num_fields(); ++i) {
        const auto& fa = a->field(i);
        const auto& fb = b->field(i);
        if (fa->nullable() != fb->nullable()) {
            return false;
        }
        if (a->id() == arrow::Type::STRUCT) {
            if (fa->name() != fb->name()) {
                return false;
            }
            // Compare IDs only when present (a map entry's key/value carry none).
            auto id_a = NestedProjectionUtils::GetPaimonFieldId(fa);
            auto id_b = NestedProjectionUtils::GetPaimonFieldId(fb);
            if (id_a.ok() && id_b.ok() && id_a.value() != id_b.value()) {
                return false;
            }
        }
        PAIMON_ASSIGN_OR_RAISE(bool child_equal, EqualWithFieldIds(fa->type(), fb->type()));
        if (!child_equal) {
            return false;
        }
    }
    return true;
}

/// Whether `read_type` is `data_type` with variant columns replaced by their variant-access
/// projections and nothing else changed (matching paimon field IDs).
Result<bool> IsVariantAccessSubstitution(const std::shared_ptr<arrow::DataType>& read_type,
                                         const std::shared_ptr<arrow::DataType>& data_type) {
    PAIMON_ASSIGN_OR_RAISE(bool equal, EqualWithFieldIds(read_type, data_type));
    if (equal) {
        return true;
    }
    if (VariantAccessUtils::IsVariantAccessType(read_type) &&
        VariantTypeUtils::IsUnshreddedVariantType(data_type)) {
        return true;
    }
    // Any other difference in shape, including a dropped field, is a real projection.
    if (read_type->id() != data_type->id() || read_type->num_fields() != data_type->num_fields()) {
        return false;
    }
    for (int32_t i = 0; i < read_type->num_fields(); ++i) {
        const std::shared_ptr<arrow::Field>& read_child = read_type->field(i);
        const std::shared_ptr<arrow::Field>& data_child = data_type->field(i);
        // LIST and MAP name their children by format convention, so only STRUCT is
        // matched by name and field ID.
        if (read_type->id() == arrow::Type::STRUCT) {
            if (read_child->name() != data_child->name()) {
                return false;
            }
            auto id_r = NestedProjectionUtils::GetPaimonFieldId(read_child);
            auto id_d = NestedProjectionUtils::GetPaimonFieldId(data_child);
            if (id_r.ok() && id_d.ok() && id_r.value() != id_d.value()) {
                return false;
            }
        }
        PAIMON_ASSIGN_OR_RAISE(bool sub,
                               IsVariantAccessSubstitution(read_child->type(), data_child->type()));
        if (!sub) {
            return false;
        }
    }
    return true;
}

// Reconcile a LIST/MAP item: read may ADD fields (evolution, null-filled
// downstream) but must not DROP one. Returns the file-readable item type;
// `container` names the container ("list"/"map") for the error message.
Result<std::shared_ptr<arrow::DataType>> PruneRepeatedItemType(
    const std::shared_ptr<arrow::DataType>& read_type,
    const std::shared_ptr<arrow::DataType>& data_type, const char* container) {
    PAIMON_ASSIGN_OR_RAISE(bool same, EqualWithFieldIds(read_type, data_type));
    if (same) {
        return data_type;
    }
    PAIMON_ASSIGN_OR_RAISE(bool substitution, IsVariantAccessSubstitution(read_type, data_type));
    if (substitution) {
        return read_type;
    }
    if (read_type->id() != data_type->id()) {
        return Status::Invalid(
            fmt::format("PruneDataType nested item type mismatch inside {}: read {} vs data {}",
                        container, read_type->ToString(), data_type->ToString()));
    }
    switch (data_type->id()) {
        case arrow::Type::STRUCT: {
            arrow::FieldVector item_fields;
            for (const auto& data_child : data_type->fields()) {
                PAIMON_ASSIGN_OR_RAISE(int32_t data_child_id,
                                       NestedProjectionUtils::GetPaimonFieldId(data_child));
                std::shared_ptr<arrow::Field> read_child;
                for (const auto& candidate : read_type->fields()) {
                    PAIMON_ASSIGN_OR_RAISE(int32_t candidate_id,
                                           NestedProjectionUtils::GetPaimonFieldId(candidate));
                    if (candidate_id == data_child_id) {
                        read_child = candidate;
                        break;
                    }
                }
                if (!read_child) {
                    // A file field is dropped -- a real partial projection.
                    return Status::Invalid(fmt::format(
                        "PruneDataType does not support partial projection inside {}: src {} vs "
                        "target {}",
                        container, data_type->ToString(), read_type->ToString()));
                }
                if (read_child->name() != data_child->name()) {
                    return Status::Invalid(fmt::format(
                        "PruneDataType does not support renaming inside {}: field id {} read '{}' "
                        "vs data '{}'",
                        container, data_child_id, read_child->name(), data_child->name()));
                }
                PAIMON_ASSIGN_OR_RAISE(
                    std::shared_ptr<arrow::DataType> item_child_type,
                    PruneRepeatedItemType(read_child->type(), data_child->type(), container));
                item_fields.push_back(data_child->WithType(item_child_type));
            }
            return arrow::struct_(item_fields);
        }
        case arrow::Type::LIST: {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> item,
                                   PruneRepeatedItemType(read_type->field(0)->type(),
                                                         data_type->field(0)->type(), container));
            return arrow::list(data_type->field(0)->WithType(item));
        }
        case arrow::Type::MAP: {
            auto read_map = std::static_pointer_cast<arrow::MapType>(read_type);
            auto data_map = std::static_pointer_cast<arrow::MapType>(data_type);
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::DataType> key,
                PruneRepeatedItemType(read_map->key_type(), data_map->key_type(), container));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::DataType> item,
                PruneRepeatedItemType(read_map->item_type(), data_map->item_type(), container));
            return std::static_pointer_cast<arrow::DataType>(std::make_shared<arrow::MapType>(
                data_map->key_field()->WithType(key), data_map->item_field()->WithType(item),
                data_map->keys_sorted()));
        }
        default:
            return data_type;
    }
}

}  // namespace

Result<std::optional<std::shared_ptr<arrow::DataType>>> NestedProjectionUtils::PruneDataType(
    const std::shared_ptr<arrow::DataType>& read_type,
    const std::shared_ptr<arrow::DataType>& data_type) {
    // Identical types (including paimon field IDs) need no pruning.
    PAIMON_ASSIGN_OR_RAISE(bool same, EqualWithFieldIds(read_type, data_type));
    if (same) {
        return std::optional<std::shared_ptr<arrow::DataType>>(data_type);
    }

    switch (read_type->id()) {
        case arrow::Type::STRUCT: {
            if (VariantAccessUtils::IsVariantAccessType(read_type) &&
                VariantTypeUtils::IsUnshreddedVariantType(data_type)) {
                // A variant-access projection replaces the variant column type; pass it through
                // so the read path extracts the described paths.
                return std::optional<std::shared_ptr<arrow::DataType>>(read_type);
            }
            arrow::FieldVector pruned_fields;
            for (const auto& read_child : read_type->fields()) {
                PAIMON_ASSIGN_OR_RAISE(int32_t read_child_id, GetPaimonFieldId(read_child));
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> data_child,
                                       FindFieldByPaimonId(data_type, read_child_id));
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
            PAIMON_ASSIGN_OR_RAISE(bool list_substitution,
                                   IsVariantAccessSubstitution(read_type, data_type));
            if (list_substitution) {
                return std::optional<std::shared_ptr<arrow::DataType>>(read_type);
            }
            // Added fields (schema evolution) are allowed; dropped fields still fail.
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> item,
                                   PruneRepeatedItemType(read_type->field(0)->type(),
                                                         data_type->field(0)->type(), "list"));
            return std::optional<std::shared_ptr<arrow::DataType>>(
                arrow::list(data_type->field(0)->WithType(item)));
        }
        case arrow::Type::MAP: {
            PAIMON_ASSIGN_OR_RAISE(bool map_substitution,
                                   IsVariantAccessSubstitution(read_type, data_type));
            if (map_substitution) {
                return std::optional<std::shared_ptr<arrow::DataType>>(read_type);
            }
            auto read_map = std::static_pointer_cast<arrow::MapType>(read_type);
            auto data_map = std::static_pointer_cast<arrow::MapType>(data_type);
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::DataType> key,
                PruneRepeatedItemType(read_map->key_type(), data_map->key_type(), "map"));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::DataType> item,
                PruneRepeatedItemType(read_map->item_type(), data_map->item_type(), "map"));
            return std::optional<std::shared_ptr<arrow::DataType>>(std::make_shared<arrow::MapType>(
                data_map->key_field()->WithType(key), data_map->item_field()->WithType(item),
                data_map->keys_sorted()));
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
    if (!field->HasMetadata() || !field->metadata()) {
        return result;
    }
    auto get_result = field->metadata()->Get(DataField::MAP_SELECTED_KEYS);
    if (!get_result.ok()) {
        return result;
    }
    auto tokens = StringUtils::Split(get_result.ValueUnsafe(), ",", /*ignore_empty=*/false);
    std::unordered_set<std::string> deduplicated;
    deduplicated.reserve(tokens.size());
    for (const auto& token : tokens) {
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
            accessor.dict_large_values =
                std::static_pointer_cast<arrow::LargeStringArray>(accessor.dict_keys->dictionary());
        }
        return accessor;
    }
    return Status::Invalid(
        fmt::format("FilterMapArrayBySelectedKeys only supports string keys or "
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

namespace {
// Strips physical-only differences from a leaf type: ORC lazy decoding wraps
// strings in a dictionary and may widen them to large_string. binary is not
// dictionary-encoded and large_binary is blob's real type, so neither is
// normalized. Two leaves with equal normalized types hold the same logical
// values.
std::shared_ptr<arrow::DataType> NormalizeLeafRepresentation(
    const std::shared_ptr<arrow::DataType>& type) {
    auto t = type;
    if (t->id() == arrow::Type::DICTIONARY) {
        t = std::static_pointer_cast<arrow::DictionaryType>(t)->value_type();
    }
    if (t->id() == arrow::Type::LARGE_STRING) {
        return arrow::utf8();
    }
    return t;
}
}  // namespace

Result<std::shared_ptr<arrow::Array>> NestedProjectionUtils::AlignArrayToReadType(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& read_type,
    arrow::MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(bool same, EqualWithFieldIds(array->type(), read_type));
    if (same) {
        return array;
    }
    // Produce exactly `read_type` so every file yields the same output type: rebuild
    // STRUCT/LIST/MAP with read-side types/nullability and cast a leaf (decodes dict).
    const auto& data = array->data();
    switch (read_type->id()) {
        case arrow::Type::STRUCT: {
            if (array->type()->id() != arrow::Type::STRUCT) {
                return Status::Invalid(fmt::format("AlignArrayToReadType cannot reconcile {} to {}",
                                                   array->type()->ToString(),
                                                   read_type->ToString()));
            }
            const auto& array_type = array->type();
            std::vector<std::shared_ptr<arrow::ArrayData>> children;
            children.reserve(read_type->num_fields());
            for (const auto& read_field : read_type->fields()) {
                // Match by name (parquet drops nested field-id metadata); if both
                // carry IDs they must agree, so a drop+add same-name field won't match.
                auto read_id = GetPaimonFieldId(read_field);
                int32_t match = -1;
                for (int32_t j = 0; j < array_type->num_fields(); j++) {
                    const auto& array_field = array_type->field(j);
                    if (array_field->name() != read_field->name()) {
                        continue;
                    }
                    auto data_id = GetPaimonFieldId(array_field);
                    if (read_id.ok() && data_id.ok() && read_id.value() != data_id.value()) {
                        continue;
                    }
                    match = j;
                    break;
                }
                if (match >= 0) {
                    auto child = arrow::MakeArray(data->child_data[match]);
                    PAIMON_ASSIGN_OR_RAISE(child,
                                           AlignArrayToReadType(child, read_field->type(), pool));
                    children.push_back(child->data());
                } else {
                    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                        std::shared_ptr<arrow::Array> null_child,
                        arrow::MakeArrayOfNull(read_field->type(), data->offset + data->length,
                                               pool));
                    children.push_back(null_child->data());
                }
            }
            auto new_data = data->Copy();
            new_data->type = read_type;
            new_data->child_data = std::move(children);
            return arrow::MakeArray(new_data);
        }
        case arrow::Type::LIST: {
            if (array->type()->id() != arrow::Type::LIST) {
                return Status::Invalid(fmt::format("AlignArrayToReadType cannot reconcile {} to {}",
                                                   array->type()->ToString(),
                                                   read_type->ToString()));
            }
            auto read_list = std::static_pointer_cast<arrow::ListType>(read_type);
            auto values = arrow::MakeArray(data->child_data[0]);
            PAIMON_ASSIGN_OR_RAISE(values,
                                   AlignArrayToReadType(values, read_list->value_type(), pool));
            auto new_data = data->Copy();
            new_data->type = read_type;
            new_data->child_data = {values->data()};
            return arrow::MakeArray(new_data);
        }
        case arrow::Type::MAP: {
            if (array->type()->id() != arrow::Type::MAP) {
                return Status::Invalid(fmt::format("AlignArrayToReadType cannot reconcile {} to {}",
                                                   array->type()->ToString(),
                                                   read_type->ToString()));
            }
            auto read_map = std::static_pointer_cast<arrow::MapType>(read_type);
            const auto& entries_data = data->child_data[0];
            auto key = arrow::MakeArray(entries_data->child_data[0]);
            auto value = arrow::MakeArray(entries_data->child_data[1]);
            PAIMON_ASSIGN_OR_RAISE(key, AlignArrayToReadType(key, read_map->key_type(), pool));
            PAIMON_ASSIGN_OR_RAISE(value, AlignArrayToReadType(value, read_map->item_type(), pool));
            auto new_entries = entries_data->Copy();
            new_entries->type = arrow::struct_({read_map->key_field(), read_map->item_field()});
            new_entries->child_data = {key->data(), value->data()};
            auto new_data = data->Copy();
            new_data->type = read_type;
            new_data->child_data = {new_entries};
            return arrow::MakeArray(new_data);
        }
        default: {
            // Leaf: only physical-representation differences are valid here (ORC
            // dictionary encoding, string/binary offset width). Genuine type
            // evolution is handled by FieldMappingReader's cast executors and
            // rejected upstream in PruneDataType, so fail anything else.
            if (!NormalizeLeafRepresentation(array->type())
                     ->Equals(*NormalizeLeafRepresentation(read_type))) {
                return Status::Invalid(
                    fmt::format("AlignArrayToReadType unsupported leaf type change: data {} vs "
                                "read {}",
                                array->type()->ToString(), read_type->ToString()));
            }
            return CastingUtils::Cast(array, read_type, arrow::compute::CastOptions::Safe(), pool);
        }
    }
}

}  // namespace paimon
