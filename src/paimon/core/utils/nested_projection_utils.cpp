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

#include "paimon/core/utils/nested_projection_utils.h"

#include <string>
#include <utility>

#include "arrow/array/array_nested.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/status.h"

namespace paimon {

Result<std::optional<std::shared_ptr<arrow::DataType>>> PruneDataType(
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
            PAIMON_ASSIGN_OR_RAISE(
                std::optional<std::shared_ptr<arrow::DataType>> pruned_elem,
                PruneDataType(read_list.value_type(), data_list.value_type()));
            if (!pruned_elem.has_value()) {
                return std::optional<std::shared_ptr<arrow::DataType>>(std::nullopt);
            }
            std::shared_ptr<arrow::DataType> result_type = arrow::list(
                arrow::field(data_list.value_field()->name(), pruned_elem.value(),
                             data_list.value_field()->nullable(),
                             data_list.value_field()->metadata()));
            return std::optional<std::shared_ptr<arrow::DataType>>(std::move(result_type));
        }

        case arrow::Type::MAP: {
            const auto& read_map = static_cast<const arrow::MapType&>(*read_type);
            const auto& data_map = static_cast<const arrow::MapType&>(*data_type);
            PAIMON_ASSIGN_OR_RAISE(
                std::optional<std::shared_ptr<arrow::DataType>> pruned_key,
                PruneDataType(read_map.key_type(), data_map.key_type()));
            PAIMON_ASSIGN_OR_RAISE(
                std::optional<std::shared_ptr<arrow::DataType>> pruned_value,
                PruneDataType(read_map.item_type(), data_map.item_type()));
            if (!pruned_key.has_value() || !pruned_value.has_value()) {
                return std::optional<std::shared_ptr<arrow::DataType>>(std::nullopt);
            }
            std::shared_ptr<arrow::DataType> result_type =
                arrow::map(pruned_key.value(), pruned_value.value(),
                           data_map.key_field()->nullable());
            return std::optional<std::shared_ptr<arrow::DataType>>(std::move(result_type));
        }

        default:
            // Atomic type: return data_type as-is (type evolution is handled
            // separately by CastExecutor).
            return std::optional<std::shared_ptr<arrow::DataType>>(data_type);
    }
}

// ---------------------------------------------------------------------------
// PruneArray — fallback for format readers that return extra nested columns
// ---------------------------------------------------------------------------

Result<std::shared_ptr<arrow::Array>> PruneArray(
    const std::shared_ptr<arrow::Array>& array,
    const std::shared_ptr<arrow::DataType>& target_type) {
    if (!array || array->type()->Equals(target_type)) {
        return array;
    }

    switch (target_type->id()) {
        case arrow::Type::STRUCT: {
            auto struct_array = std::static_pointer_cast<arrow::StructArray>(array);
            arrow::ArrayVector pruned_children;
            arrow::FieldVector pruned_fields;
            for (const auto& target_field : target_type->fields()) {
                std::shared_ptr<arrow::Array> child =
                    struct_array->GetFieldByName(target_field->name());
                if (!child) {
                    return Status::Invalid(fmt::format(
                        "PruneArray: field '{}' not found in struct array", target_field->name()));
                }
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> pruned_child,
                                       PruneArray(child, target_field->type()));
                pruned_children.push_back(std::move(pruned_child));
                pruned_fields.push_back(target_field);
            }
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::StructArray> result_struct,
                arrow::StructArray::Make(pruned_children, pruned_fields,
                                         struct_array->null_bitmap(),
                                         struct_array->null_count(), struct_array->offset()));
            return std::static_pointer_cast<arrow::Array>(result_struct);
        }

        case arrow::Type::LIST: {
            auto list_array = std::static_pointer_cast<arrow::ListArray>(array);
            const auto& target_elem_type =
                static_cast<const arrow::ListType&>(*target_type).value_type();
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> pruned_values,
                                   PruneArray(list_array->values(), target_elem_type));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::ListArray> result_list,
                arrow::ListArray::FromArrays(
                    *list_array->offsets(), *pruned_values, arrow::default_memory_pool(),
                    list_array->null_bitmap(), list_array->null_count()));
            return std::static_pointer_cast<arrow::Array>(result_list);
        }

        case arrow::Type::MAP: {
            auto map_array = std::static_pointer_cast<arrow::MapArray>(array);
            const auto& target_map_type = static_cast<const arrow::MapType&>(*target_type);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> pruned_keys,
                                   PruneArray(map_array->keys(), target_map_type.key_type()));
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> pruned_items,
                                   PruneArray(map_array->items(), target_map_type.item_type()));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> result_map,
                arrow::MapArray::FromArrays(map_array->offsets(), pruned_keys, pruned_items,
                                            arrow::default_memory_pool()));
            return result_map;
        }

        default:
            // Atomic type — no pruning needed.
            return array;
    }
}

}  // namespace paimon
