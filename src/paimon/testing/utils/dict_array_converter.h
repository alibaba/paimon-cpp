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

#include "arrow/api.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/result.h"

namespace paimon::test {
class DictArrayConverter {
 public:
    DictArrayConverter() = delete;
    ~DictArrayConverter() = delete;

    // deep copy dictionary array to string array/binary array
    static Result<std::shared_ptr<arrow::Array>> ConvertDictArray(
        const std::shared_ptr<arrow::Array>& array, arrow::MemoryPool* pool) {
        arrow::Type::type kind = array->type_id();
        switch (kind) {
            case arrow::Type::type::STRUCT: {
                // convert array
                auto struct_array =
                    arrow::internal::checked_pointer_cast<arrow::StructArray>(array);
                arrow::ArrayVector new_children;
                std::size_t size = struct_array->fields().size();
                for (size_t i = 0; i < size; i++) {
                    std::shared_ptr<arrow::Array> child = struct_array->field(static_cast<int>(i));
                    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> new_child,
                                           ConvertDictArray(child, pool));
                    new_children.push_back(new_child);
                }

                // convert type
                arrow::FieldVector fields;
                fields.reserve(new_children.size());
                for (size_t i = 0; i < new_children.size(); i++) {
                    // Note: For test consistency, intentionally left nullable unspecified, as ORC
                    // discard nullable information, making it impossible to align.
                    // Moreover, this detail is currently not important for users.
                    fields.push_back(arrow::field(struct_array->type()->field(i)->name(),
                                                  new_children[i]->type()));
                }

                return std::make_shared<arrow::StructArray>(arrow::struct_(fields),
                                                            struct_array->length(), new_children,
                                                            struct_array->null_bitmap());
            }
            case arrow::Type::type::LIST: {
                auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(array);
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> value_array,
                                       ConvertDictArray(list_array->values(), pool));
                return std::make_shared<arrow::ListArray>(
                    arrow::list(value_array->type()), list_array->length(),
                    list_array->value_offsets(), value_array, list_array->null_bitmap(),
                    list_array->null_count(), list_array->offset());
            }
            case arrow::Type::type::MAP: {
                auto map_array = arrow::internal::checked_pointer_cast<arrow::MapArray>(array);
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> key_array,
                                       ConvertDictArray(map_array->keys(), pool));
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> item_array,
                                       ConvertDictArray(map_array->items(), pool));
                auto map_type =
                    arrow::internal::checked_pointer_cast<arrow::MapType>(map_array->type());
                auto new_map_type = std::make_shared<arrow::MapType>(
                    key_array->type(), item_array->type(), map_type->keys_sorted());
                return std::make_shared<arrow::MapArray>(
                    new_map_type, map_array->length(), map_array->value_offsets(), key_array,
                    item_array, map_array->null_bitmap(), map_array->null_count(),
                    map_array->offset());
            }
            case arrow::Type::type::DICTIONARY: {
                auto dict_array =
                    arrow::internal::checked_pointer_cast<arrow::DictionaryArray>(array);
                auto dict_type = arrow::internal::checked_pointer_cast<arrow::DictionaryType>(
                    dict_array->type());
                auto value_type_id = dict_type->value_type()->id();
                auto index_type_id = dict_type->index_type()->id();
                if (value_type_id == arrow::Type::type::STRING &&
                    index_type_id == arrow::Type::type::INT32) {
                    return ConvertDictionaryArrayToBinaryArray<
                        arrow::StringArray, arrow::Int32Array, arrow::StringBuilder>(dict_array,
                                                                                     pool);
                } else if (value_type_id == arrow::Type::type::LARGE_STRING &&
                           index_type_id == arrow::Type::type::INT64) {
                    return ConvertDictionaryArrayToBinaryArray<
                        arrow::LargeStringArray, arrow::Int64Array, arrow::StringBuilder>(
                        dict_array, pool);
                } else {
                    return Status::Invalid(
                        "only support [STRING, INT32] or [LARGE_STRING, INT64] for "
                        "DictionaryArray");
                }
            }
            default: {
                return array;
            }
        }
    }

 private:
    template <typename DictArrayType, typename IndicesArrayType, typename BuilderType>
    static Result<std::shared_ptr<arrow::Array>> ConvertDictionaryArrayToBinaryArray(
        const std::shared_ptr<arrow::DictionaryArray>& dict_array, arrow::MemoryPool* pool) {
        auto dictionary = std::dynamic_pointer_cast<DictArrayType>(dict_array->dictionary());
        auto indices = std::dynamic_pointer_cast<IndicesArrayType>(dict_array->indices());
        auto string_builder = std::make_shared<BuilderType>(pool);
        for (int64_t i = 0; i < dict_array->length(); ++i) {
            if (dict_array->IsNull(i)) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(string_builder->AppendNull());
            } else {
                int64_t dict_index = indices->Value(i);
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    string_builder->Append(dictionary->GetString(dict_index)));
            }
        }
        std::shared_ptr<arrow::Array> string_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(string_builder->Finish(&string_array));
        return string_array;
    }
};
}  // namespace paimon::test
