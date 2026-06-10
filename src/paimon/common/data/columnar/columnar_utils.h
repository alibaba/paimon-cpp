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

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string_view>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_primitive.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "paimon/memory/bytes.h"

namespace paimon {
class MemoryPool;

class ColumnarUtils {
 public:
    ColumnarUtils() = delete;
    ~ColumnarUtils() = delete;

    template <typename DataType, typename ValueType>
    static ValueType GetGenericValue(const arrow::Array* array, int32_t pos) {
        using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
        const auto* typed_array = arrow::internal::checked_cast<const ArrayType*>(array);
        assert(typed_array);
        return typed_array->Value(pos);
    }

    static std::string_view GetView(const arrow::Array* array, int32_t pos) {
        auto type_id = array->type_id();
        bool is_dict = (type_id == arrow::Type::type::DICTIONARY);
        if (!is_dict) {
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::BinaryArray*>(array);
            assert(typed_array);
            return typed_array->GetView(pos);
        } else {
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::DictionaryArray*>(array);
            assert(typed_array);
            auto dict_type =
                arrow::internal::checked_pointer_cast<arrow::DictionaryType>(array->type());
            assert(dict_type);
            auto value_type_id = dict_type->value_type()->id();
            auto index_type_id = dict_type->index_type()->id();
            int64_t dict_index = -1;
            if (index_type_id == arrow::Type::type::INT8) {
                auto indices =
                    arrow::internal::checked_cast<arrow::Int8Array*>(typed_array->indices().get());
                assert(indices);
                dict_index = indices->Value(pos);
            } else if (index_type_id == arrow::Type::type::INT16) {
                auto indices =
                    arrow::internal::checked_cast<arrow::Int16Array*>(typed_array->indices().get());
                assert(indices);
                dict_index = indices->Value(pos);
            } else if (index_type_id == arrow::Type::type::INT32) {
                auto indices =
                    arrow::internal::checked_cast<arrow::Int32Array*>(typed_array->indices().get());
                assert(indices);
                dict_index = indices->Value(pos);
            } else if (index_type_id == arrow::Type::type::INT64) {
                auto indices =
                    arrow::internal::checked_cast<arrow::Int64Array*>(typed_array->indices().get());
                assert(indices);
                dict_index = indices->Value(pos);
            }
            assert(dict_index >= 0);
            if (value_type_id == arrow::Type::type::STRING) {
                auto dictionary = arrow::internal::checked_cast<arrow::StringArray*>(
                    typed_array->dictionary().get());
                assert(dictionary);
                return dictionary->GetView(dict_index);
            } else if (value_type_id == arrow::Type::type::LARGE_STRING) {
                auto dictionary = arrow::internal::checked_cast<arrow::LargeStringArray*>(
                    typed_array->dictionary().get());
                assert(dictionary);
                return dictionary->GetView(dict_index);
            }
            assert(false);
            return std::string_view();
        }
    }

    template <typename DataType>
    static std::shared_ptr<Bytes> GetBytes(const arrow::Array* array, int32_t pos,
                                           MemoryPool* pool) {
        auto view = GetView(array, pos);
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(view.size(), pool);
        if (!view.empty()) {
            memcpy(bytes->data(), view.data(), view.size());
        }
        return bytes;
    }
};
}  // namespace paimon
