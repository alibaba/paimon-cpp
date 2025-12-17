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
#include <string>

#include "arrow/api.h"
#include "paimon/common/types/data_type.h"
#include "paimon/common/utils/rapidjson_util.h"

namespace paimon {

// NOTE:
// In contrast to Java Paimon, this C++ implementation does NOT allow nullable keys in MapType.
// This restriction is due to the limitations of Apache Arrow, which does not support nullable map
// keys.
//
// When using this class, ensure that the key type is always non-nullable.
// Attempting to use a nullable key will result in incorrect behavior or runtime errors.
class MapType : public DataType {
 public:
    static constexpr char TYPE[] = "MAP";

    MapType(const std::shared_ptr<arrow::DataType>& type, bool nullable,
            const std::shared_ptr<const arrow::KeyValueMetadata>& metadata)
        : DataType(type, nullable, metadata) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember(
            rapidjson::StringRef("type"),
            RapidJsonUtil::SerializeValue(WithNullable(std::string(TYPE)), allocator).Move(),
            *allocator);
        auto type = arrow::internal::checked_cast<arrow::MapType*>(type_.get());
        auto key_field = type->key_field();
        std::shared_ptr<DataType> key_data_type =
            DataType::Create(key_field->type(), key_field->nullable(), /*metadata=*/nullptr);
        obj.AddMember(rapidjson::StringRef("key"),
                      RapidJsonUtil::SerializeValue(*key_data_type, allocator).Move(), *allocator);
        auto value_field = type->item_field();
        std::shared_ptr<DataType> value_data_type =
            DataType::Create(value_field->type(), value_field->nullable(), /*metadata=*/nullptr);
        obj.AddMember(rapidjson::StringRef("value"),
                      RapidJsonUtil::SerializeValue(*value_data_type, allocator).Move(),
                      *allocator);
        return obj;
    }
};

}  // namespace paimon
