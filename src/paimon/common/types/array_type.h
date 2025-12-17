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

class ArrayType : public DataType {
 public:
    static constexpr char TYPE[] = "ARRAY";

    ArrayType(const std::shared_ptr<arrow::DataType>& type, bool nullable,
              const std::shared_ptr<const arrow::KeyValueMetadata>& metadata)
        : DataType(type, nullable, metadata) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember(
            rapidjson::StringRef("type"),
            RapidJsonUtil::SerializeValue(WithNullable(std::string(TYPE)), allocator).Move(),
            *allocator);
        auto type = arrow::internal::checked_cast<arrow::ListType*>(type_.get());
        auto value_field = type->value_field();

        std::shared_ptr<DataType> data_type =
            DataType::Create(value_field->type(), value_field->nullable(), /*metadata=*/nullptr);
        obj.AddMember(rapidjson::StringRef("element"),
                      RapidJsonUtil::SerializeValue(*data_type, allocator).Move(), *allocator);
        return obj;
    }
};

}  // namespace paimon
