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
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/utils/jsonizable.h"
#include "paimon/result.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {
/// Defines the field of a row type.
class DataField : public Jsonizable<DataField> {
 public:
    DataField(int32_t id, const std::shared_ptr<arrow::Field>& field)
        : DataField(id, field, std::nullopt) {}

    DataField(int32_t id, const std::shared_ptr<arrow::Field>& field,
              const std::optional<std::string>& description)
        : id_(id), field_(field), description_(description) {}

    static constexpr char FIELD_ID[] = "paimon.id";
    static constexpr char DESCRIPTION[] = "paimon.description";
    /// Metadata key for map field selected keys. The value is a JSON array of
    /// string keys, e.g. '["key1","key2"]'. Only string-keyed maps are supported.
    static constexpr char MAP_SELECTED_KEYS[] = "paimon.map.selected-keys";

 public:
    static std::shared_ptr<arrow::Field> ConvertDataFieldToArrowField(const DataField& field);

    static std::shared_ptr<arrow::DataType> ConvertDataFieldsToArrowStructType(
        const std::vector<DataField>& data_fields);

    static std::shared_ptr<arrow::Schema> ConvertDataFieldsToArrowSchema(
        const std::vector<DataField>& data_fields);

    static Result<std::vector<DataField>> ConvertArrowSchemaToDataFields(
        const std::shared_ptr<arrow::Schema>& schema);

    static Result<DataField> ConvertArrowFieldToDataField(
        const std::shared_ptr<arrow::Field>& field);

    static std::vector<int32_t> GetAllFieldIds(const std::vector<DataField>& fields);

    static Result<std::vector<DataField>> ProjectFields(
        const std::vector<DataField>& fields,
        const std::optional<std::vector<std::string>>& projected_cols);

    int32_t Id() const {
        return id_;
    }

    const std::string& Name() const {
        return field_->name();
    }

    const std::shared_ptr<arrow::DataType>& Type() const {
        return field_->type();
    }

    const std::shared_ptr<arrow::Field>& ArrowField() const {
        return field_;
    }

    const std::optional<std::string>& Description() const {
        return description_;
    }

    bool Nullable() const {
        return field_->nullable();
    }

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;

    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    bool operator==(const DataField& other) const;

 private:
    JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(DataField);

 private:
    int32_t id_ = -1;
    std::shared_ptr<arrow::Field> field_;
    std::optional<std::string> description_;
};

}  // namespace paimon
