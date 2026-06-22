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

#include "paimon/common/types/data_field.h"

#include <cassert>
#include <map>
#include <stdexcept>
#include <unordered_map>
#include <utility>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/types/data_type.h"
#include "paimon/common/types/data_type_json_parser.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/status.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {

bool DataField::operator==(const DataField& other) const {
    return id_ == other.id_ && field_->Equals(other.field_, /*check_metadata=*/false) &&
           description_ == other.description_;
}

rapidjson::Value DataField::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef("id"), RapidJsonUtil::SerializeValue(id_, allocator).Move(),
                  *allocator);
    obj.AddMember(rapidjson::StringRef("name"),
                  RapidJsonUtil::SerializeValue(field_->name(), allocator).Move(), *allocator);
    std::shared_ptr<DataType> data_type =
        DataType::Create(field_->type(), field_->nullable(), field_->metadata());
    obj.AddMember(rapidjson::StringRef("type"),
                  RapidJsonUtil::SerializeValue(*data_type, allocator).Move(), *allocator);
    if (description_ != std::nullopt) {
        obj.AddMember(rapidjson::StringRef("description"),
                      RapidJsonUtil::SerializeValue(description_.value(), allocator).Move(),
                      *allocator);
    }
    return obj;
}

void DataField::FromJson(const rapidjson::Value& obj) noexcept(false) {
    id_ = RapidJsonUtil::DeserializeKeyValue<int32_t>(obj, "id");
    auto name = RapidJsonUtil::DeserializeKeyValue<std::string>(obj, "name");
    assert(obj.IsObject());
    if (!obj.HasMember("type")) {
        throw std::invalid_argument("key 'type' must exist");
    }
    auto field_result = DataTypeJsonParser::ParseType(name, obj["type"]);
    if (!field_result.ok()) {
        throw std::invalid_argument(
            fmt::format("parse data type failed, error msg: {}", field_result.status().ToString()));
    }
    field_ = field_result.value();
    assert(field_);
    description_ = RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(
        obj, "description", description_);
}

std::shared_ptr<arrow::Field> DataField::ConvertDataFieldToArrowField(const DataField& field) {
    std::unordered_map<std::string, std::string> metadata_map;
    if (field.field_->HasMetadata()) {
        field.field_->metadata()->ToUnorderedMap(&metadata_map);
    }
    metadata_map[std::string(DataField::FIELD_ID)] = std::to_string(field.Id());
    if (field.Description() && !field.Description().value().empty()) {
        metadata_map[DataField::DESCRIPTION] = field.Description().value();
    }
    auto metadata = std::make_shared<arrow::KeyValueMetadata>(metadata_map);
    return std::make_shared<arrow::Field>(field.Name(), field.Type(), field.Nullable(), metadata);
}

std::shared_ptr<arrow::DataType> DataField::ConvertDataFieldsToArrowStructType(
    const std::vector<DataField>& data_fields) {
    arrow::FieldVector arrow_fields;
    arrow_fields.reserve(data_fields.size());
    for (const auto& field : data_fields) {
        arrow_fields.push_back(ConvertDataFieldToArrowField(field));
    }
    return arrow::struct_(arrow_fields);
}

std::shared_ptr<arrow::Schema> DataField::ConvertDataFieldsToArrowSchema(
    const std::vector<DataField>& data_fields) {
    auto data_type = ConvertDataFieldsToArrowStructType(data_fields);
    return arrow::schema(data_type->fields());
}

Result<std::vector<DataField>> DataField::ConvertArrowSchemaToDataFields(
    const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<DataField> fields;
    fields.reserve(schema->num_fields());
    for (const auto& arrow_field : schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE(DataField field, ConvertArrowFieldToDataField(arrow_field));
        fields.push_back(field);
    }
    return fields;
}

Result<DataField> DataField::ConvertArrowFieldToDataField(
    const std::shared_ptr<arrow::Field>& field) {
    if (!field->HasMetadata() || !field->metadata()) {
        return Status::Invalid(fmt::format(
            "invalid read schema, lack of metadata of field id, field name '{}'", field->name()));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::string field_id_str,
                                      field->metadata()->Get(DataField::FIELD_ID));
    std::optional<int32_t> field_id = StringUtils::StringToValue<int32_t>(field_id_str);
    if (field_id == std::nullopt) {
        return Status::Invalid(
            fmt::format("invalid read schema, cannot cast field id {} to int, field name '{}'",
                        field_id_str, field->name()));
    }
    std::optional<std::string> description;
    auto description_result = field->metadata()->Get(DataField::DESCRIPTION);
    if (description_result.ok()) {
        description = description_result.ValueUnsafe();
    }

    return DataField(field_id.value(), field, description);
}

std::vector<int32_t> DataField::GetAllFieldIds(const std::vector<DataField>& fields) {
    std::vector<int32_t> ids;
    ids.reserve(fields.size());
    for (const auto& field : fields) {
        ids.push_back(field.Id());
    }
    return ids;
}

Result<std::vector<DataField>> DataField::ProjectFields(
    const std::vector<DataField>& fields,
    const std::optional<std::vector<std::string>>& projected_cols) {
    std::vector<DataField> projected_fields;
    if (projected_cols == std::nullopt) {
        projected_fields = fields;
    } else {
        // field name to field idx
        std::map<std::string, int32_t> field_idx_map = ObjectUtils::CreateIdentifierToIndexMap(
            fields, [](const DataField& field) -> std::string { return field.Name(); });
        for (const auto& projected_col : projected_cols.value()) {
            auto iter = field_idx_map.find(projected_col);
            if (iter == field_idx_map.end()) {
                return Status::Invalid(
                    fmt::format("projected field {} not in src field set", projected_col));
            }
            projected_fields.push_back(fields[iter->second]);
        }
    }
    return projected_fields;
}

std::shared_ptr<arrow::Field> DataField::MergeFieldMetadataByWhitelist(
    const std::shared_ptr<arrow::Field>& target_field,
    const std::shared_ptr<arrow::Field>& source_field,
    const std::vector<std::string>& metadata_keys_whitelist) {
    if (!source_field || !source_field->HasMetadata() || !source_field->metadata()) {
        return target_field;
    }

    std::unordered_map<std::string, std::string> metadata_map;
    for (const auto& key : metadata_keys_whitelist) {
        auto metadata_value_result = source_field->metadata()->Get(key);
        if (metadata_value_result.ok()) {
            metadata_map[key] = metadata_value_result.ValueUnsafe();
        }
    }

    if (metadata_map.empty()) {
        return target_field;
    }

    auto metadata = std::make_shared<arrow::KeyValueMetadata>(metadata_map);
    return target_field->WithMergedMetadata(metadata);
}

DataField DataField::MergeFieldMetadataByWhitelist(
    const DataField& target_field, const DataField& source_field,
    const std::vector<std::string>& metadata_keys_whitelist) {
    return DataField(target_field.Id(),
                     MergeFieldMetadataByWhitelist(target_field.ArrowField(),
                                                   source_field.ArrowField(),
                                                   metadata_keys_whitelist),
                     target_field.Description());
}

}  // namespace paimon
