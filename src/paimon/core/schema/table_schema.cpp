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

#include "paimon/core/schema/table_schema.h"

#include <algorithm>
#include <iterator>
#include <set>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/defs.h"
#include "paimon/status.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {

Result<std::unique_ptr<TableSchema>> TableSchema::Create(
    int64_t schema_id, const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<std::string>& partition_keys, const std::vector<std::string>& primary_keys,
    const std::map<std::string, std::string>& options) {
    if (schema_id != 0) {
        return Status::NotImplemented("do not support schema evolution, schema_id must be 0");
    }
    std::vector<DataField> data_fields;
    int32_t field_id = 0;
    std::set<std::string> primary_key_set;
    for (const auto& primary_key : primary_keys) {
        primary_key_set.insert(primary_key);
    }
    for (const auto& field : schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE(auto field_with_id,
                               AssignFieldIdsRecursively(field, /*set_field_id=*/true, &field_id));
        if (primary_key_set.count(field_with_id->name())) {
            field_with_id = field_with_id->WithNullable(false);
        }
        PAIMON_ASSIGN_OR_RAISE(DataField data_field,
                               DataField::ConvertArrowFieldToDataField(field_with_id));
        data_fields.push_back(data_field);
    }
    return InitSchema(schema_id, data_fields, field_id - 1, partition_keys, primary_keys, options,
                      DateTimeUtils::GetCurrentUTCTimeUs() / 1000);
}

Result<std::shared_ptr<arrow::KeyValueMetadata>> TableSchema::MakeMetaDataWithFieldId(
    const std::shared_ptr<arrow::Field>& field, int32_t field_id) {
    std::vector<std::string> keys = {std::string(DataField::FIELD_ID)};
    std::vector<std::string> values = {std::to_string(field_id)};
    std::shared_ptr<arrow::KeyValueMetadata> metadata = arrow::KeyValueMetadata::Make(keys, values);
    if (field->HasMetadata() && field->metadata()) {
        if (field->metadata()->Contains(DataField::FIELD_ID)) {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::string field_id_result,
                                              field->metadata()->Get(DataField::FIELD_ID));
            if (std::to_string(field_id) != field_id_result) {
                return Status::Invalid(fmt::format("field id {} not match with {} {} in metadata",
                                                   field_id, DataField::FIELD_ID, field_id_result));
            }
        }
        metadata = metadata->Merge(*field->metadata());
    }
    return metadata;
}

Result<std::shared_ptr<arrow::Field>> TableSchema::AssignFieldIdsRecursively(
    const std::shared_ptr<arrow::Field>& field, bool assign_id_to_self, int32_t* field_id) {
    std::shared_ptr<arrow::KeyValueMetadata> metadata;
    if (assign_id_to_self) {
        PAIMON_ASSIGN_OR_RAISE(metadata, MakeMetaDataWithFieldId(field, *field_id));
        (*field_id)++;
    }
    auto type = field->type();
    if (type->id() == arrow::Type::STRUCT) {
        auto struct_type = arrow::internal::checked_pointer_cast<arrow::StructType>(field->type());
        arrow::FieldVector new_childs;
        for (const auto& child : struct_type->fields()) {
            PAIMON_ASSIGN_OR_RAISE(
                auto new_child, AssignFieldIdsRecursively(child, /*set_field_id=*/true, field_id));
            new_childs.push_back(new_child);
        }
        return arrow::field(field->name(), arrow::struct_(new_childs), field->nullable(), metadata);
    } else if (type->id() == arrow::Type::LIST) {
        auto list_type = arrow::internal::checked_pointer_cast<arrow::ListType>(field->type());
        PAIMON_ASSIGN_OR_RAISE(auto new_value_field,
                               AssignFieldIdsRecursively(list_type->value_field(),
                                                         /*set_field_id=*/false, field_id));
        return arrow::field(field->name(), arrow::list(new_value_field), field->nullable(),
                            metadata);
    } else if (field->type()->id() == arrow::Type::MAP) {
        auto map_type = arrow::internal::checked_pointer_cast<arrow::MapType>(field->type());
        std::shared_ptr<arrow::Field> key_field = map_type->key_field();
        std::shared_ptr<arrow::Field> value_field = map_type->item_field();
        PAIMON_ASSIGN_OR_RAISE(
            key_field, AssignFieldIdsRecursively(key_field, /*set_field_id=*/false, field_id));
        PAIMON_ASSIGN_OR_RAISE(
            value_field, AssignFieldIdsRecursively(value_field, /*set_field_id=*/false, field_id));
        return arrow::field(field->name(), arrow::map(key_field->type(), value_field),
                            field->nullable(), metadata);
    }
    return metadata ? field->WithMergedMetadata(metadata) : field;
}

rapidjson::Value TableSchema::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef("version"),
                  RapidJsonUtil::SerializeValue(version_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef("id"), RapidJsonUtil::SerializeValue(id_, allocator).Move(),
                  *allocator);
    obj.AddMember(rapidjson::StringRef("fields"),
                  RapidJsonUtil::SerializeValue(fields_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef("highestFieldId"),
                  RapidJsonUtil::SerializeValue(highest_field_id_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef("partitionKeys"),
                  RapidJsonUtil::SerializeValue(partition_keys_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef("primaryKeys"),
                  RapidJsonUtil::SerializeValue(primary_keys_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef("options"),
                  RapidJsonUtil::SerializeValue(options_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef("timeMillis"),
                  RapidJsonUtil::SerializeValue(time_millis_, allocator).Move(), *allocator);
    if (comment_) {
        obj.AddMember(rapidjson::StringRef("comment"),
                      RapidJsonUtil::SerializeValue(comment_, allocator).Move(), *allocator);
    }
    return obj;
}

TableSchema::TableSchema(int32_t version, int64_t id, const std::vector<DataField>& fields,
                         int32_t highest_field_id, const std::vector<std::string>& partition_keys,
                         const std::vector<std::string>& primary_keys,
                         const std::map<std::string, std::string>& options, int64_t time_millis)
    : version_(version),
      id_(id),
      fields_(fields),
      highest_field_id_(highest_field_id),
      partition_keys_(partition_keys),
      primary_keys_(primary_keys),
      options_(options),
      time_millis_(time_millis) {}

bool TableSchema::operator==(const TableSchema& other) const {
    return version_ == other.version_ && fields_ == other.fields_ &&
           partition_keys_ == other.partition_keys_ && primary_keys_ == other.primary_keys_ &&
           options_ == other.options_ && comment_ == other.comment_ &&
           time_millis_ == other.time_millis_;
}
std::vector<std::string> TableSchema::FieldNames() const {
    std::vector<std::string> field_names;
    field_names.reserve(fields_.size());
    std::transform(fields_.begin(), fields_.end(), std::back_inserter(field_names),
                   [](const DataField& field) { return field.Name(); });
    return field_names;
}

Result<DataField> TableSchema::GetField(const std::string& field_name) const {
    for (const auto& field : Fields()) {
        if (field.Name() == field_name) {
            return field;
        }
    }
    return Status::Invalid(
        fmt::format("Get field {} failed: not exist in table schema", field_name));
}

Result<DataField> TableSchema::GetField(int32_t field_id) const {
    for (const auto& field : Fields()) {
        if (field.Id() == field_id) {
            return field;
        }
    }
    return Status::Invalid(
        fmt::format("Get field with id {} failed: not exist in table schema", field_id));
}

Result<std::vector<DataField>> TableSchema::TrimmedPrimaryKeyFields() const {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys, TrimmedPrimaryKeys());
    std::vector<DataField> data_fields;
    for (const auto& trimmed_primary_key : trimmed_primary_keys) {
        for (const auto& field : fields_) {
            if (field.Name() == trimmed_primary_key) {
                data_fields.emplace_back(field);
                break;
            }
        }
    }
    return data_fields;
}

Result<std::unique_ptr<TableSchema>> TableSchema::CreateFromJson(const std::string& json_str) {
    PAIMON_ASSIGN_OR_RAISE(TableSchema table_schema, TableSchema::FromJsonString(json_str));
    return InitSchema(table_schema.id_, table_schema.fields_, table_schema.highest_field_id_,
                      table_schema.partition_keys_, table_schema.primary_keys_,
                      table_schema.options_, table_schema.time_millis_);
}

Result<std::unique_ptr<TableSchema>> TableSchema::InitSchema(
    int64_t schema_id, const std::vector<DataField>& fields, int32_t highest_field_id,
    const std::vector<std::string>& partition_keys, const std::vector<std::string>& primary_keys,
    const std::map<std::string, std::string>& options, int64_t time_millis) {
    // validate schema first
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    PAIMON_RETURN_NOT_OK(ArrowSchemaValidator::ValidateSchemaWithFieldId(*arrow_schema));

    auto table_schema = std::unique_ptr<TableSchema>(
        new TableSchema(TableSchema::CURRENT_VERSION, schema_id, fields, highest_field_id,
                        partition_keys, primary_keys, options, time_millis));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> keys, table_schema->TrimmedPrimaryKeys());

    // Try to validate bucket keys
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> bucket_keys,
                           table_schema->OriginalBucketKeys());
    if (bucket_keys.empty()) {
        bucket_keys = keys;
    }
    table_schema->bucket_keys_ = bucket_keys;
    PAIMON_ASSIGN_OR_RAISE(
        table_schema->num_bucket_,
        OptionsUtils::GetValueFromMap<int32_t>(table_schema->Options(), Options::BUCKET, -1));

    return table_schema;
}

void TableSchema::FromJson(const rapidjson::Value& obj) noexcept(false) {
    version_ = RapidJsonUtil::DeserializeKeyValue<int32_t>(obj, "version", PAIMON_07_VERSION);
    id_ = RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, "id");
    fields_ = RapidJsonUtil::DeserializeKeyValue<std::vector<DataField>>(obj, "fields");
    highest_field_id_ = RapidJsonUtil::DeserializeKeyValue<int32_t>(obj, "highestFieldId");
    partition_keys_ =
        RapidJsonUtil::DeserializeKeyValue<std::vector<std::string>>(obj, "partitionKeys");
    primary_keys_ =
        RapidJsonUtil::DeserializeKeyValue<std::vector<std::string>>(obj, "primaryKeys");
    options_ =
        RapidJsonUtil::DeserializeKeyValue<std::map<std::string, std::string>>(obj, "options");
    if (version_ <= PAIMON_07_VERSION && options_.find(Options::BUCKET) == options_.end()) {
        // the default value of BUCKET in old version is 1
        options_[Options::BUCKET] = "1";
    }
    if (version_ <= PAIMON_08_VERSION && options_.find(Options::FILE_FORMAT) == options_.end()) {
        // the default value of FILE_FORMAT in old version is orc
        options_[Options::FILE_FORMAT] = "orc";
    }
    comment_ =
        RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(obj, "comment", comment_);
    time_millis_ = RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, "timeMillis", 0);
}

Result<std::vector<std::string>> TableSchema::TrimmedPrimaryKeys() const {
    if (primary_keys_.size() > 0) {
        std::vector<std::string> result;
        result.reserve(primary_keys_.size());
        std::set<std::string> partition_keys_set(partition_keys_.begin(), partition_keys_.end());
        for (const auto& pk : primary_keys_) {
            if (partition_keys_set.find(pk) == partition_keys_set.end()) {
                result.emplace_back(pk);
            }
        }
        if (result.size() <= 0) {
            return Status::Invalid(
                fmt::format("Primary key constraint {} should not be same with partition "
                            "fields {}, this will result in only one record in a partition",
                            primary_keys_, partition_keys_));
        }
        return result;
    }
    return primary_keys_;
}

/// Original bucket keys, maybe empty.
Result<std::vector<std::string>> TableSchema::OriginalBucketKeys() const {
    std::vector<std::string> bucket_keys;
    auto iter = options_.find(Options::BUCKET_KEY);
    if (iter == options_.end()) {
        return bucket_keys;
    }
    const auto& key = iter->second;
    if (StringUtils::IsNullOrWhitespaceOnly(key)) {
        return bucket_keys;
    }
    bucket_keys = StringUtils::Split(key, ",", /*ignore_empty=*/false);
    if (!ObjectUtils::ContainsAll(FieldNames(), bucket_keys)) {
        return Status::Invalid(fmt::format("Field names {} should contain all bucket keys {}.",
                                           FieldNames(), bucket_keys));
    }
    bool any_match =
        std::any_of(bucket_keys.begin(), bucket_keys.end(), [this](const std::string& key) {
            return std::find(partition_keys_.begin(), partition_keys_.end(), key) !=
                   partition_keys_.end();
        });
    if (any_match) {
        return Status::Invalid(fmt::format("Bucket keys {} should not in partition keys {}.",
                                           bucket_keys, partition_keys_));
    }

    if (primary_keys_.size() > 0) {
        if (!ObjectUtils::ContainsAll(primary_keys_, bucket_keys)) {
            return Status::Invalid(fmt::format("Primary keys {} should contain all bucket keys {}.",
                                               primary_keys_, bucket_keys));
        }
    }
    return bucket_keys;
}

bool TableSchema::CrossPartitionUpdate() const {
    if (primary_keys_.empty() || partition_keys_.empty()) {
        return false;
    }

    // If any partition key is not in primary keys, return true
    return !ObjectUtils::ContainsAll(primary_keys_, partition_keys_);
}

Result<std::unique_ptr<::ArrowSchema>> TableSchema::GetArrowSchema() const {
    std::shared_ptr<arrow::Schema> schema = DataField::ConvertDataFieldsToArrowSchema(fields_);
    auto arrow_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, arrow_schema.get()));
    return arrow_schema;
}

}  // namespace paimon
