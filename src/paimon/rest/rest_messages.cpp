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

#include "paimon/rest/rest_messages.h"

#include <stdexcept>

#include "paimon/rest/rest_util.h"

namespace paimon {

namespace {

constexpr const char kFieldMessage[] = "message";
constexpr const char kFieldResourceType[] = "resourceType";
constexpr const char kFieldResourceName[] = "resourceName";
constexpr const char kFieldCode[] = "code";
constexpr const char kFieldDefaults[] = "defaults";
constexpr const char kFieldOverrides[] = "overrides";
constexpr const char kFieldOwner[] = "owner";
constexpr const char kFieldCreatedAt[] = "createdAt";
constexpr const char kFieldCreatedBy[] = "createdBy";
constexpr const char kFieldUpdatedAt[] = "updatedAt";
constexpr const char kFieldUpdatedBy[] = "updatedBy";
constexpr const char kFieldName[] = "name";
constexpr const char kFieldOptions[] = "options";
constexpr const char kFieldId[] = "id";
constexpr const char kFieldLocation[] = "location";
constexpr const char kFieldDatabases[] = "databases";
constexpr const char kFieldTables[] = "tables";
constexpr const char kFieldSnapshots[] = "snapshots";
constexpr const char kFieldNextPageToken[] = "nextPageToken";
constexpr const char kFieldPath[] = "path";
constexpr const char kFieldIsExternal[] = "isExternal";
constexpr const char kFieldSchemaId[] = "schemaId";
constexpr const char kFieldSchema[] = "schema";
constexpr const char kFieldIdentifier[] = "identifier";
constexpr const char kFieldDatabase[] = "database";
constexpr const char kFieldObject[] = "object";
constexpr const char kFieldSource[] = "source";
constexpr const char kFieldDestination[] = "destination";

void AddOptionalStringMember(rapidjson::Value* obj, const char* key,
                             const std::optional<std::string>& value,
                             rapidjson::Document::AllocatorType* allocator) {
    if (value) {
        obj->AddMember(rapidjson::StringRef(key),
                       RapidJsonUtil::SerializeValue(value.value(), allocator).Move(), *allocator);
    }
}

rapidjson::Value SerializeIdentifier(const std::string& database, const std::string& table,
                                     rapidjson::Document::AllocatorType* allocator) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef(kFieldDatabase),
                  RapidJsonUtil::SerializeValue(database, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldObject),
                  RapidJsonUtil::SerializeValue(table, allocator).Move(), *allocator);
    return obj;
}

void DeserializeIdentifier(const rapidjson::Value& obj, const char* key, std::string* database,
                           std::string* table) {
    if (!obj.IsObject() || !obj.HasMember(key) || !obj[key].IsObject()) {
        throw std::invalid_argument(std::string("member '") + key +
                                    "' must exist and be an object");
    }
    const rapidjson::Value& identifier = obj[key];
    *database = RapidJsonUtil::DeserializeKeyValue<std::string>(identifier, kFieldDatabase);
    *table = RapidJsonUtil::DeserializeKeyValue<std::string>(identifier, kFieldObject);
}

std::string DeserializeRawJsonMember(const rapidjson::Value& obj, const char* key) {
    if (!obj.IsObject() || !obj.HasMember(key) || !obj[key].IsObject()) {
        throw std::invalid_argument(std::string("member '") + key +
                                    "' must exist and be an object");
    }
    return RestUtil::JsonToString(obj[key]);
}

}  // namespace

rapidjson::Value ErrorResponse::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef(kFieldMessage),
                  RapidJsonUtil::SerializeValue(message_, allocator).Move(), *allocator);
    if (!resource_type_.empty()) {
        obj.AddMember(rapidjson::StringRef(kFieldResourceType),
                      RapidJsonUtil::SerializeValue(resource_type_, allocator).Move(), *allocator);
    }
    if (!resource_name_.empty()) {
        obj.AddMember(rapidjson::StringRef(kFieldResourceName),
                      RapidJsonUtil::SerializeValue(resource_name_, allocator).Move(), *allocator);
    }
    obj.AddMember(rapidjson::StringRef(kFieldCode),
                  RapidJsonUtil::SerializeValue(code_, allocator).Move(), *allocator);
    return obj;
}

void ErrorResponse::FromJson(const rapidjson::Value& obj) noexcept(false) {
    message_ = RapidJsonUtil::DeserializeKeyValue<std::string>(obj, kFieldMessage, std::string());
    resource_type_ =
        RapidJsonUtil::DeserializeKeyValue<std::string>(obj, kFieldResourceType, std::string());
    resource_name_ =
        RapidJsonUtil::DeserializeKeyValue<std::string>(obj, kFieldResourceName, std::string());
    code_ = RapidJsonUtil::DeserializeKeyValue<int32_t>(obj, kFieldCode, 0);
}

rapidjson::Value ConfigResponse::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef(kFieldDefaults),
                  RapidJsonUtil::SerializeValue(defaults_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldOverrides),
                  RapidJsonUtil::SerializeValue(overrides_, allocator).Move(), *allocator);
    return obj;
}

namespace {
// The server may send null values in `defaults`/`overrides`; a null carries no value to
// merge, so it is collected in `null_keys` (when given) instead of failing the parse.
std::map<std::string, std::string> DeserializeStringMap(const rapidjson::Value& obj,
                                                        const char* key,
                                                        std::set<std::string>* null_keys) {
    std::map<std::string, std::string> result;
    if (!obj.IsObject() || !obj.HasMember(key) || obj[key].IsNull()) {
        return result;
    }
    const rapidjson::Value& map_value = obj[key];
    if (!map_value.IsObject()) {
        throw std::invalid_argument(std::string("member '") + key + "' must be an object");
    }
    for (auto iter = map_value.MemberBegin(); iter != map_value.MemberEnd(); ++iter) {
        if (iter->value.IsNull()) {
            if (null_keys != nullptr) {
                null_keys->insert(iter->name.GetString());
            }
            continue;
        }
        if (!iter->value.IsString()) {
            throw std::invalid_argument(std::string("member '") + key +
                                        "' must only contain string values");
        }
        result[iter->name.GetString()] = iter->value.GetString();
    }
    return result;
}
}  // namespace

void ConfigResponse::FromJson(const rapidjson::Value& obj) noexcept(false) {
    removed_keys_.clear();
    // A null default carries no value and never unsets anything; only a null override
    // removes a key.
    defaults_ = DeserializeStringMap(obj, kFieldDefaults, nullptr);
    overrides_ = DeserializeStringMap(obj, kFieldOverrides, &removed_keys_);
}

std::map<std::string, std::string> ConfigResponse::Merge(
    const std::map<std::string, std::string>& client_options) const {
    std::map<std::string, std::string> merged = defaults_;
    for (const auto& [key, value] : client_options) {
        merged[key] = value;
    }
    for (const auto& [key, value] : overrides_) {
        merged[key] = value;
    }
    for (const std::string& key : removed_keys_) {
        merged.erase(key);
    }
    return merged;
}

void RestAuditFields::ParseFrom(const rapidjson::Value& obj) {
    owner = RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(obj, kFieldOwner,
                                                                           std::nullopt);
    created_at = RapidJsonUtil::DeserializeKeyValue<std::optional<int64_t>>(obj, kFieldCreatedAt,
                                                                            std::nullopt);
    created_by = RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(
        obj, kFieldCreatedBy, std::nullopt);
    updated_at = RapidJsonUtil::DeserializeKeyValue<std::optional<int64_t>>(obj, kFieldUpdatedAt,
                                                                            std::nullopt);
    updated_by = RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(
        obj, kFieldUpdatedBy, std::nullopt);
}

void RestAuditFields::AddTo(rapidjson::Value* obj,
                            rapidjson::Document::AllocatorType* allocator) const {
    AddOptionalStringMember(obj, kFieldOwner, owner, allocator);
    if (created_at) {
        obj->AddMember(rapidjson::StringRef(kFieldCreatedAt),
                       RapidJsonUtil::SerializeValue(created_at.value(), allocator).Move(),
                       *allocator);
    }
    AddOptionalStringMember(obj, kFieldCreatedBy, created_by, allocator);
    if (updated_at) {
        obj->AddMember(rapidjson::StringRef(kFieldUpdatedAt),
                       RapidJsonUtil::SerializeValue(updated_at.value(), allocator).Move(),
                       *allocator);
    }
    AddOptionalStringMember(obj, kFieldUpdatedBy, updated_by, allocator);
}

void RestAuditFields::PutAuditOptionsTo(std::map<std::string, std::string>* options) const {
    if (owner) {
        (*options)[kFieldOwner] = owner.value();
    }
    if (created_at) {
        (*options)[kFieldCreatedAt] = std::to_string(created_at.value());
    }
    if (created_by) {
        (*options)[kFieldCreatedBy] = created_by.value();
    }
    if (updated_at) {
        (*options)[kFieldUpdatedAt] = std::to_string(updated_at.value());
    }
    if (updated_by) {
        (*options)[kFieldUpdatedBy] = updated_by.value();
    }
}

rapidjson::Value CreateDatabaseRequest::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef(kFieldName),
                  RapidJsonUtil::SerializeValue(name_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldOptions),
                  RapidJsonUtil::SerializeValue(options_, allocator).Move(), *allocator);
    return obj;
}

void CreateDatabaseRequest::FromJson(const rapidjson::Value& obj) noexcept(false) {
    name_ = RapidJsonUtil::DeserializeKeyValue<std::string>(obj, kFieldName);
    options_ = RapidJsonUtil::DeserializeKeyValue<std::map<std::string, std::string>>(
        obj, kFieldOptions, {});
}

rapidjson::Value GetDatabaseResponse::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef(kFieldId),
                  RapidJsonUtil::SerializeValue(id_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldName),
                  RapidJsonUtil::SerializeValue(name_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldLocation),
                  RapidJsonUtil::SerializeValue(location_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldOptions),
                  RapidJsonUtil::SerializeValue(options_, allocator).Move(), *allocator);
    audit_.AddTo(&obj, allocator);
    return obj;
}

void GetDatabaseResponse::FromJson(const rapidjson::Value& obj) noexcept(false) {
    id_ = RapidJsonUtil::DeserializeKeyValue<std::string>(obj, kFieldId, std::string());
    name_ = RapidJsonUtil::DeserializeKeyValue<std::string>(obj, kFieldName);
    location_ = RapidJsonUtil::DeserializeKeyValue<std::string>(obj, kFieldLocation, std::string());
    options_ = RapidJsonUtil::DeserializeKeyValue<std::map<std::string, std::string>>(
        obj, kFieldOptions, {});
    audit_.ParseFrom(obj);
}

rapidjson::Value ListDatabasesResponse::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef(kFieldDatabases),
                  RapidJsonUtil::SerializeValue(databases_, allocator).Move(), *allocator);
    AddOptionalStringMember(&obj, kFieldNextPageToken, next_page_token_, allocator);
    return obj;
}

void ListDatabasesResponse::FromJson(const rapidjson::Value& obj) noexcept(false) {
    databases_ =
        RapidJsonUtil::DeserializeKeyValue<std::vector<std::string>>(obj, kFieldDatabases, {});
    next_page_token_ = RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(
        obj, kFieldNextPageToken, std::nullopt);
}

rapidjson::Value ListTablesResponse::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef(kFieldTables),
                  RapidJsonUtil::SerializeValue(tables_, allocator).Move(), *allocator);
    AddOptionalStringMember(&obj, kFieldNextPageToken, next_page_token_, allocator);
    return obj;
}

void ListTablesResponse::FromJson(const rapidjson::Value& obj) noexcept(false) {
    tables_ = RapidJsonUtil::DeserializeKeyValue<std::vector<std::string>>(obj, kFieldTables, {});
    next_page_token_ = RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(
        obj, kFieldNextPageToken, std::nullopt);
}

rapidjson::Value ListSnapshotsResponse::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef(kFieldSnapshots),
                  RapidJsonUtil::SerializeValue(snapshots_, allocator).Move(), *allocator);
    AddOptionalStringMember(&obj, kFieldNextPageToken, next_page_token_, allocator);
    return obj;
}

void ListSnapshotsResponse::FromJson(const rapidjson::Value& obj) noexcept(false) {
    snapshots_ =
        RapidJsonUtil::DeserializeKeyValue<std::vector<Snapshot>>(obj, kFieldSnapshots, {});
    next_page_token_ = RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(
        obj, kFieldNextPageToken, std::nullopt);
}

rapidjson::Value GetTableResponse::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef(kFieldId),
                  RapidJsonUtil::SerializeValue(id_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldDatabase),
                  RapidJsonUtil::SerializeValue(database_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldName),
                  RapidJsonUtil::SerializeValue(name_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldPath),
                  RapidJsonUtil::SerializeValue(path_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldIsExternal),
                  RapidJsonUtil::SerializeValue(is_external_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldSchemaId),
                  RapidJsonUtil::SerializeValue(schema_id_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldSchema),
                  RestUtil::ParseToValue(schema_json_, allocator).Move(), *allocator);
    audit_.AddTo(&obj, allocator);
    return obj;
}

void GetTableResponse::FromJson(const rapidjson::Value& obj) noexcept(false) {
    id_ = RapidJsonUtil::DeserializeKeyValue<std::string>(obj, kFieldId, std::string());
    database_ = RapidJsonUtil::DeserializeKeyValue<std::string>(obj, kFieldDatabase, std::string());
    name_ = RapidJsonUtil::DeserializeKeyValue<std::string>(obj, kFieldName, std::string());
    path_ = RapidJsonUtil::DeserializeKeyValue<std::string>(obj, kFieldPath);
    is_external_ = RapidJsonUtil::DeserializeKeyValue<bool>(obj, kFieldIsExternal, false);
    schema_id_ = RapidJsonUtil::DeserializeKeyValue<int64_t>(obj, kFieldSchemaId);
    schema_json_ = DeserializeRawJsonMember(obj, kFieldSchema);
    audit_.ParseFrom(obj);
}

rapidjson::Value CreateTableRequest::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef(kFieldIdentifier),
                  SerializeIdentifier(database_, table_, allocator).Move(), *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldSchema),
                  RestUtil::ParseToValue(schema_json_, allocator).Move(), *allocator);
    return obj;
}

void CreateTableRequest::FromJson(const rapidjson::Value& obj) noexcept(false) {
    DeserializeIdentifier(obj, kFieldIdentifier, &database_, &table_);
    schema_json_ = DeserializeRawJsonMember(obj, kFieldSchema);
}

rapidjson::Value RenameTableRequest::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef(kFieldSource),
                  SerializeIdentifier(source_database_, source_table_, allocator).Move(),
                  *allocator);
    obj.AddMember(rapidjson::StringRef(kFieldDestination),
                  SerializeIdentifier(destination_database_, destination_table_, allocator).Move(),
                  *allocator);
    return obj;
}

void RenameTableRequest::FromJson(const rapidjson::Value& obj) noexcept(false) {
    DeserializeIdentifier(obj, kFieldSource, &source_database_, &source_table_);
    DeserializeIdentifier(obj, kFieldDestination, &destination_database_, &destination_table_);
}

}  // namespace paimon
