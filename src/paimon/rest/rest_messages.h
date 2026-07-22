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

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "paimon/common/utils/jsonizable.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/core/snapshot.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {

// Request and response objects of the REST catalog protocol. The JSON field names must
// stay aligned with the REST catalog open api.

class ErrorResponse : public Jsonizable<ErrorResponse> {
 public:
    static constexpr const char* kResourceTypeDatabase = "DATABASE";
    static constexpr const char* kResourceTypeTable = "TABLE";

    ErrorResponse(const std::string& resource_type, const std::string& resource_name,
                  const std::string& message, int32_t code)
        : resource_type_(resource_type),
          resource_name_(resource_name),
          message_(message),
          code_(code) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;
    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    const std::string& GetResourceType() const {
        return resource_type_;
    }
    const std::string& GetResourceName() const {
        return resource_name_;
    }
    const std::string& GetMessage() const {
        return message_;
    }
    int32_t GetCode() const {
        return code_;
    }

    ErrorResponse() = default;

 private:
    std::string resource_type_;
    std::string resource_name_;
    std::string message_;
    int32_t code_ = 0;
};

/// The response of "/v1/config".
class ConfigResponse : public Jsonizable<ConfigResponse> {
 public:
    ConfigResponse(const std::map<std::string, std::string>& defaults,
                   const std::map<std::string, std::string>& overrides)
        : defaults_(defaults), overrides_(overrides) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;
    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    /// Merges with the client options; the precedence is
    /// `overrides` > `client_options` > `defaults`. A key sent as null in `overrides` is
    /// removed from the result, which is how the server unsets a client option; a null in
    /// `defaults` is ignored.
    std::map<std::string, std::string> Merge(
        const std::map<std::string, std::string>& client_options) const;

    const std::map<std::string, std::string>& GetDefaults() const {
        return defaults_;
    }
    const std::map<std::string, std::string>& GetOverrides() const {
        return overrides_;
    }

    ConfigResponse() = default;

 private:
    std::map<std::string, std::string> defaults_;
    std::map<std::string, std::string> overrides_;
    /// Keys the server sent as null in `overrides`; `Merge` removes them from the result
    /// and `ToJson` does not re-emit them.
    std::set<std::string> removed_keys_;
};

/// The audit fields shared by database/table responses.
struct RestAuditFields {
    std::optional<std::string> owner;
    std::optional<int64_t> created_at;
    std::optional<std::string> created_by;
    std::optional<int64_t> updated_at;
    std::optional<std::string> updated_by;

    void ParseFrom(const rapidjson::Value& obj);
    void AddTo(rapidjson::Value* obj, rapidjson::Document::AllocatorType* allocator) const;
    /// Adds the present fields to `options`, keyed by their JSON field names.
    void PutAuditOptionsTo(std::map<std::string, std::string>* options) const;
};

class CreateDatabaseRequest : public Jsonizable<CreateDatabaseRequest> {
 public:
    CreateDatabaseRequest(const std::string& name,
                          const std::map<std::string, std::string>& options)
        : name_(name), options_(options) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;
    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    const std::string& GetName() const {
        return name_;
    }
    const std::map<std::string, std::string>& GetOptions() const {
        return options_;
    }

    CreateDatabaseRequest() = default;

 private:
    std::string name_;
    std::map<std::string, std::string> options_;
};

class GetDatabaseResponse : public Jsonizable<GetDatabaseResponse> {
 public:
    GetDatabaseResponse(const std::string& id, const std::string& name, const std::string& location,
                        const std::map<std::string, std::string>& options)
        : id_(id), name_(name), location_(location), options_(options) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;
    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    const std::string& GetId() const {
        return id_;
    }
    const std::string& GetName() const {
        return name_;
    }
    const std::string& GetLocation() const {
        return location_;
    }
    const std::map<std::string, std::string>& GetOptions() const {
        return options_;
    }
    const RestAuditFields& GetAuditFields() const {
        return audit_;
    }

    GetDatabaseResponse() = default;

 private:
    std::string id_;
    std::string name_;
    std::string location_;
    std::map<std::string, std::string> options_;
    RestAuditFields audit_;
};

class ListDatabasesResponse : public Jsonizable<ListDatabasesResponse> {
 public:
    using ItemType = std::string;

    ListDatabasesResponse(const std::vector<std::string>& databases,
                          const std::optional<std::string>& next_page_token)
        : databases_(databases), next_page_token_(next_page_token) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;
    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    const std::vector<std::string>& Data() const {
        return databases_;
    }
    const std::optional<std::string>& NextPageToken() const {
        return next_page_token_;
    }

    ListDatabasesResponse() = default;

 private:
    std::vector<std::string> databases_;
    std::optional<std::string> next_page_token_;
};

class ListTablesResponse : public Jsonizable<ListTablesResponse> {
 public:
    using ItemType = std::string;

    ListTablesResponse(const std::vector<std::string>& tables,
                       const std::optional<std::string>& next_page_token)
        : tables_(tables), next_page_token_(next_page_token) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;
    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    const std::vector<std::string>& Data() const {
        return tables_;
    }
    const std::optional<std::string>& NextPageToken() const {
        return next_page_token_;
    }

    ListTablesResponse() = default;

 private:
    std::vector<std::string> tables_;
    std::optional<std::string> next_page_token_;
};

/// Each element is a snapshot in the same JSON layout as the snapshot files.
class ListSnapshotsResponse : public Jsonizable<ListSnapshotsResponse> {
 public:
    using ItemType = Snapshot;

    ListSnapshotsResponse(const std::vector<Snapshot>& snapshots,
                          const std::optional<std::string>& next_page_token)
        : snapshots_(snapshots), next_page_token_(next_page_token) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;
    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    const std::vector<Snapshot>& Data() const {
        return snapshots_;
    }
    const std::optional<std::string>& NextPageToken() const {
        return next_page_token_;
    }

    ListSnapshotsResponse() = default;

 private:
    std::vector<Snapshot> snapshots_;
    std::optional<std::string> next_page_token_;
};

/// The nested `schema` object (fields/partitionKeys/primaryKeys/options/comment) is
/// kept as a raw JSON string and converted to a `TableSchema` by the catalog.
class GetTableResponse : public Jsonizable<GetTableResponse> {
 public:
    GetTableResponse(const std::string& id, const std::string& database, const std::string& name,
                     const std::string& path, bool is_external, int64_t schema_id,
                     const std::string& schema_json)
        : id_(id),
          database_(database),
          name_(name),
          path_(path),
          is_external_(is_external),
          schema_id_(schema_id),
          schema_json_(schema_json) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;
    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    const std::string& GetId() const {
        return id_;
    }
    /// The database of the table. The server reports it so that a table addressed by its
    /// id alone can be resolved; it is empty on the endpoints that address a table by
    /// database and name, which are the ones `RestCatalog` uses.
    const std::string& GetDatabase() const {
        return database_;
    }
    const std::string& GetName() const {
        return name_;
    }
    const std::string& GetPath() const {
        return path_;
    }
    bool IsExternal() const {
        return is_external_;
    }
    int64_t GetSchemaId() const {
        return schema_id_;
    }
    const std::string& GetSchemaJson() const {
        return schema_json_;
    }
    const RestAuditFields& GetAuditFields() const {
        return audit_;
    }

    GetTableResponse() = default;

 private:
    std::string id_;
    std::string database_;
    std::string name_;
    std::string path_;
    bool is_external_ = false;
    int64_t schema_id_ = 0;
    std::string schema_json_;
    RestAuditFields audit_;
};

/// `schema_json` uses the same schema JSON layout as `GetTableResponse`.
class CreateTableRequest : public Jsonizable<CreateTableRequest> {
 public:
    CreateTableRequest(const std::string& database, const std::string& table,
                       const std::string& schema_json)
        : database_(database), table_(table), schema_json_(schema_json) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;
    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    const std::string& GetDatabase() const {
        return database_;
    }
    const std::string& GetTable() const {
        return table_;
    }
    const std::string& GetSchemaJson() const {
        return schema_json_;
    }

    CreateTableRequest() = default;

 private:
    std::string database_;
    std::string table_;
    std::string schema_json_;
};

class RenameTableRequest : public Jsonizable<RenameTableRequest> {
 public:
    RenameTableRequest(const std::string& source_database, const std::string& source_table,
                       const std::string& destination_database,
                       const std::string& destination_table)
        : source_database_(source_database),
          source_table_(source_table),
          destination_database_(destination_database),
          destination_table_(destination_table) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override;
    void FromJson(const rapidjson::Value& obj) noexcept(false) override;

    const std::string& GetSourceDatabase() const {
        return source_database_;
    }
    const std::string& GetSourceTable() const {
        return source_table_;
    }
    const std::string& GetDestinationDatabase() const {
        return destination_database_;
    }
    const std::string& GetDestinationTable() const {
        return destination_table_;
    }

    RenameTableRequest() = default;

 private:
    std::string source_database_;
    std::string source_table_;
    std::string destination_database_;
    std::string destination_table_;
};

}  // namespace paimon
