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

#include "paimon/rest/rest_catalog.h"

#include <algorithm>
#include <utility>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/catalog/table.h"
#include "paimon/catalog_options.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/system/global_system_tables.h"
#include "paimon/core/table/system/system_table.h"
#include "paimon/core/table/system/system_table_schema.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/rest/rest_util.h"
#include "rapidjson/document.h"

namespace paimon {

namespace {

constexpr const char kPathOption[] = "path";

// Maps the default branch "main" to "no branch": it is addressed as the bare table. The
// comparison ignores case, as the identifier the Java client sends is built the same way,
// so "MAIN" also resolves to the bare table and cannot address a branch of that name.
// `BranchManager::IsMainBranch`, which names the branch directory of a table, stays
// case-sensitive: this normalization only decides how a table is addressed on the server.
std::optional<std::string> NormalizeBranch(std::optional<std::string> branch) {
    if (branch && StringUtils::ToLowerCase(branch.value()) == Identifier::kDefaultMainBranch) {
        return std::nullopt;
    }
    return branch;
}

// Builds the "<table>$branch_<branch>" object name addressing `branch` of `table_name`
// on the rest server.
std::string BranchObjectName(std::string table_name, const std::string& branch) {
    table_name.append(Identifier::kSystemTableSplitter);
    table_name.append(Identifier::kSystemBranchPrefix);
    table_name.append(branch);
    return table_name;
}

// Builds the identifier sent to the rest server: the system table suffix is stripped
// while the branch stays in the object name, so the server resolves the branch itself and
// returns the branch's own schema. The path the server reports is the data table root in
// either case; the branch subdirectory is derived downstream from the branch option and
// must not be applied twice (see `ToTableSchema`).
Result<Identifier> ToLoadIdentifier(const Identifier& identifier) {
    PAIMON_ASSIGN_OR_RAISE(std::string data_table_name, identifier.GetDataTableName());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> branch, identifier.GetBranchName());
    branch = NormalizeBranch(std::move(branch));
    std::string object_name = branch ? BranchObjectName(std::move(data_table_name), branch.value())
                                     : std::move(data_table_name);
    return Identifier(identifier.GetDatabaseName(), object_name);
}

}  // namespace

RestCatalog::RestCatalog(std::unique_ptr<RestApi> api, const std::shared_ptr<FileSystem>& fs,
                         const std::string& warehouse)
    : api_(std::move(api)),
      fs_(fs),
      warehouse_(warehouse),
      table_default_options_(RestUtil::ExtractPrefixMap(
          api_->GetMergedOptions(), CatalogOptions::TABLE_DEFAULT_OPTION_PREFIX)),
      logger_(Logger::GetLogger("RestCatalog")) {}

Result<std::unique_ptr<RestCatalog>> RestCatalog::Create(
    const std::string& warehouse, const std::map<std::string, std::string>& options,
    const std::shared_ptr<FileSystem>& file_system, const RestHttpClient::Config& http_config) {
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<RestApi> api,
        RestApi::Create(options, warehouse, /*config_required=*/true, http_config));
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(api->GetMergedOptions(), file_system));
    return std::unique_ptr<RestCatalog>(
        new RestCatalog(std::move(api), core_options.GetFileSystem(), warehouse));
}

const std::map<std::string, std::string>& RestCatalog::GetOptions() const {
    return api_->GetMergedOptions();
}

Status RestCatalog::CreateDatabase(const std::string& name,
                                   const std::map<std::string, std::string>& options,
                                   bool ignore_if_exists) {
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotSystemDatabase(name, "createDatabase"));
    Status status = api_->CreateDatabase(name, options);
    if (status.IsExist() && ignore_if_exists) {
        return Status::OK();
    }
    return status;
}

Result<std::vector<std::string>> RestCatalog::ListDatabases() const {
    return api_->ListDatabases();
}

Result<bool> RestCatalog::DatabaseExists(const std::string& db_name) const {
    if (CatalogUtils::IsSystemDatabase(db_name)) {
        return true;
    }
    Result<GetDatabaseResponse> response = api_->GetDatabase(db_name);
    if (response.ok()) {
        return true;
    }
    if (response.status().IsNotExist()) {
        return false;
    }
    return response.status();
}

Status RestCatalog::DropDatabase(const std::string& name, bool ignore_if_not_exists, bool cascade) {
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotSystemDatabase(name, "dropDatabase"));
    if (!cascade) {
        Result<std::vector<std::string>> tables = ListTables(name);
        if (!tables.ok()) {
            if (tables.status().IsNotExist() && ignore_if_not_exists) {
                return Status::OK();
            }
            return tables.status();
        }
        if (!tables.value().empty()) {
            return Status::Invalid(
                fmt::format("Cannot drop non-empty database {}. Use cascade=true to force.", name));
        }
    }
    Status status = api_->DropDatabase(name);
    if (status.IsNotExist() && ignore_if_not_exists) {
        return Status::OK();
    }
    return status;
}

std::string RestCatalog::GetDatabaseLocation(const std::string& db_name) const {
    // The virtual "sys" database has no location and is unknown to the server.
    if (CatalogUtils::IsSystemDatabase(db_name)) {
        return "";
    }
    Result<GetDatabaseResponse> response = api_->GetDatabase(db_name);
    if (!response.ok()) {
        PAIMON_LOG_WARN(logger_, "failed to get location of database %s: %s", db_name.c_str(),
                        response.status().ToString().c_str());
        return "";
    }
    return response.value().GetLocation();
}

Result<std::vector<std::string>> RestCatalog::ListTables(const std::string& db_name) const {
    if (CatalogUtils::IsSystemDatabase(db_name)) {
        return GlobalSystemTableLoader::GetSupportedTableNames(api_->GetMergedOptions());
    }
    return api_->ListTables(db_name);
}

Status RestCatalog::CreateTable(const Identifier& identifier, ArrowSchema* c_schema,
                                const std::vector<std::string>& partition_keys,
                                const std::vector<std::string>& primary_keys,
                                const std::map<std::string, std::string>& options,
                                bool ignore_if_exists) {
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotBranch(identifier, "createTable"));
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotSystemTable(identifier, "createTable"));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(c_schema));
    std::map<std::string, std::string> effective_options = options;
    for (const auto& [key, value] : table_default_options_) {
        effective_options.emplace(key, value);
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> table_schema,
                           TableSchema::Create(TableSchema::FIRST_SCHEMA_ID, schema, partition_keys,
                                               primary_keys, effective_options));
    std::string schema_json;
    try {
        rapidjson::Document doc;
        doc.SetObject();
        rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();
        doc.AddMember(rapidjson::StringRef("fields"),
                      RapidJsonUtil::SerializeValue(table_schema->Fields(), &allocator).Move(),
                      allocator);
        doc.AddMember(rapidjson::StringRef("partitionKeys"),
                      RapidJsonUtil::SerializeValue(partition_keys, &allocator).Move(), allocator);
        doc.AddMember(rapidjson::StringRef("primaryKeys"),
                      RapidJsonUtil::SerializeValue(primary_keys, &allocator).Move(), allocator);
        doc.AddMember(rapidjson::StringRef("options"),
                      RapidJsonUtil::SerializeValue(effective_options, &allocator).Move(),
                      allocator);
        schema_json = RestUtil::JsonToString(doc);
    } catch (const std::exception& e) {
        return Status::SerializationError("failed to serialize create table schema: ", e.what());
    }
    Status status = api_->CreateTable(identifier, schema_json);
    if (status.IsExist() && ignore_if_exists) {
        return Status::OK();
    }
    return status;
}

Result<bool> RestCatalog::TableExists(const Identifier& identifier) const {
    if (CatalogUtils::IsSystemDatabase(identifier.GetDatabaseName())) {
        return GlobalSystemTableLoader::IsSupported(identifier.GetTableName(),
                                                    api_->GetMergedOptions());
    }
    PAIMON_ASSIGN_OR_RAISE(bool is_system_table, identifier.IsSystemTable());
    if (is_system_table) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> system_table_name,
                               identifier.GetSystemTableName());
        if (!system_table_name || !SystemTableLoader::IsSupported(system_table_name.value())) {
            return false;
        }
    }
    PAIMON_ASSIGN_OR_RAISE(Identifier load_identifier, ToLoadIdentifier(identifier));
    Result<GetTableResponse> response = api_->GetTable(load_identifier);
    if (response.ok()) {
        return true;
    }
    if (response.status().IsNotExist()) {
        return false;
    }
    return response.status();
}

Result<std::string> RestCatalog::GetTableLocation(const Identifier& identifier) const {
    PAIMON_ASSIGN_OR_RAISE(Identifier load_identifier, ToLoadIdentifier(identifier));
    PAIMON_ASSIGN_OR_RAISE(GetTableResponse response, api_->GetTable(load_identifier));
    return response.GetPath();
}

Status RestCatalog::DropTable(const Identifier& identifier, bool ignore_if_not_exists) {
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotBranch(identifier, "dropTable"));
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotSystemTable(identifier, "dropTable"));
    Status status = api_->DropTable(identifier);
    if (status.IsNotExist() && ignore_if_not_exists) {
        return Status::OK();
    }
    return status;
}

Status RestCatalog::RenameTable(const Identifier& from_table, const Identifier& to_table,
                                bool ignore_if_not_exists) {
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotBranch(from_table, "renameTable"));
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotBranch(to_table, "renameTable"));
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotSystemTable(from_table, "renameTable"));
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotSystemTable(to_table, "renameTable"));
    Status status = api_->RenameTable(from_table, to_table);
    if (status.IsNotExist() && ignore_if_not_exists) {
        return Status::OK();
    }
    return status;
}

Result<std::unique_ptr<TableSchema>> RestCatalog::ToTableSchema(
    const GetTableResponse& response, const std::optional<std::string>& branch) {
    std::string table_schema_json;
    try {
        rapidjson::Document src;
        src.Parse(response.GetSchemaJson().c_str());
        if (src.HasParseError() || !src.IsObject() || !src.HasMember("fields") ||
            !src["fields"].IsArray()) {
            return Status::Invalid("invalid table schema json from the rest server");
        }
        rapidjson::Document out;
        out.SetObject();
        rapidjson::Document::AllocatorType& allocator = out.GetAllocator();
        out.AddMember(rapidjson::StringRef("version"), TableSchema::CURRENT_VERSION, allocator);
        out.AddMember(rapidjson::StringRef("id"), response.GetSchemaId(), allocator);
        PAIMON_ASSIGN_OR_RAISE(int32_t highest_field_id,
                               TableSchema::ComputeHighestFieldId(src["fields"]));
        rapidjson::Value fields(rapidjson::kArrayType);
        fields.CopyFrom(src["fields"], allocator);
        out.AddMember(rapidjson::StringRef("highestFieldId"), highest_field_id, allocator);
        out.AddMember(rapidjson::StringRef("fields"), fields.Move(), allocator);
        // A missing or wrong-typed member fails instead of defaulting to an empty value,
        // which would silently change table semantics (an omitted "partitionKeys" would
        // load a partitioned table as unpartitioned); Java rejects absent members too.
        for (const char* key : {"partitionKeys", "primaryKeys"}) {
            if (!src.HasMember(key)) {
                return Status::Invalid(fmt::format(
                    "invalid table schema json from the rest server: missing '{}'", key));
            }
            if (!src[key].IsArray()) {
                return Status::Invalid(fmt::format(
                    "invalid table schema json from the rest server: '{}' is not an array", key));
            }
            rapidjson::Value keys(rapidjson::kArrayType);
            keys.CopyFrom(src[key], allocator);
            out.AddMember(rapidjson::StringRef(key), keys.Move(), allocator);
        }
        if (!src.HasMember("options")) {
            return Status::Invalid(
                "invalid table schema json from the rest server: missing 'options'");
        }
        if (!src["options"].IsObject()) {
            return Status::Invalid(
                "invalid table schema json from the rest server: 'options' is not an object");
        }
        auto options_map =
            RapidJsonUtil::DeserializeValue<std::map<std::string, std::string>>(src["options"]);
        options_map[kPathOption] = response.GetPath();
        response.GetAuditFields().PutAuditOptionsTo(&options_map);
        if (branch) {
            options_map[Options::BRANCH] = branch.value();
        }
        out.AddMember(rapidjson::StringRef("options"),
                      RapidJsonUtil::SerializeValue(options_map, &allocator).Move(), allocator);
        if (src.HasMember("comment") && src["comment"].IsString()) {
            rapidjson::Value comment;
            comment.CopyFrom(src["comment"], allocator);
            out.AddMember(rapidjson::StringRef("comment"), comment.Move(), allocator);
        }
        // The server's audit time keeps the conversion deterministic; 0 (the epoch)
        // when the server did not report an "updatedAt".
        out.AddMember(rapidjson::StringRef("timeMillis"),
                      response.GetAuditFields().updated_at.value_or(0), allocator);
        table_schema_json = RestUtil::JsonToString(out);
    } catch (const std::exception& e) {
        return Status::Invalid("failed to convert rest table schema: ", e.what());
    }
    return TableSchema::CreateFromJson(table_schema_json);
}

Result<std::shared_ptr<TableSchema>> RestCatalog::LoadDataTableSchema(
    const Identifier& data_identifier, const std::optional<std::string>& branch,
    std::string* table_path) const {
    PAIMON_ASSIGN_OR_RAISE(GetTableResponse response, api_->GetTable(data_identifier));
    if (table_path != nullptr) {
        *table_path = response.GetPath();
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> schema, ToTableSchema(response, branch));
    return std::shared_ptr<TableSchema>(std::move(schema));
}

Result<std::shared_ptr<Schema>> RestCatalog::LoadTableSchema(const Identifier& identifier) const {
    // Serve the global system tables of the "sys" database locally, like
    // FileSystemCatalog.
    if (CatalogUtils::IsSystemDatabase(identifier.GetDatabaseName())) {
        PAIMON_ASSIGN_OR_RAISE(bool supported,
                               GlobalSystemTableLoader::IsSupported(identifier.GetTableName(),
                                                                    api_->GetMergedOptions()));
        if (!supported) {
            return Status::NotExist(fmt::format("{} not exist", identifier.ToString()));
        }
        GlobalSystemTableContext context;
        context.catalog = this;
        context.fs = fs_;
        context.warehouse = warehouse_;
        context.catalog_options = api_->GetMergedOptions();
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SystemTable> system_table,
                               GlobalSystemTableLoader::Load(identifier.GetTableName(), context));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> arrow_schema,
                               system_table->ArrowSchema());
        return std::make_shared<SystemTableSchema>(std::move(arrow_schema));
    }
    PAIMON_ASSIGN_OR_RAISE(bool is_system_table, identifier.IsSystemTable());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> branch, identifier.GetBranchName());
    branch = NormalizeBranch(std::move(branch));
    PAIMON_ASSIGN_OR_RAISE(Identifier load_identifier, ToLoadIdentifier(identifier));
    if (is_system_table) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> system_table_name,
                               identifier.GetSystemTableName());
        if (!system_table_name || !SystemTableLoader::IsSupported(system_table_name.value())) {
            return Status::NotExist(fmt::format("{} not exist", identifier.ToString()));
        }
        std::string table_path;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> latest_schema,
                               LoadDataTableSchema(load_identifier, branch, &table_path));
        std::map<std::string, std::string> dynamic_options;
        if (branch) {
            dynamic_options[Options::BRANCH] = branch.value();
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SystemTable> system_table,
                               SystemTableLoader::Load(system_table_name.value(), fs_, table_path,
                                                       latest_schema, dynamic_options));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> arrow_schema,
                               system_table->ArrowSchema());
        return std::make_shared<SystemTableSchema>(std::move(arrow_schema));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> schema,
                           LoadDataTableSchema(load_identifier, branch, nullptr));
    return std::static_pointer_cast<Schema>(schema);
}

Result<std::shared_ptr<Table>> RestCatalog::GetTable(const Identifier& identifier) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Schema> schema, LoadTableSchema(identifier));
    return std::make_shared<Table>(schema, identifier.GetDatabaseName(), identifier.GetTableName());
}

std::string RestCatalog::GetRootPath() const {
    return warehouse_;
}

std::shared_ptr<FileSystem> RestCatalog::GetFileSystem() const {
    return fs_;
}

Result<std::vector<SnapshotInfo>> RestCatalog::ListSnapshots(const Identifier& identifier,
                                                             const std::string& branch) const {
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotBranch(identifier, "listSnapshots"));
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotSystemTable(identifier, "listSnapshots"));
    std::optional<std::string> normalized_branch =
        NormalizeBranch(branch.empty() ? std::nullopt : std::make_optional(branch));
    std::string object_name =
        normalized_branch ? BranchObjectName(identifier.GetTableName(), normalized_branch.value())
                          : identifier.GetTableName();
    Identifier load_identifier(identifier.GetDatabaseName(), object_name);
    // The Catalog interface has no pagination, so all pages are fetched; the server
    // does not order snapshots across pages while the contract requires ascending ids.
    PAIMON_ASSIGN_OR_RAISE(std::vector<Snapshot> snapshots, api_->ListSnapshots(load_identifier));
    std::sort(snapshots.begin(), snapshots.end(),
              [](const Snapshot& a, const Snapshot& b) { return a.Id() < b.Id(); });
    std::vector<SnapshotInfo> result;
    result.reserve(snapshots.size());
    for (const auto& snapshot : snapshots) {
        result.push_back(snapshot.ToSnapshotInfo());
    }
    return result;
}

}  // namespace paimon
