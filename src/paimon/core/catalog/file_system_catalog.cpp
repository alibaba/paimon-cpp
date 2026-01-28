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

#include "paimon/core/catalog/file_system_catalog.h"

#include <cstring>
#include <optional>
#include <utility>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow
struct ArrowSchema;

namespace paimon {
FileSystemCatalog::FileSystemCatalog(const std::shared_ptr<FileSystem>& fs,
                                     const std::string& warehouse)
    : fs_(fs), warehouse_(warehouse), logger_(Logger::GetLogger("FileSystemCatalog")) {}

Status FileSystemCatalog::CreateDatabase(const std::string& db_name,
                                         const std::map<std::string, std::string>& options,
                                         bool ignore_if_exists) {
    if (IsSystemDatabase(db_name)) {
        return Status::Invalid(
            fmt::format("Cannot create database for system database {}.", db_name));
    }
    PAIMON_ASSIGN_OR_RAISE(bool exist, DatabaseExists(db_name));
    if (exist) {
        if (ignore_if_exists) {
            return Status::OK();
        } else {
            return Status::Invalid(fmt::format("database {} already exist", db_name));
        }
    }
    return CreateDatabaseImpl(db_name, options);
}

Status FileSystemCatalog::CreateDatabaseImpl(const std::string& db_name,
                                             const std::map<std::string, std::string>& options) {
    if (options.find(Catalog::DB_LOCATION_PROP) != options.end()) {
        return Status::Invalid(
            "Cannot specify location for a database when using fileSystem catalog.");
    }
    if (!options.empty()) {
        std::string log_msg = fmt::format(
            "Currently filesystem catalog can't store database properties, discard properties: "
            "{{{}}}",
            fmt::join(options, ", "));
        PAIMON_LOG_DEBUG(logger_, "%s", log_msg.c_str());
    }
    std::string db_path = NewDatabasePath(warehouse_, db_name);
    PAIMON_RETURN_NOT_OK(fs_->Mkdirs(db_path));
    return Status::OK();
}

Result<bool> FileSystemCatalog::DatabaseExists(const std::string& db_name) const {
    if (IsSystemDatabase(db_name)) {
        return Status::NotImplemented(
            "do not support checking DatabaseExists for system database.");
    }
    return fs_->Exists(NewDatabasePath(warehouse_, db_name));
}

Result<bool> FileSystemCatalog::TableExists(const Identifier& identifier) const {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           TableSchemaExists(identifier));
    return latest_schema != std::nullopt;
}

std::string FileSystemCatalog::GetDatabaseLocation(const std::string& db_name) const {
    return NewDatabasePath(warehouse_, db_name);
}

std::string FileSystemCatalog::GetTableLocation(const Identifier& identifier) const {
    return NewDataTablePath(warehouse_, identifier);
}

Status FileSystemCatalog::CreateTable(const Identifier& identifier, ArrowSchema* c_schema,
                                      const std::vector<std::string>& partition_keys,
                                      const std::vector<std::string>& primary_keys,
                                      const std::map<std::string, std::string>& options,
                                      bool ignore_if_exists) {
    if (IsSystemTable(identifier)) {
        return Status::Invalid(
            fmt::format("Cannot create table for system table {}, please use data table.",
                        identifier.ToString()));
    }
    PAIMON_ASSIGN_OR_RAISE(bool db_exist, DatabaseExists(identifier.GetDatabaseName()));
    if (!db_exist) {
        return Status::Invalid(
            fmt::format("database {} is not exist", identifier.GetDatabaseName()));
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           TableSchemaExists(identifier));
    bool table_exist = (latest_schema != std::nullopt);
    if (table_exist) {
        if (ignore_if_exists) {
            return Status::OK();
        } else {
            return Status::Invalid(
                fmt::format("table {} already exist", identifier.GetTableName()));
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(c_schema));
    PAIMON_ASSIGN_OR_RAISE(bool is_object_store, FileSystem::IsObjectStore(warehouse_));
    if (is_object_store &&
        options.find("enable-object-store-catalog-in-inte-test") == options.end()) {
        return Status::NotImplemented(
            "create table operation does not support object store file system for now");
    }
    SchemaManager schema_manager(fs_, NewDataTablePath(warehouse_, identifier));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<TableSchema> table_schema,
        schema_manager.CreateTable(schema, partition_keys, primary_keys, options));
    return Status::OK();
}

Result<std::optional<std::shared_ptr<TableSchema>>> FileSystemCatalog::TableSchemaExists(
    const Identifier& identifier) const {
    if (IsSystemTable(identifier)) {
        return Status::NotImplemented(
            "do not support checking TableSchemaExists for system table.");
    }
    SchemaManager schema_manager(fs_, NewDataTablePath(warehouse_, identifier));
    return schema_manager.Latest();
}

std::string FileSystemCatalog::GetRootPath() const {
    return warehouse_;
}

std::shared_ptr<FileSystem> FileSystemCatalog::GetFileSystem() const {
    return fs_;
}

bool FileSystemCatalog::IsSystemDatabase(const std::string& db_name) {
    return db_name == SYSTEM_DATABASE_NAME;
}

bool FileSystemCatalog::IsSpecifiedSystemTable(const Identifier& identifier) {
    return (identifier.GetTableName().find(SYSTEM_TABLE_SPLITTER) != std::string::npos);
}

bool FileSystemCatalog::IsSystemTable(const Identifier& identifier) {
    return IsSystemDatabase(identifier.GetDatabaseName()) || IsSpecifiedSystemTable(identifier);
}

std::string FileSystemCatalog::NewDatabasePath(const std::string& warehouse,
                                               const std::string& db_name) {
    return PathUtil::JoinPath(warehouse, db_name + DB_SUFFIX);
}

std::string FileSystemCatalog::NewDataTablePath(const std::string& warehouse,
                                                const Identifier& identifier) {
    return PathUtil::JoinPath(NewDatabasePath(warehouse, identifier.GetDatabaseName()),
                              identifier.GetTableName());
}

Result<std::vector<std::string>> FileSystemCatalog::ListDatabases() const {
    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    PAIMON_RETURN_NOT_OK(fs_->ListDir(warehouse_, &file_status_list));
    std::vector<std::string> db_names;
    for (const auto& file_status : file_status_list) {
        if (file_status->IsDir()) {
            std::string name = PathUtil::GetName(file_status->GetPath());
            if (StringUtils::EndsWith(name, DB_SUFFIX)) {
                db_names.push_back(name.substr(0, name.length() - std::strlen(DB_SUFFIX)));
            }
        }
    }
    return db_names;
}

Result<std::vector<std::string>> FileSystemCatalog::ListTables(const std::string& db_name) const {
    if (IsSystemDatabase(db_name)) {
        return Status::NotImplemented("do not support listing tables for system database.");
    }
    std::string database_path = NewDatabasePath(warehouse_, db_name);
    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    PAIMON_RETURN_NOT_OK(fs_->ListDir(database_path, &file_status_list));
    std::vector<std::string> table_names;
    for (const auto& file_status : file_status_list) {
        if (file_status->IsDir()) {
            std::string table_path = file_status->GetPath();
            PAIMON_ASSIGN_OR_RAISE(bool table_exist, TableExistsInFileSystem(table_path));
            if (table_exist) {
                table_names.push_back(PathUtil::GetName(table_path));
            }
        }
    }
    return table_names;
}

Result<bool> FileSystemCatalog::TableExistsInFileSystem(const std::string& table_path) const {
    SchemaManager schema_manager(fs_, table_path);
    // in order to improve the performance, check the schema-0 firstly.
    PAIMON_ASSIGN_OR_RAISE(bool schema_zero_exists, schema_manager.SchemaExists(0));
    if (schema_zero_exists) {
        return true;
    } else {
        // if schema-0 not exists, fallback to check other schemas
        PAIMON_ASSIGN_OR_RAISE(auto schema_ids, schema_manager.ListAllIds());
        return !schema_ids.empty();
    }
}

Result<std::shared_ptr<Schema>> FileSystemCatalog::LoadTableSchema(
    const Identifier& identifier) const {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           TableSchemaExists(identifier));
    if (!latest_schema) {
        return Status::NotExist(fmt::format("{} not exist", identifier.ToString()));
    }
    return std::static_pointer_cast<Schema>(*latest_schema);
}

}  // namespace paimon
