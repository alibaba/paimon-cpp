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

#include <algorithm>
#include <cstring>
#include <optional>
#include <set>
#include <utility>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
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
        PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> schema_ids, schema_manager.ListAllIds());
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

Result<std::shared_ptr<Table>> FileSystemCatalog::GetTable(const Identifier& identifier) const {
    return Table::Create(fs_, GetTableLocation(identifier), identifier);
}

Status FileSystemCatalog::DropDatabase(const std::string& name, bool ignore_if_not_exists,
                                       bool cascade) {
    if (IsSystemDatabase(name)) {
        return Status::Invalid(fmt::format("Cannot drop system database {}.", name));
    }

    PAIMON_ASSIGN_OR_RAISE(bool exist, DatabaseExists(name));
    if (!exist) {
        if (ignore_if_not_exists) {
            return Status::OK();
        } else {
            return Status::NotExist(fmt::format("database {} does not exist", name));
        }
    }

    std::string db_path = NewDatabasePath(warehouse_, name);

    if (cascade) {
        // List all tables in the database and drop them
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tables, ListTables(name));
        for (const std::string& table_name : tables) {
            Identifier table_id(name, table_name);
            PAIMON_RETURN_NOT_OK(DropTable(table_id, false));
        }
    } else {
        // Check if database is empty
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tables, ListTables(name));
        if (!tables.empty()) {
            return Status::Invalid(
                fmt::format("Cannot drop non-empty database {}. Use cascade=true to force.", name));
        }
    }

    // Delete the database directory
    PAIMON_RETURN_NOT_OK(fs_->Delete(db_path));
    return Status::OK();
}

Result<std::vector<std::string>> FileSystemCatalog::GetSchemaExternalPaths(
    const std::vector<std::shared_ptr<TableSchema>>& schemas) const {
    std::set<std::string> external_paths_set;
    for (const auto& schema : schemas) {
        const auto& options = schema->Options();
        auto iter = options.find(Options::DATA_FILE_EXTERNAL_PATHS);
        if (iter != options.end() && !iter->second.empty()) {
            auto paths = StringUtils::Split(iter->second, ",", /*ignore_empty=*/true);
            for (const auto& path : paths) {
                std::string trimmed_path = path;
                StringUtils::Trim(&trimmed_path);
                if (!trimmed_path.empty()) {
                    external_paths_set.insert(trimmed_path);
                }
            }
        }
    }
    return std::vector<std::string>(external_paths_set.begin(), external_paths_set.end());
}

Result<std::vector<std::string>> FileSystemCatalog::GetTableBranches(
    const std::string& table_path) const {
    std::vector<std::string> branches;
    std::string branch_dir = PathUtil::JoinPath(table_path, "branch");
    PAIMON_ASSIGN_OR_RAISE(bool branch_dir_exists, fs_->Exists(branch_dir));
    if (!branch_dir_exists) {
        return branches;
    }

    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    PAIMON_RETURN_NOT_OK(fs_->ListDir(branch_dir, &file_status_list));

    for (const auto& file_status : file_status_list) {
        if (file_status->IsDir()) {
            std::string dir_name = PathUtil::GetName(file_status->GetPath());
            // Branch directory name format: branch-{branch_name}
            const std::string branch_prefix = BranchManager::BRANCH_PREFIX;
            if (StringUtils::StartsWith(dir_name, branch_prefix, /*start_pos=*/0)) {
                std::string branch_name = dir_name.substr(branch_prefix.length());
                branches.push_back(branch_name);
            }
        }
    }
    return branches;
}

Status FileSystemCatalog::DropTableImpl(const Identifier& identifier,
                                        const std::vector<std::string>& external_paths) {
    std::string table_path = GetTableLocation(identifier);

    // Delete external paths first
    for (const auto& external_path : external_paths) {
        PAIMON_ASSIGN_OR_RAISE(bool exists, fs_->Exists(external_path));
        if (exists) {
            PAIMON_RETURN_NOT_OK(fs_->Delete(external_path));
        }
    }

    // Delete the table directory
    PAIMON_RETURN_NOT_OK(fs_->Delete(table_path));
    return Status::OK();
}

Status FileSystemCatalog::DropTable(const Identifier& identifier, bool ignore_if_not_exists) {
    if (IsSystemTable(identifier)) {
        return Status::Invalid(fmt::format("Cannot drop system table {}.", identifier.ToString()));
    }

    std::string table_path = GetTableLocation(identifier);
    PAIMON_ASSIGN_OR_RAISE(bool exist, fs_->Exists(table_path));
    if (!exist) {
        if (ignore_if_not_exists) {
            return Status::OK();
        } else {
            return Status::NotExist(fmt::format("table {} does not exist", identifier.ToString()));
        }
    }

    // Check if table has valid schema (table exists)
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           TableSchemaExists(identifier));
    if (!latest_schema) {
        if (ignore_if_not_exists) {
            return Status::OK();
        } else {
            return Status::NotExist(fmt::format("table {} does not exist", identifier.ToString()));
        }
    }

    // Collect external paths from all schemas
    std::set<std::string> external_paths_set;

    // Get external paths from main branch
    SchemaManager schema_manager(fs_, table_path);
    PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> schema_ids, schema_manager.ListAllIds());
    std::vector<std::shared_ptr<TableSchema>> schemas;
    for (int64_t id : schema_ids) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> schema, schema_manager.ReadSchema(id));
        schemas.push_back(schema);
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> main_external_paths,
                           GetSchemaExternalPaths(schemas));
    external_paths_set.insert(main_external_paths.begin(), main_external_paths.end());

    // Get external paths from all branches
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> branches, GetTableBranches(table_path));
    for (const auto& branch : branches) {
        SchemaManager branch_schema_manager(fs_, table_path, branch);
        PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> branch_schema_ids,
                               branch_schema_manager.ListAllIds());
        std::vector<std::shared_ptr<TableSchema>> branch_schemas;
        for (int64_t id : branch_schema_ids) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> schema,
                                   branch_schema_manager.ReadSchema(id));
            branch_schemas.push_back(schema);
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> branch_external_paths,
                               GetSchemaExternalPaths(branch_schemas));
        external_paths_set.insert(branch_external_paths.begin(), branch_external_paths.end());
    }

    std::vector<std::string> external_paths(external_paths_set.begin(), external_paths_set.end());
    return DropTableImpl(identifier, external_paths);
}

Status FileSystemCatalog::RenameTable(const Identifier& from_table, const Identifier& to_table,
                                      bool ignore_if_not_exists) {
    if (IsSystemTable(from_table) || IsSystemTable(to_table)) {
        return Status::Invalid(fmt::format("Cannot rename system table {} or {}.",
                                           from_table.ToString(), to_table.ToString()));
    }

    if (from_table.GetDatabaseName() != to_table.GetDatabaseName()) {
        return Status::Invalid(
            "Cannot rename table across databases. Cross-database rename is not supported.");
    }

    PAIMON_ASSIGN_OR_RAISE(bool from_exist, TableExists(from_table));
    if (!from_exist) {
        if (ignore_if_not_exists) {
            return Status::OK();
        } else {
            return Status::NotExist(
                fmt::format("source table {} does not exist", from_table.ToString()));
        }
    }

    PAIMON_ASSIGN_OR_RAISE(bool to_exist, TableExists(to_table));
    if (to_exist) {
        return Status::Invalid(fmt::format("target table {} already exists", to_table.ToString()));
    }

    std::string from_path = GetTableLocation(from_table);
    std::string to_path = GetTableLocation(to_table);
    PAIMON_RETURN_NOT_OK(fs_->Rename(from_path, to_path));
    return Status::OK();
}

namespace {
SnapshotInfo::CommitKind ConvertCommitKind(Snapshot::CommitKind internal) {
    if (internal == Snapshot::CommitKind::Append()) {
        return SnapshotInfo::CommitKind::APPEND;
    }
    if (internal == Snapshot::CommitKind::Compact()) {
        return SnapshotInfo::CommitKind::COMPACT;
    }
    if (internal == Snapshot::CommitKind::Overwrite()) {
        return SnapshotInfo::CommitKind::OVERWRITE;
    }
    if (internal == Snapshot::CommitKind::Analyze()) {
        return SnapshotInfo::CommitKind::ANALYZE;
    }
    return SnapshotInfo::CommitKind::UNKNOWN;
}
}  // namespace

Result<std::vector<SnapshotInfo>> FileSystemCatalog::ListSnapshots(
    const Identifier& identifier, const std::string& branch) const {
    PAIMON_ASSIGN_OR_RAISE(bool exists, TableExists(identifier));
    if (!exists) {
        return Status::NotExist(fmt::format("table {} does not exist", identifier.ToString()));
    }

    auto table_path = GetTableLocation(identifier);
    SnapshotManager mgr(fs_, table_path, branch);
    PAIMON_ASSIGN_OR_RAISE(std::vector<Snapshot> snapshots, mgr.GetAllSnapshots());
    std::sort(snapshots.begin(), snapshots.end(),
              [](const Snapshot& a, const Snapshot& b) { return a.Id() < b.Id(); });

    std::vector<SnapshotInfo> result;
    result.reserve(snapshots.size());

    for (const auto& snap : snapshots) {
        SnapshotInfo info;
        info.snapshot_id = snap.Id();
        info.schema_id = snap.SchemaId();
        info.commit_user = snap.CommitUser();
        info.commit_kind = ConvertCommitKind(snap.GetCommitKind());
        info.time_millis = snap.TimeMillis();
        info.total_record_count = snap.TotalRecordCount();
        info.delta_record_count = snap.DeltaRecordCount();
        info.watermark = snap.Watermark();
        result.push_back(std::move(info));
    }

    return result;
}

}  // namespace paimon
