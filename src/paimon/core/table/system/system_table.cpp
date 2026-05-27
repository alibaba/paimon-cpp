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

#include "paimon/core/table/system/system_table.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/catalog/identifier.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/system/audit_log_system_table.h"
#include "paimon/core/table/system/binlog_system_table.h"
#include "paimon/core/table/system/metadata_system_tables.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/status.h"

namespace paimon {
namespace {

using SystemTableFactory = std::function<Result<std::shared_ptr<SystemTable>>(
    const std::shared_ptr<FileSystem>&, const std::string&, const std::shared_ptr<TableSchema>&,
    const std::map<std::string, std::string>&)>;

struct SystemTableRegistryEntry {
    std::string name;
    SystemTableFactory factory;
};

std::map<std::string, std::string> MergeOptions(
    const std::shared_ptr<TableSchema>& table_schema,
    const std::map<std::string, std::string>& dynamic_options) {
    auto options = table_schema->Options();
    for (const auto& [key, value] : dynamic_options) {
        options[key] = value;
    }
    return options;
}

std::string LoadBranch(const std::map<std::string, std::string>& options) {
    auto branch_iter = options.find(Options::BRANCH);
    return branch_iter == options.end() ? BranchManager::DEFAULT_MAIN_BRANCH : branch_iter->second;
}

const std::vector<SystemTableRegistryEntry>& SystemTableRegistry() {
    static const std::vector<SystemTableRegistryEntry> registry = {
        {OptionsSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& /*fs*/, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& /*dynamic_options*/)
             -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<OptionsSystemTable>(table_path, table_schema);
         }},
        {AuditLogSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options)
             -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<AuditLogSystemTable>(
                 fs, table_path, table_schema, MergeOptions(table_schema, dynamic_options));
         }},
        {BinlogSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options)
             -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<BinlogSystemTable>(
                 fs, table_path, table_schema, MergeOptions(table_schema, dynamic_options));
         }},
        {SnapshotsSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options)
             -> Result<std::shared_ptr<SystemTable>> {
             auto options = MergeOptions(table_schema, dynamic_options);
             return std::make_shared<SnapshotsSystemTable>(fs, table_path, LoadBranch(options));
         }},
        {SchemasSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options)
             -> Result<std::shared_ptr<SystemTable>> {
             auto options = MergeOptions(table_schema, dynamic_options);
             return std::make_shared<SchemasSystemTable>(fs, table_path, LoadBranch(options));
         }},
        {TagsSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options)
             -> Result<std::shared_ptr<SystemTable>> {
             auto options = MergeOptions(table_schema, dynamic_options);
             return std::make_shared<TagsSystemTable>(fs, table_path, LoadBranch(options));
         }},
        {BranchesSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options)
             -> Result<std::shared_ptr<SystemTable>> {
             auto options = MergeOptions(table_schema, dynamic_options);
             return std::make_shared<BranchesSystemTable>(fs, table_path, LoadBranch(options));
         }},
        {ConsumersSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options)
             -> Result<std::shared_ptr<SystemTable>> {
             auto options = MergeOptions(table_schema, dynamic_options);
             return std::make_shared<ConsumersSystemTable>(fs, table_path, LoadBranch(options));
         }},
    };
    return registry;
}

std::optional<SystemTableFactory> FindSystemTableFactory(const std::string& system_table_name) {
    std::string normalized_name = StringUtils::ToLowerCase(system_table_name);
    for (const auto& entry : SystemTableRegistry()) {
        if (entry.name == normalized_name) {
            return entry.factory;
        }
    }
    return std::nullopt;
}

}  // namespace

bool SystemTableLoader::IsSupported(const std::string& system_table_name) {
    return FindSystemTableFactory(system_table_name).has_value();
}

Result<std::shared_ptr<SystemTable>> SystemTableLoader::Load(
    const std::string& system_table_name, const std::shared_ptr<FileSystem>& fs,
    const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
    const std::map<std::string, std::string>& dynamic_options) {
    std::optional<SystemTableFactory> factory = FindSystemTableFactory(system_table_name);
    if (factory) {
        return factory.value()(fs, table_path, table_schema, dynamic_options);
    }
    return Status::NotImplemented("unsupported system table: ", system_table_name);
}

Result<std::optional<SystemTablePath>> SystemTableLoader::TryParsePath(const std::string& path) {
    std::string table_name = PathUtil::GetName(path);
    Identifier identifier(table_name);
    PAIMON_ASSIGN_OR_RAISE(bool is_system_table, identifier.IsSystemTable());
    if (!is_system_table) {
        return std::optional<SystemTablePath>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::string data_table_name, identifier.GetDataTableName());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> branch, identifier.GetBranchName());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> system_table_name,
                           identifier.GetSystemTableName());
    std::string parent = PathUtil::GetParentDirPath(path);
    SystemTablePath system_table_path;
    system_table_path.table_path = PathUtil::JoinPath(parent, data_table_name);
    system_table_path.branch = std::move(branch);
    system_table_path.system_table_name = system_table_name.value();
    return std::optional<SystemTablePath>(std::move(system_table_path));
}

Result<std::shared_ptr<SystemTable>> SystemTableLoader::LoadFromPath(
    const std::shared_ptr<FileSystem>& fs, const std::string& path,
    const std::map<std::string, std::string>& dynamic_options) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path, TryParsePath(path));
    if (!system_table_path) {
        return Status::Invalid("path is not a system table path: ", path);
    }
    const auto& parsed = system_table_path.value();
    SchemaManager schema_manager(fs, parsed.table_path,
                                 parsed.branch.value_or(BranchManager::DEFAULT_MAIN_BRANCH));
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           schema_manager.Latest());
    if (!latest_schema) {
        return Status::NotExist("base table schema not found for system table path: ", path);
    }
    auto options = dynamic_options;
    if (parsed.branch) {
        options[Options::BRANCH] = parsed.branch.value();
    }
    return Load(parsed.system_table_name, fs, parsed.table_path, latest_schema.value(), options);
}

}  // namespace paimon
