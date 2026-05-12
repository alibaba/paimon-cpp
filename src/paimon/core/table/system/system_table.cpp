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

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "paimon/catalog/identifier.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/system/options_system_table.h"
#include "paimon/core/table/system/system_table_read.h"
#include "paimon/core/utils/branch_manager.h"

namespace paimon {

Result<std::unique_ptr<SystemTableRead>> SystemTable::NewRead(
    const std::shared_ptr<MemoryPool>& pool) const {
    return std::make_unique<SystemTableRead>(
        std::const_pointer_cast<SystemTable>(shared_from_this()), pool);
}

bool SystemTableLoader::IsSupported(const std::string& system_table_name) {
    return StringUtils::ToLowerCase(system_table_name) == OptionsSystemTable::kName;
}

Result<std::shared_ptr<SystemTable>> SystemTableLoader::Load(
    const std::string& system_table_name, const std::shared_ptr<FileSystem>& /*fs*/,
    const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema) {
    std::string normalized_name = StringUtils::ToLowerCase(system_table_name);
    if (normalized_name == OptionsSystemTable::kName) {
        return std::make_shared<OptionsSystemTable>(table_path, table_schema);
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
    const std::shared_ptr<FileSystem>& fs, const std::string& path) {
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
    return Load(parsed.system_table_name, fs, path, latest_schema.value());
}

}  // namespace paimon
