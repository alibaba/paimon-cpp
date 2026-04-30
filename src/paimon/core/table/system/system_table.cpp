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
#include <stdexcept>
#include <string>
#include <utility>

#include "paimon/catalog/identifier.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/system/options_system_table.h"

namespace paimon {

bool IsSupportedSystemTable(const std::string& system_table_name) {
    return StringUtils::ToLowerCase(system_table_name) == OptionsSystemTable::NAME;
}

Result<std::shared_ptr<SystemTable>> CreateSystemTable(
    const std::string& system_table_name, const std::shared_ptr<FileSystem>& /*fs*/,
    const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema) {
    std::string normalized_name = StringUtils::ToLowerCase(system_table_name);
    if (normalized_name == OptionsSystemTable::NAME) {
        return std::make_shared<OptionsSystemTable>(table_path, table_schema);
    }
    return Status::NotExist("unsupported system table: ", system_table_name);
}

Result<std::optional<SystemTablePath>> TryParseSystemTablePath(const std::string& path) {
    std::string table_name = PathUtil::GetName(path);
    Identifier identifier(table_name);
    try {
        if (!identifier.IsSystemTable()) {
            return std::optional<SystemTablePath>();
        }
        std::string parent = PathUtil::GetParentDirPath(path);
        SystemTablePath system_table_path;
        system_table_path.table_path = PathUtil::JoinPath(parent, identifier.GetDataTableName());
        system_table_path.branch = identifier.GetBranchName();
        system_table_path.system_table_name = identifier.GetSystemTableName().value();
        return std::optional<SystemTablePath>(std::move(system_table_path));
    } catch (const std::exception& e) {
        return Status::Invalid(e.what());
    }
}

Result<std::shared_ptr<SystemTable>> LoadSystemTableFromPath(const std::shared_ptr<FileSystem>& fs,
                                                             const std::string& path) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           TryParseSystemTablePath(path));
    if (!system_table_path) {
        return Status::Invalid("path is not a system table path: ", path);
    }
    const auto& parsed = system_table_path.value();
    SchemaManager schema_manager(fs, parsed.table_path, parsed.branch.value_or(""));
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           schema_manager.Latest());
    if (!latest_schema) {
        return Status::NotExist("base table schema not found for system table path: ", path);
    }
    return CreateSystemTable(parsed.system_table_name, fs, path, latest_schema.value());
}

}  // namespace paimon
