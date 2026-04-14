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

#include "paimon/catalog/table.h"

#include <optional>

#include "fmt/format.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/fs/file_system.h"

namespace paimon {

Result<std::shared_ptr<Table>> Table::Create(const std::shared_ptr<FileSystem>& file_system,
                                             const std::string& table_path,
                                             const Identifier& identifier) {
    PAIMON_ASSIGN_OR_RAISE(bool exist, file_system->Exists(table_path));
    if (!exist) {
        return Status::NotExist(fmt::format("{} not exist", identifier.ToString()));
    }

    SchemaManager schema_manager(file_system, table_path);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           schema_manager.Latest());
    if (!latest_schema) {
        return Status::NotExist(
            fmt::format("load table schema for {} failed", identifier.ToString()));
    }

    auto schema = std::static_pointer_cast<Schema>(*latest_schema);
    return std::make_shared<Table>(schema, identifier.GetDatabaseName(), identifier.GetTableName());
}

std::string Table::FullName() const {
    return database_ == Identifier::kUnknownDatabase ? table_name_ : database_ + "." + table_name_;
}

}  // namespace paimon
