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

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/catalog/catalog.h"
#include "paimon/logging.h"
#include "paimon/result.h"
#include "paimon/status.h"

struct ArrowSchema;

namespace paimon {
class TableSchema;
class FileSystem;
class Identifier;
class Logger;

class FileSystemCatalog : public Catalog {
 public:
    FileSystemCatalog(const std::shared_ptr<FileSystem>& fs, const std::string& warehouse);

    Status CreateDatabase(const std::string& db_name,
                          const std::map<std::string, std::string>& options,
                          bool ignore_if_exists) override;
    Status CreateTable(const Identifier& identifier, ArrowSchema* c_schema,
                       const std::vector<std::string>& partition_keys,
                       const std::vector<std::string>& primary_keys,
                       const std::map<std::string, std::string>& options,
                       bool ignore_if_exists) override;

    Result<std::vector<std::string>> ListDatabases() const override;
    Result<std::vector<std::string>> ListTables(const std::string& db_name) const override;
    Result<bool> DatabaseExists(const std::string& db_name) const override;
    Result<bool> TableExists(const Identifier& identifier) const override;
    std::string GetDatabaseLocation(const std::string& db_name) const override;
    std::string GetTableLocation(const Identifier& identifier) const override;
    Result<std::shared_ptr<Schema>> LoadTableSchema(const Identifier& identifier) const override;
    std::string GetRootPath() const override;
    std::shared_ptr<FileSystem> GetFileSystem() const override;

 private:
    static std::string NewDatabasePath(const std::string& warehouse, const std::string& db_name);
    static std::string NewDataTablePath(const std::string& warehouse, const Identifier& identifier);
    static bool IsSystemDatabase(const std::string& db_name);
    static bool IsSpecifiedSystemTable(const Identifier& identifier);
    static bool IsSystemTable(const Identifier& identifier);
    Result<std::optional<std::shared_ptr<TableSchema>>> TableSchemaExists(
        const Identifier& identifier) const;

    Status CreateDatabaseImpl(const std::string& db_name,
                              const std::map<std::string, std::string>& options);
    Result<bool> TableExistsInFileSystem(const std::string& table_path) const;

    std::shared_ptr<FileSystem> fs_;
    std::string warehouse_;

    std::shared_ptr<Logger> logger_;
};

}  // namespace paimon
