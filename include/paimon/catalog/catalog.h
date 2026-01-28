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

#include "paimon/catalog/identifier.h"
#include "paimon/result.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {
class Identifier;

/// This interface is responsible for reading and writing metadata such as database/table from a
/// paimon catalog.
class PAIMON_EXPORT Catalog {
 public:
    static const char SYSTEM_DATABASE_NAME[];
    static const char SYSTEM_TABLE_SPLITTER[];
    static const char DB_SUFFIX[];
    static const char DB_LOCATION_PROP[];

    /// %Factory method for creating a `Catalog` instance.
    ///
    /// @param root_path Path to the root directory where the catalog is located.
    /// @param options Configuration options for catalog initialization.
    /// @param file_system Specifies the file system for file operations.
    ///                    If not set, use default file system (configured in
    ///                    `Options::FILE_SYSTEM`)
    /// @return A result containing a unique pointer to a `Catalog` instance, or an error status.
    static Result<std::unique_ptr<Catalog>> Create(
        const std::string& root_path, const std::map<std::string, std::string>& options,
        const std::shared_ptr<FileSystem>& file_system = nullptr);

    virtual ~Catalog() = default;

    /// Creates a database with the specified properties.
    ///
    /// @param name Name of the database to be created.
    /// @param options Additional properties associated with the database.
    /// @param ignore_if_exists If true, no action is taken if the database already exists.
    ///                         If false, an error status is returned if the database exists.
    /// @return A status indicating success or failure.
    virtual Status CreateDatabase(const std::string& name,
                                  const std::map<std::string, std::string>& options,
                                  bool ignore_if_exists) = 0;

    /// Creates a new table in the catalog.
    ///
    /// @note System tables cannot be created using this method.
    ///
    /// @param identifier Identifier of the table to be created.
    /// @param c_schema The schema of the table to be created.
    /// @param partition_keys List of columns that should be used as partition keys for the table.
    /// @param primary_keys List of columns that should be used as primary keys for the table.
    /// @param options Additional table-specific options.
    /// @param ignore_if_exists If true, no action is taken if the table already exists.
    ///                         If false, an error status is returned if the table exists.
    /// @return A status indicating success or failure.
    virtual Status CreateTable(const Identifier& identifier, ArrowSchema* c_schema,
                               const std::vector<std::string>& partition_keys,
                               const std::vector<std::string>& primary_keys,
                               const std::map<std::string, std::string>& options,
                               bool ignore_if_exists) = 0;

    /// Lists all the databases available in the catalog.
    ///
    /// @return A result containing a vector of database names, or an error status.
    virtual Result<std::vector<std::string>> ListDatabases() const = 0;

    /// Lists all the tables within a specified database.
    ///
    /// @note System tables will not be listed.
    ///
    /// @param db_name The name of the database to list tables from.
    /// @return A result containing a vector of table names in the specified database, or an error
    /// status.
    virtual Result<std::vector<std::string>> ListTables(const std::string& db_name) const = 0;

    /// Checks whether a database with the specified name exists in the catalog.
    ///
    /// @param db_name The name of the database to check for existence.
    /// @return A result containing true if the database exists, false otherwise, or an error
    /// status.
    virtual Result<bool> DatabaseExists(const std::string& db_name) const = 0;

    /// Checks whether a table with the specified identifier exists in the catalog.
    ///
    /// @param identifier The identifier of the table to check for existence.
    /// @return A result containing true if the table exists, false otherwise, or an error status.
    virtual Result<bool> TableExists(const Identifier& identifier) const = 0;

    /// Returns the expected location of a specified database.
    ///
    /// @note This does not check whether the database actually exists.
    ///
    /// @param db_name The name of the database to get the location for.
    /// @return A string representing the expected location of the database.
    virtual std::string GetDatabaseLocation(const std::string& db_name) const = 0;

    /// Returns the expected location of a specified table.
    ///
    /// @note This does not check whether the table actually exists.
    ///
    /// @param identifier The table identifier containing database and table name.
    /// @return A string representing the expected location of the table.
    virtual std::string GetTableLocation(const Identifier& identifier) const = 0;

    /// Returns the root path of the catalog.
    ///
    /// @return A string representing the root path of the catalog.
    virtual std::string GetRootPath() const = 0;

    /// Returns the file system used by the catalog.
    ///
    /// @return A shared pointer to the file system instance.
    virtual std::shared_ptr<FileSystem> GetFileSystem() const = 0;

    /// Loads the latest schema of a specified table.
    ///
    /// @note System tables will not be supported.
    ///
    /// @param identifier The identifier (database and table name) of the table to load.
    /// @return A result containing table schema if the table exists, or an error status on failure.
    virtual Result<std::shared_ptr<Schema>> LoadTableSchema(const Identifier& identifier) const = 0;
};

}  // namespace paimon
