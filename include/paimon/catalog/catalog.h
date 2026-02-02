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

#include "paimon/catalog/database.h"
#include "paimon/catalog/identifier.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/table.h"
#include "paimon/core/view/view.h"
#include "paimon/result.h"
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

    /// Drops a database.
    ///
    /// @param name Name of the database to be dropped.
    /// @param ignore_if_not_exists If true, no action is taken if the database does not exist.
    /// @param cascade If true, drops all tables and functions in the database before dropping the
    /// database.
    /// @return A status indicating success or failure.
    virtual Status DropDatabase(const std::string& name, bool ignore_if_not_exists, bool cascade) {
        return Status::NotImplemented("DropDatabase not implemented");
    }

    /// Alters a database.
    ///
    /// @param name Name of the database to alter.
    /// @param changes Properties to be changed.
    /// @param ignore_if_not_exists If true, no action is taken if the database does not exist.
    /// @return A status indicating success or failure.
    virtual Status AlterDatabase(const std::string& name,
                                 const std::map<std::string, std::string>& changes,
                                 bool ignore_if_not_exists) {
        return Status::NotImplemented("AlterDatabase not implemented");
    }

    /// Gets a database.
    ///
    /// @param name Name of the database to get.
    /// @return A result containing the database information, or an error status.
    virtual Result<std::shared_ptr<Database>> GetDatabase(const std::string& name) const {
        return Status::NotImplemented("GetDatabase not implemented");
    }

    /// Gets a table.
    ///
    /// @param identifier Identifier of the table to get.
    /// @return A result containing the table, or an error status.
    virtual Result<std::shared_ptr<Table>> GetTable(const Identifier& identifier) const {
        return Status::NotImplemented("GetTable not implemented");
    }

    /// Gets a table by ID.
    ///
    /// @param table_id ID of the table to get.
    /// @return A result containing the table, or an error status.
    virtual Result<std::shared_ptr<Table>> GetTableById(const std::string& table_id) const {
        return Status::NotImplemented("GetTableById not implemented");
    }

    /// Drops a table.
    ///
    /// @param identifier Identifier of the table to drop.
    /// @param ignore_if_not_exists If true, no action is taken if the table does not exist.
    /// @return A status indicating success or failure.
    virtual Status DropTable(const Identifier& identifier, bool ignore_if_not_exists) {
        return Status::NotImplemented("DropTable not implemented");
    }

    /// Renames a table.
    ///
    /// @param from_table Current identifier of the table.
    /// @param to_table New identifier for the table.
    /// @param ignore_if_not_exists If true, no action is taken if the table does not exist.
    /// @return A status indicating success or failure.
    virtual Status RenameTable(const Identifier& from_table, const Identifier& to_table,
                               bool ignore_if_not_exists) {
        return Status::NotImplemented("RenameTable not implemented");
    }

    /// TODO(liangzi): Support Alter table

    /// Invalidates cached table metadata.
    ///
    /// @param identifier Identifier of the table to invalidate.
    virtual void InvalidateTable(const Identifier& identifier) {}

    /// Marks partitions as done.
    ///
    /// @param identifier Identifier of the table.
    /// @param partitions List of partition specifications.
    /// @return A status indicating success or failure.
    virtual Status MarkDonePartitions(
        const Identifier& identifier,
        const std::vector<std::map<std::string, std::string>>& partitions) {
        return Status::NotImplemented("MarkDonePartitions not implemented");
    }

    /// Lists all partitions of a table.
    ///
    /// @param identifier Identifier of the table.
    /// @return A result containing a list of partitions, or an error status.
    virtual Result<std::vector<std::map<std::string, std::string>>> ListPartitions(
        const Identifier& identifier) const {
        return Status::NotImplemented("ListPartitions not implemented");
    }

    /// Creates partitions.
    ///
    /// @param identifier Identifier of the table.
    /// @param partitions List of partition specifications to create.
    /// @return A status indicating success or failure.
    virtual Status CreatePartitions(
        const Identifier& identifier,
        const std::vector<std::map<std::string, std::string>>& partitions) {
        return Status::NotImplemented("CreatePartitions not implemented");
    }

    /// Drops partitions.
    ///
    /// @param identifier Identifier of the table.
    /// @param partitions List of partition specifications to drop.
    /// @return A status indicating success or failure.
    virtual Status DropPartitions(
        const Identifier& identifier,
        const std::vector<std::map<std::string, std::string>>& partitions) {
        return Status::NotImplemented("DropPartitions not implemented");
    }

    /// Alters partitions.
    ///
    /// @param identifier Identifier of the table.
    /// @param partitions List of partition statistics to alter.
    /// @return A status indicating success or failure.
    virtual Status AlterPartitions(const Identifier& identifier,
                                   const std::vector<PartitionStatistics>& partitions) {
        return Status::NotImplemented("AlterPartitions not implemented");
    }

    /// Gets a view.
    ///
    /// @param identifier Identifier of the view to get.
    /// @return A result containing the view, or an error status.
    virtual Result<std::shared_ptr<View>> GetView(const Identifier& identifier) const {
        return Status::NotImplemented("GetView not implemented");
    }

    /// Drops a view.
    ///
    /// @param identifier Identifier of the view to drop.
    /// @param ignore_if_not_exists If true, no action is taken if the view does not exist.
    /// @return A status indicating success or failure.
    virtual Status DropView(const Identifier& identifier, bool ignore_if_not_exists) {
        return Status::NotImplemented("DropView not implemented");
    }

    /// Creates a view.
    ///
    /// @param identifier Identifier of the view to create.
    /// @param view The view definition.
    /// @param ignore_if_exists If true, no action is taken if the view already exists.
    /// @return A status indicating success or failure.
    virtual Status CreateView(const Identifier& identifier, const View& view,
                              bool ignore_if_exists) {
        return Status::NotImplemented("CreateView not implemented");
    }

    /// Lists all views in a database.
    ///
    /// @param database_name Name of the database.
    /// @return A result containing a list of view names, or an error status.
    virtual Result<std::vector<std::string>> ListViews(const std::string& database_name) const {
        return Status::NotImplemented("ListViews not implemented");
    }

    /// Renames a view.
    ///
    /// @param from_view Current identifier of the view.
    /// @param to_view New identifier for the view.
    /// @param ignore_if_not_exists If true, no action is taken if the view does not exist.
    /// @return A status indicating success or failure.
    virtual Status RenameView(const Identifier& from_view, const Identifier& to_view,
                              bool ignore_if_not_exists) {
        return Status::NotImplemented("RenameView not implemented");
    }

    /// TODO(liangzi): Support Function/Snapshot/Tag/Branch/Authorizes api

    /// Repairs the entire catalog.
    ///
    /// @return A status indicating success or failure.
    virtual Status RepairCatalog() {
        return Status::NotImplemented("RepairCatalog not implemented");
    }

    /// Repairs a database.
    ///
    /// @param database_name Name of the database to repair.
    /// @return A status indicating success or failure.
    virtual Status RepairDatabase(const std::string& database_name) {
        return Status::NotImplemented("RepairDatabase not implemented");
    }

    /// Repairs a table.
    ///
    /// @param identifier Identifier of the table to repair.
    /// @return A status indicating success or failure.
    virtual Status RepairTable(const Identifier& identifier) {
        return Status::NotImplemented("RepairTable not implemented");
    }

    /// Registers a table.
    ///
    /// @param identifier Identifier of the table to register.
    /// @param path Path of the table.
    /// @return A status indicating success or failure.
    virtual Status RegisterTable(const Identifier& identifier, const std::string& path) {
        return Status::NotImplemented("RegisterTable not implemented");
    }

    /// Checks if list objects paged is supported.
    ///
    /// @return True if supported, false otherwise.
    virtual bool SupportsListObjectsPaged() const {
        return false;
    }

    /// Checks if list by pattern is supported.
    ///
    /// @return True if supported, false otherwise.
    virtual bool SupportsListByPattern() const {
        return false;
    }

    /// Checks if list table by type is supported.
    ///
    /// @return True if supported, false otherwise.
    virtual bool SupportsListTableByType() const {
        return false;
    }

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
