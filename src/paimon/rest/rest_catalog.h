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

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/catalog/catalog.h"
#include "paimon/logging.h"
#include "paimon/rest/rest_api.h"
#include "paimon/result.h"
#include "paimon/status.h"

struct ArrowSchema;

namespace paimon {
class FileSystem;
class TableSchema;

/// A catalog backed by a REST catalog server. Metadata operations are delegated to
/// `RestApi`; table data is accessed through the file system configured by the
/// server-merged options.
class RestCatalog : public Catalog {
 public:
    /// Creates the catalog: fetches and merges "/v1/config" from the server configured
    /// by `CatalogOptions::URI`, then builds the file system from the merged options.
    ///
    /// @param warehouse The warehouse identifier sent to the server; may be empty.
    static Result<std::unique_ptr<RestCatalog>> Create(
        const std::string& warehouse, const std::map<std::string, std::string>& options,
        const std::shared_ptr<FileSystem>& file_system,
        const RestHttpClient::Config& http_config = RestHttpClient::Config());

    Status CreateDatabase(const std::string& name,
                          const std::map<std::string, std::string>& options,
                          bool ignore_if_exists) override;
    Status CreateTable(const Identifier& identifier, ArrowSchema* c_schema,
                       const std::vector<std::string>& partition_keys,
                       const std::vector<std::string>& primary_keys,
                       const std::map<std::string, std::string>& options,
                       bool ignore_if_exists) override;
    Status DropDatabase(const std::string& name, bool ignore_if_not_exists, bool cascade) override;
    Status DropTable(const Identifier& identifier, bool ignore_if_not_exists) override;
    Status RenameTable(const Identifier& from_table, const Identifier& to_table,
                       bool ignore_if_not_exists) override;
    Result<std::vector<std::string>> ListDatabases() const override;
    Result<std::vector<std::string>> ListTables(const std::string& db_name) const override;
    Result<bool> DatabaseExists(const std::string& db_name) const override;
    Result<bool> TableExists(const Identifier& identifier) const override;
    std::string GetDatabaseLocation(const std::string& db_name) const override;
    Result<std::string> GetTableLocation(const Identifier& identifier) const override;
    Result<std::shared_ptr<Schema>> LoadTableSchema(const Identifier& identifier) const override;
    std::string GetRootPath() const override;
    std::shared_ptr<FileSystem> GetFileSystem() const override;
    Result<std::shared_ptr<Table>> GetTable(const Identifier& identifier) const override;
    Result<std::vector<SnapshotInfo>> ListSnapshots(const Identifier& identifier,
                                                    const std::string& branch) const override;

    /// Options merged with the server side config.
    const std::map<std::string, std::string>& GetOptions() const override;

 private:
    RestCatalog(std::unique_ptr<RestApi> api, const std::shared_ptr<FileSystem>& fs,
                const std::string& warehouse);

    /// Loads the table from the server and converts the response to a `TableSchema`
    /// (options are enriched with the table path, audit info and branch).
    Result<std::shared_ptr<TableSchema>> LoadDataTableSchema(
        const Identifier& data_identifier, const std::optional<std::string>& branch,
        std::string* table_path) const;

    static Result<std::unique_ptr<TableSchema>> ToTableSchema(
        const GetTableResponse& response, const std::optional<std::string>& branch);

    std::unique_ptr<RestApi> api_;
    std::shared_ptr<FileSystem> fs_;
    std::string warehouse_;
    /// The "table-default." options of the merged config, applied to `CreateTable`
    /// options when absent.
    std::map<std::string, std::string> table_default_options_;
    std::shared_ptr<Logger> logger_;
};

}  // namespace paimon
