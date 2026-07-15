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
#include <string>
#include <vector>

#include "paimon/core/table/system/in_memory_system_table.h"

namespace paimon {
class Catalog;
class FileSystem;

/// Context passed to global system table constructors, providing catalog-level
/// access for enumerating databases, tables, and reading metadata.
struct GlobalSystemTableContext {
    const Catalog* catalog = nullptr;  // non-owning pointer
    std::shared_ptr<FileSystem> fs;
    std::string warehouse;
    std::map<std::string, std::string> catalog_options;
};

/// System table for `sys.catalog_options`, exposing catalog-level configuration
/// as key/value rows.
class CatalogOptionsSystemTable : public InMemorySystemTable {
 public:
    static constexpr const char* kName = "catalog_options";
    static constexpr const char* kEnabledOption = "catalog-options-table.enabled";

    explicit CatalogOptionsSystemTable(GlobalSystemTableContext context);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::vector<GenericRow>> BuildRows() const override;

 private:
    GlobalSystemTableContext context_;
};

/// System table for `sys.all_table_options`, exposing all table options across
/// all databases as (database_name, table_name, key, value) rows.
class AllTableOptionsSystemTable : public InMemorySystemTable {
 public:
    static constexpr const char* kName = "all_table_options";

    explicit AllTableOptionsSystemTable(GlobalSystemTableContext context);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::vector<GenericRow>> BuildRows() const override;

 private:
    GlobalSystemTableContext context_;
};

/// System table for `sys.tables`, exposing metadata for all tables across all
/// databases including record counts and file statistics.
class TablesSystemTable : public InMemorySystemTable {
 public:
    static constexpr const char* kName = "tables";

    explicit TablesSystemTable(GlobalSystemTableContext context);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::vector<GenericRow>> BuildRows() const override;

 private:
    GlobalSystemTableContext context_;
};

/// System table for `sys.partitions`, exposing partition-level file statistics
/// for all tables across all databases.
class PartitionsSystemTable : public InMemorySystemTable {
 public:
    static constexpr const char* kName = "partitions";

    explicit PartitionsSystemTable(GlobalSystemTableContext context);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::vector<GenericRow>> BuildRows() const override;

 private:
    GlobalSystemTableContext context_;
};

/// Loader for global system tables under the `sys` database.
///
/// Maintains its own registry with a factory signature that receives a
/// GlobalSystemTableContext instead of a per-table TableSchema.
class GlobalSystemTableLoader {
 public:
    static Result<bool> IsSupported(const std::string& table_name,
                                    const std::map<std::string, std::string>& catalog_options);

    static Result<std::shared_ptr<SystemTable>> Load(const std::string& table_name,
                                                     const GlobalSystemTableContext& context);

    static Result<std::vector<std::string>> GetSupportedTableNames(
        const std::map<std::string, std::string>& catalog_options);
};

}  // namespace paimon
