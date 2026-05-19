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

#include <memory>
#include <string>
#include <vector>

#include "paimon/core/table/system/in_memory_system_table.h"

namespace paimon {
class FileSystem;
class TableSchema;

/// System table for `T$options`, exposing the latest base table options as key/value rows.
class OptionsSystemTable : public InMemorySystemTable {
 public:
    static constexpr const char* kName = "options";

    OptionsSystemTable(std::string table_path, std::shared_ptr<TableSchema> table_schema);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::vector<GenericRow>> BuildRows() const override;

 private:
    std::shared_ptr<TableSchema> table_schema_;
};

/// Shared table metadata location used by metadata system tables.
struct MetadataSystemTableContext {
    std::shared_ptr<FileSystem> fs;
    std::string table_path;
    std::string branch;
};

/// System table for `T$snapshots`, exposing snapshot commit history.
class SnapshotsSystemTable : public InMemorySystemTable {
 public:
    static constexpr const char* kName = "snapshots";

    SnapshotsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                         std::string branch);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::vector<GenericRow>> BuildRows() const override;

 private:
    MetadataSystemTableContext context_;
};

/// System table for `T$schemas`, exposing schema evolution history.
class SchemasSystemTable : public InMemorySystemTable {
 public:
    static constexpr const char* kName = "schemas";

    SchemasSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path, std::string branch);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::vector<GenericRow>> BuildRows() const override;

 private:
    MetadataSystemTableContext context_;
};

/// System table for `T$tags`, exposing tags and the snapshots they reference.
class TagsSystemTable : public InMemorySystemTable {
 public:
    static constexpr const char* kName = "tags";

    TagsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path, std::string branch);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::vector<GenericRow>> BuildRows() const override;

 private:
    MetadataSystemTableContext context_;
};

/// System table for `T$branches`, exposing table branches including `main`.
class BranchesSystemTable : public InMemorySystemTable {
 public:
    static constexpr const char* kName = "branches";

    BranchesSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path, std::string branch);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::vector<GenericRow>> BuildRows() const override;

 private:
    MetadataSystemTableContext context_;
};

/// System table for `T$consumers`, exposing persisted streaming consumer offsets.
class ConsumersSystemTable : public InMemorySystemTable {
 public:
    static constexpr const char* kName = "consumers";

    ConsumersSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                         std::string branch);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::vector<GenericRow>> BuildRows() const override;

 private:
    MetadataSystemTableContext context_;
};

}  // namespace paimon
