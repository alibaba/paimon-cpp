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

#include "paimon/core/table/system/metadata_system_table.h"

namespace paimon {
class FileSystem;

/// System table for `T$snapshots`, exposing snapshot commit history.
class SnapshotsSystemTable : public MetadataSystemTable {
 public:
    static constexpr const char* kName = "snapshots";

    SnapshotsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                         std::string branch);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::shared_ptr<arrow::RecordBatch>> BuildRecordBatch(
        arrow::MemoryPool* pool) const override;
};

/// System table for `T$schemas`, exposing schema evolution history.
class SchemasSystemTable : public MetadataSystemTable {
 public:
    static constexpr const char* kName = "schemas";

    SchemasSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path, std::string branch);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::shared_ptr<arrow::RecordBatch>> BuildRecordBatch(
        arrow::MemoryPool* pool) const override;
};

/// System table for `T$tags`, exposing tags and the snapshots they reference.
class TagsSystemTable : public MetadataSystemTable {
 public:
    static constexpr const char* kName = "tags";

    TagsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path, std::string branch);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::shared_ptr<arrow::RecordBatch>> BuildRecordBatch(
        arrow::MemoryPool* pool) const override;
};

/// System table for `T$branches`, exposing table branches including `main`.
class BranchesSystemTable : public MetadataSystemTable {
 public:
    static constexpr const char* kName = "branches";

    BranchesSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path, std::string branch);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::shared_ptr<arrow::RecordBatch>> BuildRecordBatch(
        arrow::MemoryPool* pool) const override;
};

/// System table for `T$consumers`, exposing persisted streaming consumer offsets.
class ConsumersSystemTable : public MetadataSystemTable {
 public:
    static constexpr const char* kName = "consumers";

    ConsumersSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                         std::string branch);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::shared_ptr<arrow::RecordBatch>> BuildRecordBatch(
        arrow::MemoryPool* pool) const override;
};

}  // namespace paimon
