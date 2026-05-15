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

#include "paimon/core/table/system/system_table.h"

namespace paimon {
class FileSystem;
class TableSchema;

class ChangelogBatchConverter {
 public:
    virtual ~ChangelogBatchConverter() = default;

    virtual Result<std::shared_ptr<arrow::Array>> ConvertDataColumn(
        const std::shared_ptr<arrow::Array>& array, arrow::MemoryPool* pool) const = 0;
};

class AuditLogSystemTable : public SystemTable {
 public:
    static constexpr const char* kName = "audit_log";

    AuditLogSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                        std::shared_ptr<TableSchema> table_schema,
                        std::map<std::string, std::string> options);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::unique_ptr<TableScan>> NewScan(
        const std::shared_ptr<ScanContext>& context) const override;
    Result<std::unique_ptr<TableRead>> NewRead(
        const std::shared_ptr<ReadContext>& context) const override;

 protected:
    Result<std::unique_ptr<TableRead>> NewChangelogRead(
        const std::shared_ptr<ReadContext>& context,
        std::shared_ptr<const ChangelogBatchConverter> converter) const;
    Result<std::shared_ptr<arrow::Schema>> BaseReadSchema() const;
    Result<std::map<std::string, std::string>> ReadOptions() const;

    std::shared_ptr<FileSystem> fs_;
    std::string table_path_;
    std::shared_ptr<TableSchema> table_schema_;
    std::map<std::string, std::string> options_;
};

}  // namespace paimon
