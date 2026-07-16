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

#include "paimon/core/table/system/system_table.h"

namespace paimon {
class TableSchema;

/// System table for `T$ro`, exposing read-optimized data.
class ReadOptimizedSystemTable : public SystemTable {
 public:
    static constexpr const char* kName = "ro";

    ReadOptimizedSystemTable(std::string table_path, std::shared_ptr<TableSchema> table_schema,
                             std::map<std::string, std::string> options);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::unique_ptr<TableScan>> NewScan(
        const std::shared_ptr<ScanContext>& context) const override;
    Result<std::unique_ptr<TableRead>> NewRead(
        const std::shared_ptr<ReadContext>& context) const override;

 private:
    std::map<std::string, std::string> ReadOptimizedOptions() const;

    std::string table_path_;
    std::shared_ptr<TableSchema> table_schema_;
    std::map<std::string, std::string> options_;
};

}  // namespace paimon
