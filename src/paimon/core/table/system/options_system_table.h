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
class TableSchema;

class OptionsSystemTable : public SystemTable {
 public:
    static constexpr const char* kName = "options";

    OptionsSystemTable(std::string table_path, std::shared_ptr<TableSchema> table_schema);

    std::string Name() const override;
    std::shared_ptr<arrow::Schema> ArrowSchema() const override;
    Result<std::unique_ptr<TableScan>> NewScan() const override;
    Result<std::unique_ptr<BatchReader>> CreateBatchReader(
        const std::vector<std::shared_ptr<Split>>& splits,
        const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    std::string table_path_;
    std::shared_ptr<TableSchema> table_schema_;
};

}  // namespace paimon
