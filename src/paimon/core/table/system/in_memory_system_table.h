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

#include "paimon/common/data/generic_row.h"
#include "paimon/core/table/system/system_table.h"

namespace paimon {

/// Base class for system tables whose result can be materialized as a single in-memory
/// RecordBatch.
///
/// It provides the common singleton split scan and one-shot batch reader.
class InMemorySystemTable : public SystemTable {
 public:
    explicit InMemorySystemTable(std::string table_path);

    Result<std::unique_ptr<TableScan>> NewScan(
        const std::shared_ptr<ScanContext>& context) const override;
    Result<std::unique_ptr<TableRead>> NewRead(
        const std::shared_ptr<ReadContext>& context) const override;
    virtual Result<std::vector<GenericRow>> BuildRows() const = 0;

 protected:
    const std::string& TablePath() const {
        return table_path_;
    }

 private:
    std::string table_path_;
};

}  // namespace paimon
