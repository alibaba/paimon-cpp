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
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"

namespace paimon {
class FileSystem;
class MemoryPool;
class Split;
class TableScan;
class TableSchema;

struct SystemTablePath {
    std::string table_path;
    std::optional<std::string> branch;
    std::string system_table_name;
};

class SystemTable {
 public:
    virtual ~SystemTable() = default;

    virtual std::string Name() const = 0;
    virtual std::shared_ptr<arrow::Schema> ArrowSchema() const = 0;
    virtual Result<std::unique_ptr<TableScan>> NewScan() const = 0;
    virtual Result<std::unique_ptr<BatchReader>> NewReader(
        const std::vector<std::shared_ptr<Split>>& splits,
        const std::shared_ptr<MemoryPool>& pool) const = 0;
};

Result<std::shared_ptr<SystemTable>> CreateSystemTable(
    const std::string& system_table_name, const std::shared_ptr<FileSystem>& fs,
    const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema);

bool IsSupportedSystemTable(const std::string& system_table_name);

Result<std::optional<SystemTablePath>> TryParseSystemTablePath(const std::string& path);

Result<std::shared_ptr<SystemTable>> LoadSystemTableFromPath(const std::shared_ptr<FileSystem>& fs,
                                                             const std::string& path);

}  // namespace paimon
