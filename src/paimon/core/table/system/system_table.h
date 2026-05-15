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

#include "arrow/api.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"

namespace paimon {
class FileSystem;
class ReadContext;
class ScanContext;
class TableScan;
class TableRead;
class TableSchema;

struct SystemTablePath {
    std::string table_path;
    std::optional<std::string> branch;
    std::string system_table_name;
};

class SystemTable : public std::enable_shared_from_this<SystemTable> {
 public:
    virtual ~SystemTable() = default;

    virtual std::string Name() const = 0;
    virtual Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const = 0;
    virtual Result<std::unique_ptr<TableScan>> NewScan(
        const std::shared_ptr<ScanContext>& context) const = 0;
    virtual Result<std::unique_ptr<TableRead>> NewRead(
        const std::shared_ptr<ReadContext>& context) const = 0;
};

class SystemTableLoader {
 public:
    static bool IsSupported(const std::string& system_table_name);

    static Result<std::shared_ptr<SystemTable>> Load(
        const std::string& system_table_name, const std::shared_ptr<FileSystem>& fs,
        const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
        const std::map<std::string, std::string>& dynamic_options);

    static Result<std::optional<SystemTablePath>> TryParsePath(const std::string& path);

    static Result<std::shared_ptr<SystemTable>> LoadFromPath(
        const std::shared_ptr<FileSystem>& fs, const std::string& path,
        const std::map<std::string, std::string>& dynamic_options);
};

}  // namespace paimon
