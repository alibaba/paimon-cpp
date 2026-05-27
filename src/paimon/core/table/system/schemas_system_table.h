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
#include "paimon/core/table/system/system_table_utils.h"

namespace paimon {
class FileSystem;

/// System table for `T$schemas`, exposing schema evolution history.
class SchemasSystemTable : public InMemorySystemTable {
 public:
    static constexpr const char* kName = "schemas";

    SchemasSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path, std::string branch);

    std::string Name() const override;
    Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const override;
    Result<std::vector<GenericRow>> BuildRows() const override;

 private:
    SystemTableContext context_;
};

}  // namespace paimon
