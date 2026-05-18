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

#include "arrow/api.h"
#include "paimon/core/table/system/in_memory_system_table.h"

namespace paimon {
class FileSystem;

/// Base class for table metadata system tables backed by Paimon metadata files.
/// Subclasses read metadata from the selected table branch and materialize it in memory.
class MetadataSystemTable : public InMemorySystemTable {
 public:
    MetadataSystemTable(const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
                        const std::string& branch);

 protected:
    std::shared_ptr<FileSystem> fs_;
    std::string branch_;
};

}  // namespace paimon
