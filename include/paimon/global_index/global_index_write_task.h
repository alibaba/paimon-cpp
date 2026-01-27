/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/global_index/indexed_split.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/utils/range.h"
#include "paimon/visibility.h"

namespace paimon {
/// Writes a range-level global index for a specific data split and field.
class PAIMON_EXPORT GlobalIndexWriteTask {
 public:
    GlobalIndexWriteTask() = delete;
    ~GlobalIndexWriteTask() = delete;
    /// Builds and writes a global index for the specified data range.
    ///
    /// @param table_path   Path to the table root directory where index files are stored.
    /// @param field_name   Name of the indexed column (must be present in the table schema).
    /// @param index_type   Type of global index to build (e.g., "bitmap", "lumina").
    /// @param index_split  The indexed split containing the actual data (e.g., Parquet file) and
    //                      row id range [from, to] for data to build index.
    ///                     The range must be fully contained within the data covered
    ///                     by the given `split`.
    /// @param options      Index-specific configuration (e.g., false positive rate for bloom
    /// filters).
    /// @param pool         Memory pool for temporary allocations during index construction.
    ///                     If `nullptr`, the system's default memory pool will be used.
    /// @param file_system  Specifies the file system for file operations.
    ///                     If `nullptr`, use default file system.
    /// @return A `Result` containing a shared pointer to the `CommitMessage` with index metadata,
    ///         or an error if indexing fails (e.g., unsupported type, I/O error).
    static Result<std::shared_ptr<CommitMessage>> WriteIndex(
        const std::string& table_path, const std::string& field_name, const std::string& index_type,
        const std::shared_ptr<IndexedSplit>& indexed_split,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool,
        const std::shared_ptr<FileSystem>& file_system = nullptr);
};

}  // namespace paimon
