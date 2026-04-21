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

#include "paimon/result.h"

namespace paimon {

class CommitMessage;
class FileSystem;
class MemoryPool;

/// Compact coordinator for append-only unaware-bucket tables.
///
/// This coordinator scans the latest snapshot for small files, groups them by partition,
/// and generates compaction tasks using a bin-packing algorithm. It then synchronously
/// executes all tasks and returns the resulting commit messages.
///
/// @note This implementation does not support deletion vectors or streaming mode.
///       It only scans the current latest snapshot (batch mode).
class PAIMON_EXPORT AppendCompactCoordinator {
 public:
    AppendCompactCoordinator() = delete;
    ~AppendCompactCoordinator() = delete;
    /// Run the compaction coordinator.
    ///
    /// Scans the latest snapshot for small files across the specified partitions,
    /// generates compact tasks via bin-packing, executes them synchronously,
    /// and returns the resulting commit messages.
    ///
    /// @param table_path The root path of the table.
    /// @param options User-defined options (will be merged with schema options).
    /// @param partitions Partition filters; each element is a partition spec as key-value pairs.
    ///                   Empty vector means all partitions.
    /// @param file_system The file system to use. If nullptr, will be created from options.
    /// @param pool The memory pool to use. If nullptr, will use default pool.
    /// @return Result containing a vector of commit messages from compaction tasks.
    static Result<std::vector<std::shared_ptr<CommitMessage>>> Run(
        const std::string& table_path, const std::map<std::string, std::string>& options,
        const std::vector<std::map<std::string, std::string>>& partitions,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon
