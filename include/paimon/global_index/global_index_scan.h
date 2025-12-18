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
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "paimon/global_index/row_range_global_index_scanner.h"
#include "paimon/utils/range.h"
#include "paimon/visibility.h"

namespace paimon {
class MemoryPool;
class FileSystem;
/// Represents a logical scan over a global index for a table.
class PAIMON_EXPORT GlobalIndexScan {
 public:
    /// Creates a `GlobalIndexScan` instance for the specified table and context.
    ///
    /// @param table_path     Root directory of the table.
    /// @param snapshot_id    Optional snapshot ID to read from; if not provided, uses the latest.
    /// @param partitions     Optional list of partition specs to restrict the scan scope.
    ///                       Each map represents one partition (e.g., {"dt": "2024-06-01"}).
    ///                       If omitted, scans all partitions.
    /// @param options        Index-specific configuration.
    /// @param file_system    File system for accessing index files.
    ///                       If not provided (nullptr), it is inferred from the `FILE_SYSTEM`
    ///                       key in the `options` parameter.
    /// @param pool           Memory pool for temporary allocations; if nullptr, uses default.
    /// @return A `Result` containing a unique pointer to the created scanner,
    ///         or an error if initialization fails (e.g., I/O error).
    static Result<std::unique_ptr<GlobalIndexScan>> Create(
        const std::string& table_path, const std::optional<int64_t>& snapshot_id,
        const std::optional<std::vector<std::map<std::string, std::string>>>& partitions,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool);

    virtual ~GlobalIndexScan() = default;

    /// Creates a scanner for the global index over the specified row ID range.
    ///
    /// This method instantiates a low-level scanner that can evaluate predicates and
    /// retrieve matching row IDs from the global index data corresponding to the given
    /// row ID range.
    ///
    /// @param range The inclusive row ID range [start, end] for which to create the scanner.
    ///              The range must be fully covered by existing global index data (from
    ///              `GetRowRangeList()`).
    /// @return A `Result` containing a range-level scanner, or an error if parse index meta fails.
    virtual Result<std::shared_ptr<RowRangeGlobalIndexScanner>> CreateRangeScan(
        const Range& range) = 0;

    /// Returns row ID ranges covered by this global index (sorted and non-overlapping
    /// ranges).
    ///
    /// Each `Range` represents a contiguous segment of row IDs for which global index
    /// data exists. This allows the query engine to parallelize scanning and be aware
    /// of ranges that are not covered by any global index.
    ///
    /// @return A `Result` containing sorted and non-overlapping `Range` objects.
    virtual Result<std::vector<Range>> GetRowRangeList() = 0;
};

}  // namespace paimon
