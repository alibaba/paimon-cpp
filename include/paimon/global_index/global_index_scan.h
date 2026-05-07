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

#include "paimon/global_index/global_index_reader.h"
#include "paimon/global_index/global_index_result.h"
#include "paimon/predicate/predicate.h"
#include "paimon/utils/range.h"
#include "paimon/utils/row_range_index.h"
#include "paimon/visibility.h"
namespace paimon {
class MemoryPool;
class FileSystem;

/// Represents a logical scan over a global index for a table.
class PAIMON_EXPORT GlobalIndexScan {
 public:
    /// Creates a `GlobalIndexScan` instance for the specified table and context.
    /// @param table_path     Root directory of the table.
    /// @param snapshot_id    Optional snapshot id to read from; if not provided, uses the latest.
    /// @param partitions     Optional list of specific partitions to restrict the scan scope.
    ///                       Each map represents one partition (e.g., {"dt": "2024-06-01"}).
    ///                       If omitted (`std::nullopt`), scans all partitions of the table.
    /// @param options        User defined configuration.
    /// @param file_system    File system for accessing index files.
    ///                       If not provided (nullptr), it is inferred from the `FILE_SYSTEM`
    ///                       key in the `options` parameter.
    /// @param executor       The executor to be used for asynchronous operations during global
    ///                       index scan.
    /// @param pool           Memory pool for temporary allocations; if nullptr, uses default.
    /// @return A `Result` containing a unique pointer to the created scanner,
    ///         or an error if initialization fails (e.g., I/O error, invalid snapshot id,
    ///         unknown partition).
    static Result<std::unique_ptr<GlobalIndexScan>> Create(
        const std::string& table_path, const std::optional<int64_t>& snapshot_id,
        const std::optional<std::vector<std::map<std::string, std::string>>>& partitions,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& pool);

    /// Creates a `GlobalIndexScan` instance for the specified table and context, with a
    /// predicate-based partition filter.
    /// @param root_path         Root directory of the table.
    /// @param snapshot_id       Optional snapshot id to read from; if not provided, uses the
    ///                          latest snapshot.
    /// @param partition_filters Optional partition-level predicate used for partition pruning.
    ///                          If nullptr, all partitions are scanned.
    /// @param options           User defined configuration.
    /// @param file_system       File system for accessing index files. If nullptr, it is
    ///                          inferred from the `FILE_SYSTEM` key in `options`.
    /// @param executor          The executor to be used for asynchronous operations during global
    ///                          index scan.
    /// @param pool              Memory pool for temporary allocations; if nullptr, uses default.
    /// @return A `Result` containing a unique pointer to the created scanner,
    ///         or an error if initialization fails.
    static Result<std::unique_ptr<GlobalIndexScan>> Create(
        const std::string& root_path, const std::optional<int64_t>& snapshot_id,
        const std::shared_ptr<Predicate>& partition_filters,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& pool);

    virtual ~GlobalIndexScan() = default;

    /// Creates several `GlobalIndexReader`s for a specific field.
    /// @param field_name       Name of the indexed column.
    /// @param row_range_index  Optional row range that limits the scan to a sub-range of row ids.
    ///                         If not provided, the entire row range is considered.
    /// @return A `Result` that is:
    ///         - Successful with several readers(with global row id) if the indexes exist and load
    ///         correctly;
    ///         - Successful with an empty vector if no index was built for the given field;
    ///         - Error returns when loading fails (e.g., file corruption, I/O error,
    ///           unsupported format).
    virtual Result<std::vector<std::shared_ptr<GlobalIndexReader>>> CreateReaders(
        const std::string& field_name,
        const std::optional<RowRangeIndex>& row_range_index) const = 0;

    /// Creates several `GlobalIndexReader`s for a specific field (looked up by id),
    /// @param field_id         Field id of the indexed column.
    /// @param row_range_index  Optional row range that limits the scan to a sub-range of row ids.
    ///                         If not provided, the entire row range is considered.
    /// @return A `Result` that is:
    ///         - Successful with several readers(with global row id) if the indexes exist and load
    ///         correctly;
    ///         - Successful with an empty vector if no index was built for the given field;
    ///         - Error returns when loading fails (e.g., file corruption, I/O error,
    ///           unsupported format).
    virtual Result<std::vector<std::shared_ptr<GlobalIndexReader>>> CreateReaders(
        int32_t field_id, const std::optional<RowRangeIndex>& row_range_index) const = 0;
};

}  // namespace paimon
