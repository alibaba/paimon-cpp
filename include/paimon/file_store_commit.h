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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class CommitContext;
class CommitMessage;

/// Interface for commit operations in a file store.
///
/// The `FileStoreCommit` class provides interfaces for committing changes, expiring old snapshots,
/// dropping partitions, and retrieving commit metrics.
class PAIMON_EXPORT FileStoreCommit {
 public:
    /// Create an instance of `FileStoreCommit`.
    ///
    /// @param context A unique pointer to the `CommitContext` used for commit operations.
    ///
    /// @return A Result containing a unique pointer to the `FileStoreCommit` instance.
    static Result<std::unique_ptr<FileStoreCommit>> Create(std::unique_ptr<CommitContext> context);

    virtual ~FileStoreCommit() = default;

    /// Commit changes to the file store.
    ///
    /// @param commit_messages A vector of commit messages to be committed.
    /// @param commit_identifier An optional identifier for the commit operation. Default is
    /// `BATCH_WRITE_COMMIT_IDENTIFIER`.
    /// @param watermark An optional event-time watermark used to indicate the progress of data
    ///     processing. Default is std::nullopt.
    /// @return Status indicating the success or failure of the commit operation.
    virtual Status Commit(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                          int64_t commit_identifier = BATCH_WRITE_COMMIT_IDENTIFIER,
                          std::optional<int64_t> watermark = std::nullopt) = 0;

    /// Filter out all `std::vector<CommitMessage>` which have been committed and commit the
    /// remaining ones.
    ///
    /// Compared to commit, this method will first check if a commit_identifier has been
    /// committed, so this method might be slower. A common usage of this method is to retry the
    /// commit process after a failure.
    ///
    /// @param commit_identifier_and_messages A map containing all {@link CommitMessage}s in
    ///     question. The key is the commit_identifier.
    ///
    /// @param watermark An optional event-time watermark used to indicate the progress of data
    ///     processing. Default is std::nullopt.
    /// @return Number of `std::vector<CommitMessage>` committed.
    virtual Result<int32_t> FilterAndCommit(
        const std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>>&
            commit_identifier_and_messages,
        std::optional<int64_t> watermark = std::nullopt) = 0;

    /// Overwrite from manifest committable and partition.
    ///
    /// @param partition A single partition maps each partition key to a partition value. Depending
    ///     on the user-defined statement, the partition might not include all partition keys. Also
    ///     note that this partition does not necessarily equal to the partitions of the newly added
    ///     key-values. This is just the partition to be cleaned up.
    /// @param commit_messages Description of the commit messages.
    /// @param commit_identifier Unique identifier.
    /// @param watermark An optional event-time watermark used to indicate the progress of data
    ///     processing. Default is std::nullopt.
    /// @return Result of the operation.
    virtual Status Overwrite(const std::map<std::string, std::string>& partition,
                             const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                             int64_t commit_identifier,
                             std::optional<int64_t> watermark = std::nullopt) = 0;

    /// This is a temporary interface for internal use. It will be removed in a future version.
    /// Please do not rely on it for long-term use.
    ///
    /// @param partition Description of the partition.
    /// @param commit_messages Description of the commit messages.
    /// @param commit_identifier Unique identifier.
    /// @param watermark An optional event-time watermark used to indicate the progress of data
    ///     processing. Default is std::nullopt.
    /// @return Result of the operation.
    virtual Result<int32_t> FilterAndOverwrite(
        const std::map<std::string, std::string>& partition,
        const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
        int64_t commit_identifier, std::optional<int64_t> watermark = std::nullopt) = 0;

    /// If user want to use REST catalog commit, please set
    /// `CommitContextBuilder::UseRESTCatalogCommit()`, then call `Commit()` (or
    /// `FilterAndCommit()`) normally, then call this method to get the last commit table request,
    /// which is a JSON string that can be used to send to REST catalog server.
    ///
    /// @note Temporary interface for internal use, will be removed in the future.
    ///
    /// @return A Result containing a JSON string which including `snapshot` and `statistics`, but
    /// excluding `tableId`.
    virtual Result<std::string> GetLastCommitTableRequest() = 0;

    /// Expire old snapshot in the file store.
    ///
    /// @return Result<int32_t> indicating the number of expired items or an error status.
    virtual Result<int32_t> Expire() = 0;

    /// Drop specified partitions from the file store.
    ///
    /// @param partitions A vector of partitions to be dropped.
    /// @param commit_identifier An identifier for the commit operation.
    /// @return Status indicating the success or failure of the drop partition operation.
    virtual Status DropPartition(const std::vector<std::map<std::string, std::string>>& partitions,
                                 int64_t commit_identifier) = 0;

    /// Truncate the whole table by overwriting all partitions with empty data. The generated
    /// snapshot has commit kind OVERWRITE.
    ///
    /// @param commit_identifier An identifier for the commit operation.
    /// @return Status indicating the success or failure of the truncate operation.
    virtual Status TruncateTable(int64_t commit_identifier) = 0;

    /// Abort an unsuccessful commit. The data and index files described by the given commit
    /// messages will be deleted on a best-effort basis (delete failures are ignored).
    ///
    /// @param commit_messages A vector of commit messages whose files should be cleaned up.
    /// @return Status indicating the success or failure of the abort operation.
    virtual Status Abort(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages) = 0;

    /// Roll back to the target snapshot and materialize it as the latest snapshot.
    ///
    /// Reads the surviving files of both the current latest snapshot and the target
    /// snapshot, then commits an OVERWRITE snapshot whose visible state equals the target.
    ///
    /// @param target_snapshot_id The snapshot id to roll back to.
    /// @return Result<bool>; true if the atomic commit succeeded. Returns an error status if
    ///     there is no latest snapshot or the target snapshot does not exist.
    virtual Result<bool> RollbackToAsLatest(int64_t target_snapshot_id) = 0;

    /// Configure row-id conflict checking from a specific snapshot id.
    ///
    /// If set to a snapshot id, commit conflict detection will additionally validate row-id
    /// conflicts against snapshots after that id. Passing std::nullopt disables this behavior.
    ///
    /// @param row_id_check_from_snapshot Snapshot id to start row-id conflict checks from, or
    ///     std::nullopt to disable.
    /// @return Current commit object for chaining.
    virtual FileStoreCommit& RowIdCheckConflict(
        std::optional<int64_t> row_id_check_from_snapshot) = 0;

    /// Retrieve metrics related to commit operations.
    ///
    /// @return A shared pointer to a `Metrics` object containing commit metrics.
    virtual std::shared_ptr<Metrics> GetCommitMetrics() const = 0;
};

}  // namespace paimon
