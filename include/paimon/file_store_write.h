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
#include <memory>
#include <vector>

#include "paimon/commit_message.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class RecordBatch;
class WriteContext;

/// Interface for write operations in a file store.
class PAIMON_EXPORT FileStoreWrite {
 public:
    /// Create an instance of `FileStoreWrite`.
    ///
    /// @param context A unique pointer to the `WriteContext` used for write operations.
    ///
    /// @return A Result containing a unique pointer to the `FileStoreWrite` instance.
    static Result<std::unique_ptr<FileStoreWrite>> Create(std::unique_ptr<WriteContext> context);

    virtual ~FileStoreWrite() = default;

    /// Support write an input `RecordBatch` to internal buffer or file.
    /// @note If a field in table schema is marked as non-nullable (`nullable = false`),
    ///       the corresponding array in `batch` must have zero null entries.
    virtual Status Write(std::unique_ptr<RecordBatch>&& batch) = 0;

    /// Generate a list of commit messages with the latest generated data file meta
    /// information of the current snapshot.
    ///
    /// When we need commit, call PrepareCommit to get the current {@link CommitMessage}s with the
    /// latest generated data file meta information of the current snapshot.
    ///
    /// This function is designed to be called when a commit is required. Depending on the writing
    /// scenario, the behavior will differ:
    ///
    /// - For batch write, simply call `PrepareCommit()` without any parameters.
    /// - For streaming write, you need to provide both parameters:
    ///     `PrepareCommit(bool wait_compaction, int64_t commit_identifier)`.
    ///
    /// @param wait_compaction Indicates whether to wait for any ongoing compaction process to
    ///                        complete.
    /// @param commit_identifier A unique identifier for the commit operation. This parameter is
    ///                          only relevant in streaming write scenarios.
    ///
    /// @return A Result containing `std::vector<std::shared_ptr<CommitMessage>>` objects,
    ///         representing the generated commit messages.
    virtual Result<std::vector<std::shared_ptr<CommitMessage>>> PrepareCommit(
        bool wait_compaction = true, int64_t commit_identifier = BATCH_WRITE_COMMIT_IDENTIFIER) = 0;
    virtual std::shared_ptr<Metrics> GetMetrics() const = 0;
    virtual Status Close() = 0;
};

}  // namespace paimon
