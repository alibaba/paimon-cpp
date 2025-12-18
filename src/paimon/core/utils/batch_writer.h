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

#include <memory>

#include "paimon/result.h"

struct ArrowArray;

namespace paimon {

class CommitIncrement;
class RecordBatch;

/// The BatchWriter is responsible for writing data and handling in-progress files used to
/// write yet un-staged data. The incremental files ready to commit is returned to the system by the
/// `PrepareCommit(bool)`.
class BatchWriter {
 public:
    BatchWriter() = default;
    virtual ~BatchWriter() = default;

    /// Add a record batch to the writer.
    virtual Status Write(std::unique_ptr<RecordBatch>&& batch) = 0;
    /// Prepare for a commit.
    ///
    /// @param wait_compaction if this method need to wait for current compaction to complete
    /// @return Incremental files in this snapshot cycle
    virtual Result<CommitIncrement> PrepareCommit(bool wait_compaction) = 0;
    /// Check if a compaction is in progress, or if a compaction result remains to be fetched.
    virtual bool IsCompacting() const = 0;
    /// Close this writer, the call will delete newly generated but not committed files.
    virtual Status Close() = 0;

    virtual std::shared_ptr<Metrics> GetMetrics() const = 0;
};

}  // namespace paimon
