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
#include <optional>
#include <vector>

#include "paimon/core/compact/compact_result.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class CompactManager {
 public:
    virtual ~CompactManager() = default;
    /// Add a new file.
    virtual void AddNewFile(const std::shared_ptr<DataFileMeta>& file) = 0;

    virtual std::vector<std::shared_ptr<DataFileMeta>> AllFiles() const = 0;

    /// Trigger a new compaction task.
    ///
    /// @param full_compaction if caller needs a guaranteed full compaction
    virtual Status TriggerCompaction(bool full_compaction) = 0;

    /// Get compaction result. Wait finish if `blocking` is true.
    virtual Result<std::optional<std::shared_ptr<CompactResult>>> GetCompactionResult(
        bool blocking) = 0;

    /// Cancel currently running compaction task.
    virtual void CancelCompaction() = 0;

    /// Check if a compaction is in progress, or if a compaction result remains to be fetched, or if
    /// a compaction should be triggered later.
    virtual bool CompactNotCompleted() const = 0;

    virtual bool ShouldWaitForLatestCompaction() const = 0;

    virtual bool ShouldWaitForPreparingCheckpoint() const = 0;

    virtual Status Close() = 0;
};

}  // namespace paimon
