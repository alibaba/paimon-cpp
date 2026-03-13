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

#include "fmt/format.h"
#include "paimon/core/compact/compact_manager.h"
#include "paimon/core/compact/compact_result.h"
#include "paimon/defs.h"

namespace paimon {

/// A `CompactManager` which never compacts.
class NoopCompactManager : public CompactManager {
 public:
    NoopCompactManager() = default;
    ~NoopCompactManager() override = default;

    void AddNewFile(const std::shared_ptr<DataFileMeta>& file) override {}

    std::vector<std::shared_ptr<DataFileMeta>> AllFiles() const override {
        static std::vector<std::shared_ptr<DataFileMeta>> empty;
        return empty;
    }

    Status TriggerCompaction(bool full_compaction) override {
        if (full_compaction) {
            return Status::Invalid(
                fmt::format("NoopCompactManager does not support user triggered compaction.\n"
                            "If you really need a guaranteed compaction, please set {} property of "
                            "this table to false.",
                            Options::WRITE_ONLY));
        }
        return Status::OK();
    }

    /// Get compaction result. Wait finish if `blocking` is true.
    Result<std::optional<std::shared_ptr<CompactResult>>> GetCompactionResult(
        bool blocking) override {
        return std::optional<std::shared_ptr<CompactResult>>();
    }

    /// Cancel currently running compaction task.
    void CancelCompaction() override {}

    /// Check if a compaction is in progress, or if a compaction result remains to be fetched, or if
    /// a compaction should be triggered later.
    bool CompactNotCompleted() const override {
        return false;
    }

    bool ShouldWaitForLatestCompaction() const override {
        return false;
    }

    bool ShouldWaitForPreparingCheckpoint() const override {
        return false;
    }

    Status Close() override {
        return Status::OK();
    }
};

}  // namespace paimon
