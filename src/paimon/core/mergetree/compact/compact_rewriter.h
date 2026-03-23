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

#include "paimon/core/compact/compact_result.h"
#include "paimon/core/mergetree/sorted_run.h"
#include "paimon/result.h"

namespace paimon {

/// Rewrite sections to new level.
class CompactRewriter {
 public:
    virtual ~CompactRewriter() = default;
    /// Rewrite sections to new level
    ///
    /// @param output_level new level
    /// @param drop_delete whether to drop the deletion, see
    /// `MergeTreeCompactManager::TriggerCompaction`
    /// @param sections list of sections (section is a list of `SortedRun`s, and key intervals
    /// between sections do not overlap)
    /// @return compaction result
    virtual Result<CompactResult> Rewrite(int32_t output_level, bool drop_delete,
                                          const std::vector<std::vector<SortedRun>>& sections) = 0;

    /// Upgrade file to new level, usually file data is not rewritten, only the metadata is updated.
    /// But in some certain scenarios, we must rewrite file too, e.g. `ChangelogMergeTreeRewriter`
    ///
    /// @param output_level new level
    /// @param file file to be updated
    /// @return compaction result
    virtual Result<CompactResult> Upgrade(int32_t output_level,
                                          const std::shared_ptr<DataFileMeta>& file) = 0;

    /// Close rewriter.
    virtual Status Close() = 0;
};

}  // namespace paimon
