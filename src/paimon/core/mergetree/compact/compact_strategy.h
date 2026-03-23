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

#include "paimon/core/compact/compact_unit.h"
#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/mergetree/level_sorted_run.h"
namespace paimon {
/// Compact strategy to decide which files to select for compaction.
class CompactStrategy {
 public:
    virtual ~CompactStrategy() = default;

    /// Pick compaction unit from runs.
    /// @note Compaction is runs-based, not file-based.
    ///       Level 0 is special, one run per file; all other levels are one run per level.
    ///       Compaction is sequential from small level to large level.
    virtual Result<std::optional<CompactUnit>> Pick(int32_t num_levels,
                                                    const std::vector<LevelSortedRun>& runs) = 0;
    /// Pick a compaction unit consisting of all existing files.
    // TODO(xinyu.lxy): support RecordLevelExpire and BucketedDvMaintainer
    static std::optional<CompactUnit> PickFullCompaction(
        int32_t num_levels, const std::vector<LevelSortedRun>& runs,
        const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, bool force_rewrite_all_files) {
        int32_t max_level = num_levels - 1;
        if (runs.empty()) {
            // no sorted run, no need to compact
            return std::nullopt;
        }
        // only max level files
        if (runs.size() == 1 && runs[0].level == max_level) {
            std::vector<std::shared_ptr<DataFileMeta>> files_to_be_compacted;
            const auto& run = runs[0];
            for (const auto& file : run.run.Files()) {
                if (force_rewrite_all_files) {
                    // add all files when force compacted
                    files_to_be_compacted.push_back(file);
                } else if (dv_maintainer && dv_maintainer->DeletionVectorOf(file->file_name)) {
                    // check deletion vector for large files
                    files_to_be_compacted.push_back(file);
                }
                // TODO(xinyu.lxy): support RecordLevelExpire
            }
            if (files_to_be_compacted.empty()) {
                return std::nullopt;
            }
            return CompactUnit::FromFiles(max_level, files_to_be_compacted, /*file_rewrite=*/true);
        }
        // full compaction
        return CompactUnit::FromLevelRuns(max_level, runs);
    }
};
}  // namespace paimon
