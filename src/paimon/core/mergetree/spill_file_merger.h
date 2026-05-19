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

#include <cstdint>
#include <functional>
#include <vector>

#include "paimon/core/disk/file_io_channel.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// Manages spill files in a leveled structure (similar to LSM tree) to minimize read/write
/// amplification during external sort merge operations.
///
/// Files are organized into levels. Level 0 contains the original spill files. When a level
/// accumulates max_fan_in files, they are merged into a single file at the next level. Before the
/// final read, a greedy merge reduces the total file count to <= max_fan_in.
///
/// Read/write amplification: O(log_K(N)) vs O(N/K) for naive sequential merge.
class SpillFileMerger {
 public:
    using MergeFn = std::function<Result<FileChannelInfo>(const std::vector<FileChannelInfo>&)>;

    explicit SpillFileMerger(int32_t max_fan_in);

    /// Update the maximum fan-in (merge width).
    void SetMaxFanIn(int32_t max_fan_in);

    /// Remove all files from all levels.
    void Clear();

    /// Add a new spill file to level 0.
    void AddFile(const FileChannelInfo& file_info);

    /// Merge any single level that has accumulated >= max_fan_in files into one file at the next
    /// level. Repeats until every level has fewer than max_fan_in files.
    Status RunMergeIfNeeded(const MergeFn& merge_fn);

    /// Reduce the total file count across all levels to <= target_file_count by greedily merging
    /// the smallest files first. Each round merges at most max_fan_in files.
    Status RunFinalMergeIfNeeded(int32_t target_file_count, const MergeFn& merge_fn);

    /// Collect all files across all levels into a flat vector.
    std::vector<FileChannelInfo> GetAllFiles() const;

 private:
    struct MergeTask {
        int32_t target_level;
        std::vector<FileChannelInfo> input_files;
    };

    bool NeedMerge() const;
    void ApplyMergeResult(const MergeTask& task, const FileChannelInfo& output);
    MergeTask PickMergeTask() const;
    MergeTask PickFinalMergeBatch(int32_t target_file_count) const;
    int32_t GetTotalFileCount() const;
    void EnsureLevel(int32_t level);
    void RemoveFile(const FileIOChannel::ID& channel_id);

    int32_t max_fan_in_;
    std::vector<std::vector<FileChannelInfo>> levels_;
};

}  // namespace paimon
