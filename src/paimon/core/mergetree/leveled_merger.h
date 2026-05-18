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

/// Manages spill files in a leveled structure (similar to LSM tree) to minimize
/// read/write amplification during external sort merge operations.
///
/// Files are organized into levels. Level 0 contains the original spill files.
/// When a level accumulates max_fan_in files, they are compacted into a single
/// file at the next level. Before the final merge, a greedy cleanup merges the
/// smallest files to reduce total file count to <= max_fan_in.
///
/// Read/write amplification: O(log_K(N)) vs O(N/K) for naive sequential merge.
class LeveledMerger {
 public:
    using MergeFn = std::function<Result<FileChannelInfo>(const std::vector<FileChannelInfo>&)>;

    explicit LeveledMerger(int32_t max_fan_in);

    void SetMaxFanIn(int32_t max_fan_in);
    void Clear();

    void AddFile(const FileChannelInfo& file_info);

    /// Compact any single level that has accumulated >= max_fan_in files,
    /// merging max_fan_in files into one at the next level. Repeats until
    /// every level has fewer than max_fan_in files.
    Status RunCompactionIfNeeded(const MergeFn& merge_fn);

    /// Reduce the total file count across all levels to <= target_file_count
    /// by greedily merging the smallest files first. Each round merges at most
    /// max_fan_in files.
    Status RunFinalCleanupIfNeeded(int32_t target_file_count, const MergeFn& merge_fn);

    std::vector<FileChannelInfo> GetAllFiles() const;

 private:
    struct CompactionTask {
        int32_t target_level;
        std::vector<FileChannelInfo> input_files;
    };

    bool HasPendingCompaction() const;
    void ApplyCompactionResult(const CompactionTask& task, const FileChannelInfo& output);
    CompactionTask PickCompaction() const;
    CompactionTask PickCleanupBatch(int32_t target_file_count) const;
    int32_t GetTotalFileCount() const;
    void EnsureLevel(int32_t level);
    void RemoveFile(const FileIOChannel::ID& channel_id);

    int32_t max_fan_in_;
    std::vector<std::vector<FileChannelInfo>> levels_;
};

}  // namespace paimon
