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

#include "paimon/core/mergetree/spill_file_merger.h"

#include <algorithm>
#include <cassert>

namespace paimon {

SpillFileMerger::SpillFileMerger(int32_t max_fan_in) : max_fan_in_(max_fan_in) {
    assert(max_fan_in >= 2);
}

void SpillFileMerger::SetMaxFanIn(int32_t max_fan_in) {
    assert(max_fan_in >= 2);
    max_fan_in_ = max_fan_in;
}

void SpillFileMerger::Clear() {
    levels_.clear();
}

void SpillFileMerger::AddFile(const FileChannelInfo& file_info) {
    EnsureLevel(0);
    levels_[0].push_back(file_info);
}

Status SpillFileMerger::RunMergeIfNeeded(const MergeFn& merge_fn) {
    while (NeedMerge()) {
        auto task = PickMergeTask();
        PAIMON_ASSIGN_OR_RAISE(FileChannelInfo output, merge_fn(task.input_files));
        ApplyMergeResult(task, output);
    }
    return Status::OK();
}

Status SpillFileMerger::RunFinalMergeIfNeeded(int32_t target_file_count, const MergeFn& merge_fn) {
    while (GetTotalFileCount() > target_file_count) {
        auto task = PickFinalMergeBatch(target_file_count);
        PAIMON_ASSIGN_OR_RAISE(FileChannelInfo output, merge_fn(task.input_files));
        ApplyMergeResult(task, output);
    }
    return Status::OK();
}

bool SpillFileMerger::NeedMerge() const {
    for (const auto& level : levels_) {
        if (static_cast<int32_t>(level.size()) >= max_fan_in_) {
            return true;
        }
    }
    return false;
}

void SpillFileMerger::ApplyMergeResult(const MergeTask& task, const FileChannelInfo& output) {
    for (const auto& file : task.input_files) {
        RemoveFile(file.channel_id);
    }
    EnsureLevel(task.target_level);
    levels_[task.target_level].push_back(output);
}

SpillFileMerger::MergeTask SpillFileMerger::PickMergeTask() const {
    for (int32_t i = 0; i < static_cast<int32_t>(levels_.size()); ++i) {
        if (static_cast<int32_t>(levels_[i].size()) >= max_fan_in_) {
            MergeTask task;
            task.target_level = i + 1;
            task.input_files.assign(levels_[i].begin(), levels_[i].begin() + max_fan_in_);
            return task;
        }
    }
    assert(false && "PickMergeTask called but no pending merge");
    return {};
}

SpillFileMerger::MergeTask SpillFileMerger::PickFinalMergeBatch(int32_t target_file_count) const {
    int32_t total = GetTotalFileCount();
    assert(total > target_file_count);

    // Collect all files with their levels, sort by size ascending.
    struct LeveledFile {
        int32_t level;
        FileChannelInfo entry;
    };
    std::vector<LeveledFile> all_files;
    for (int32_t level_idx = 0; level_idx < static_cast<int32_t>(levels_.size()); ++level_idx) {
        for (const auto& file : levels_[level_idx]) {
            all_files.push_back({level_idx, file});
        }
    }
    std::sort(all_files.begin(), all_files.end(),
              [](const LeveledFile& lhs, const LeveledFile& rhs) {
                  return lhs.entry.file_size < rhs.entry.file_size;
              });

    // Merge `files_to_merge` (alias: n) files into 1 eliminates (n-1) files.
    // Need to eliminate (total - target_file_count), so n = total - target_file_count + 1.
    // Bounded by max_fan_in_ (max merge width per round).
    int32_t files_to_merge = std::min(total - target_file_count + 1, max_fan_in_);

    MergeTask task;
    int32_t max_level = 0;
    for (int32_t i = 0; i < files_to_merge; ++i) {
        max_level = std::max(max_level, all_files[i].level);
        task.input_files.push_back(all_files[i].entry);
    }
    task.target_level = max_level + 1;
    return task;
}

std::vector<FileChannelInfo> SpillFileMerger::GetAllFiles() const {
    std::vector<FileChannelInfo> result;
    for (const auto& level : levels_) {
        for (const auto& file : level) {
            result.push_back(file);
        }
    }
    return result;
}

int32_t SpillFileMerger::GetTotalFileCount() const {
    int32_t total = 0;
    for (const auto& level : levels_) {
        total += static_cast<int32_t>(level.size());
    }
    return total;
}

void SpillFileMerger::EnsureLevel(int32_t level) {
    while (static_cast<int32_t>(levels_.size()) <= level) {
        levels_.emplace_back();
    }
}

void SpillFileMerger::RemoveFile(const FileIOChannel::ID& channel_id) {
    for (auto& level : levels_) {
        for (auto it = level.begin(); it != level.end(); ++it) {
            if (it->channel_id == channel_id) {
                level.erase(it);
                return;
            }
        }
    }
}

}  // namespace paimon
