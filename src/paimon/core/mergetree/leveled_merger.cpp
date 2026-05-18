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

#include "paimon/core/mergetree/leveled_merger.h"

#include <algorithm>
#include <cassert>

namespace paimon {

LeveledMerger::LeveledMerger(int32_t max_fan_in) : max_fan_in_(max_fan_in) {
    assert(max_fan_in >= 2);
}

void LeveledMerger::SetMaxFanIn(int32_t max_fan_in) {
    assert(max_fan_in >= 2);
    max_fan_in_ = max_fan_in;
}

void LeveledMerger::Clear() {
    levels_.clear();
}

void LeveledMerger::AddFile(const FileChannelInfo& file_info) {
    EnsureLevel(0);
    levels_[0].push_back(file_info);
}

Status LeveledMerger::RunCompactionIfNeeded(const MergeFn& merge_fn) {
    while (HasPendingCompaction()) {
        auto task = PickCompaction();
        PAIMON_ASSIGN_OR_RAISE(FileChannelInfo output, merge_fn(task.input_files));
        ApplyCompactionResult(task, output);
    }
    return Status::OK();
}

Status LeveledMerger::RunFinalCleanupIfNeeded(int32_t target_file_count, const MergeFn& merge_fn) {
    while (GetTotalFileCount() > target_file_count) {
        auto task = PickCleanupBatch(target_file_count);
        PAIMON_ASSIGN_OR_RAISE(FileChannelInfo output, merge_fn(task.input_files));
        ApplyCompactionResult(task, output);
    }
    return Status::OK();
}

bool LeveledMerger::HasPendingCompaction() const {
    for (const auto& level : levels_) {
        if (static_cast<int32_t>(level.size()) >= max_fan_in_) {
            return true;
        }
    }
    return false;
}

void LeveledMerger::ApplyCompactionResult(const CompactionTask& task,
                                          const FileChannelInfo& output) {
    for (const auto& file : task.input_files) {
        RemoveFile(file.channel_id);
    }
    EnsureLevel(task.target_level);
    levels_[task.target_level].push_back(output);
}

LeveledMerger::CompactionTask LeveledMerger::PickCompaction() const {
    for (int32_t i = 0; i < static_cast<int32_t>(levels_.size()); ++i) {
        if (static_cast<int32_t>(levels_[i].size()) >= max_fan_in_) {
            CompactionTask task;
            task.target_level = i + 1;
            task.input_files.assign(levels_[i].begin(), levels_[i].begin() + max_fan_in_);
            return task;
        }
    }
    return {};
}

LeveledMerger::CompactionTask LeveledMerger::PickCleanupBatch(int32_t target_file_count) const {
    int32_t total = GetTotalFileCount();
    if (total <= target_file_count) {
        return {};
    }

    // Collect all files with their levels, sort by size ascending.
    struct LeveledFile {
        int32_t level;
        FileChannelInfo entry;
    };
    std::vector<LeveledFile> all_files;
    for (int32_t l = 0; l < static_cast<int32_t>(levels_.size()); ++l) {
        for (const auto& f : levels_[l]) {
            all_files.push_back({l, f});
        }
    }
    std::sort(all_files.begin(), all_files.end(), [](const LeveledFile& a, const LeveledFile& b) {
        return a.entry.file_size < b.entry.file_size;
    });

    // Merge n files into 1 eliminates (n-1) files.
    // Need to eliminate (total - target_file_count), so n = total - target_file_count + 1.
    // Bounded by max_fan_in_ (max merge width per round).
    int32_t n = std::min(total - target_file_count + 1, max_fan_in_);

    CompactionTask task;
    int32_t max_level = 0;
    for (int32_t i = 0; i < n; ++i) {
        max_level = std::max(max_level, all_files[i].level);
        task.input_files.push_back(all_files[i].entry);
    }
    task.target_level = max_level + 1;
    return task;
}

std::vector<FileChannelInfo> LeveledMerger::GetAllFiles() const {
    std::vector<FileChannelInfo> result;
    for (const auto& level : levels_) {
        for (const auto& file : level) {
            result.push_back(file);
        }
    }
    return result;
}

int32_t LeveledMerger::GetTotalFileCount() const {
    int32_t total = 0;
    for (const auto& level : levels_) {
        total += static_cast<int32_t>(level.size());
    }
    return total;
}

void LeveledMerger::EnsureLevel(int32_t level) {
    while (static_cast<int32_t>(levels_.size()) <= level) {
        levels_.emplace_back();
    }
}

void LeveledMerger::RemoveFile(const FileIOChannel::ID& channel_id) {
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
