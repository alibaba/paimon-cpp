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
#include "paimon/core/mergetree/levels.h"

#include <algorithm>
namespace paimon {
bool Levels::Level0Comparator::operator()(const std::shared_ptr<DataFileMeta>& a,
                                          const std::shared_ptr<DataFileMeta>& b) const {
    if (a->max_sequence_number != b->max_sequence_number) {
        // file with larger sequence number should be in front
        return a->max_sequence_number > b->max_sequence_number;
    } else {
        // When two or more jobs are writing the same merge tree, it is
        // possible that multiple files have the same maxSequenceNumber. In
        // this case we have to compare their file names so that files with
        // same maxSequenceNumber won't be "de-duplicated" by the tree set.
        int64_t min_seq_a = a->min_sequence_number;
        int64_t min_seq_b = b->min_sequence_number;
        if (min_seq_a != min_seq_b) {
            return min_seq_a < min_seq_b;
        }
        // If minSequenceNumber is also the same, use creation time
        Timestamp time_a = a->creation_time;
        Timestamp time_b = b->creation_time;
        if (time_a != time_b) {
            return time_a < time_b;
        }
        // Final fallback: filename (to ensure uniqueness in set)
        return a->file_name < b->file_name;
    }
}

Result<std::unique_ptr<Levels>> Levels::Create(
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::vector<std::shared_ptr<DataFileMeta>>& input_files, int32_t num_levels) {
    // in case the num of levels is not specified explicitly
    int32_t restored_num_levels = -1;
    for (const auto& file : input_files) {
        if (file->level > restored_num_levels) {
            restored_num_levels = file->level;
        }
    }
    restored_num_levels = std::max(restored_num_levels + 1, num_levels);
    if (restored_num_levels <= 1) {
        return Status::Invalid("Number of levels must be at least 2.");
    }

    std::set<std::shared_ptr<DataFileMeta>, Levels::Level0Comparator> level0;
    std::vector<SortedRun> levels;
    levels.reserve(restored_num_levels - 1);
    for (int32_t i = 1; i < restored_num_levels; ++i) {
        levels.push_back(SortedRun::Empty());
    }
    auto level_map = GroupByLevel(input_files);
    for (auto& [level, files] : level_map) {
        PAIMON_RETURN_NOT_OK(
            UpdateLevel(level, /*before=*/{}, /*after=*/files, key_comparator, &levels, &level0));
    }

    size_t total_file_num = level0.size();
    for (const auto& run : levels) {
        total_file_num += run.Files().size();
    }
    if (total_file_num != input_files.size()) {
        return Status::Invalid(
            "Number of files stored in Levels does not equal to the size of inputFiles. This "
            "is unexpected.");
    }
    return std::unique_ptr<Levels>(new Levels(key_comparator, level0, levels));
}

int32_t Levels::NumberOfSortedRuns() const {
    int32_t number_of_runs = level0_.size();
    for (const auto& run : levels_) {
        if (!run.IsEmpty()) {
            number_of_runs++;
        }
    }
    return number_of_runs;
}

void Levels::AddDropFileCallback(DropFileCallback* callback) {
    drop_file_callbacks_.push_back(callback);
}

void Levels::RemoveDropFileCallback(DropFileCallback* callback) {
    drop_file_callbacks_.erase(
        std::remove(drop_file_callbacks_.begin(), drop_file_callbacks_.end(), callback),
        drop_file_callbacks_.end());
}

Status Levels::AddLevel0File(const std::shared_ptr<DataFileMeta>& file) {
    if (file->level != 0) {
        return Status::Invalid("must add level0 file in AddLevel0File");
    }
    level0_.insert(file);
    return Status::OK();
}

int32_t Levels::NonEmptyHighestLevel() const {
    for (int32_t i = levels_.size() - 1; i >= 0; i--) {
        if (!levels_[i].IsEmpty()) {
            return i + 1;
        }
    }
    return level0_.empty() ? -1 : 0;
}

int64_t Levels::TotalFileSize() const {
    int64_t total_size = 0;
    for (const auto& file : level0_) {
        total_size += file->file_size;
    }
    for (const auto& run : levels_) {
        total_size += run.TotalSize();
    }
    return total_size;
}

std::vector<std::shared_ptr<DataFileMeta>> Levels::AllFiles() const {
    std::vector<std::shared_ptr<DataFileMeta>> all_files;
    auto runs = LevelSortedRuns();
    for (const auto& run : runs) {
        all_files.insert(all_files.end(), run.run.Files().begin(), run.run.Files().end());
    }
    return all_files;
}

std::vector<LevelSortedRun> Levels::LevelSortedRuns() const {
    std::vector<LevelSortedRun> runs;
    for (const auto& file : level0_) {
        runs.emplace_back(/*level=*/0, SortedRun::FromSingle(file));
    }
    for (int32_t i = 0; i < static_cast<int32_t>(levels_.size()); i++) {
        const auto& run = levels_[i];
        if (!run.IsEmpty()) {
            runs.emplace_back(/*level=*/i + 1, run);
        }
    }
    return runs;
}

Status Levels::Update(const std::vector<std::shared_ptr<DataFileMeta>>& before,
                      const std::vector<std::shared_ptr<DataFileMeta>>& after) {
    auto grouped_before = GroupByLevel(before);
    auto grouped_after = GroupByLevel(after);
    int32_t number_of_levels = NumberOfLevels();
    for (int32_t i = 0; i < number_of_levels; i++) {
        PAIMON_RETURN_NOT_OK(UpdateLevel(i, grouped_before[i], grouped_after[i], key_comparator_,
                                         &levels_, &level0_));
    }
    if (!drop_file_callbacks_.empty()) {
        std::set<std::string> dropped_files;
        for (const auto& file : before) {
            dropped_files.insert(file->file_name);
        }
        // exclude upgrade files
        for (const auto& file : after) {
            dropped_files.erase(file->file_name);
        }
        for (auto* callback : drop_file_callbacks_) {
            if (!callback) {
                continue;
            }
            for (const auto& file_name : dropped_files) {
                callback->NotifyDropFile(file_name);
            }
        }
    }
    return Status::OK();
}

Status Levels::UpdateLevel(int32_t level, const std::vector<std::shared_ptr<DataFileMeta>>& before,
                           const std::vector<std::shared_ptr<DataFileMeta>>& after,
                           const std::shared_ptr<FieldsComparator>& key_comparator,
                           std::vector<SortedRun>* levels,
                           std::set<std::shared_ptr<DataFileMeta>, Level0Comparator>* level0) {
    if (before.empty() && after.empty()) {
        return Status::OK();
    }
    if (level == 0) {
        for (const auto& file : before) {
            level0->erase(file);
        }
        for (const auto& file : after) {
            level0->insert(file);
        }
    } else {
        PAIMON_ASSIGN_OR_RAISE(SortedRun run, RunOfLevel(level, *levels));
        std::vector<std::shared_ptr<DataFileMeta>> files = run.Files();
        for (const auto& before_file : before) {
            auto iter = std::find_if(files.begin(), files.end(), [&before_file](const auto& cur) {
                return before_file->file_name == cur->file_name;
            });
            if (iter != files.end()) {
                files.erase(iter);
            }
        }
        files.insert(files.end(), after.begin(), after.end());
        PAIMON_ASSIGN_OR_RAISE((*levels)[level - 1],
                               SortedRun::FromUnsorted(files, key_comparator));
    }
    return Status::OK();
}

Result<SortedRun> Levels::RunOfLevel(int32_t level, const std::vector<SortedRun>& levels) {
    if (level <= 0) {
        return Status::Invalid("Level0 does not have one single sorted run.");
    }
    return levels[level - 1];
}

std::map<int32_t, std::vector<std::shared_ptr<DataFileMeta>>> Levels::GroupByLevel(
    const std::vector<std::shared_ptr<DataFileMeta>>& files) {
    std::map<int32_t, std::vector<std::shared_ptr<DataFileMeta>>> level_map;
    for (const auto& file : files) {
        level_map[file->level].push_back(file);
    }
    return level_map;
}

}  // namespace paimon
