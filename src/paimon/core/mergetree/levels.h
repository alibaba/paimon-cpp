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
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/mergetree/level_sorted_run.h"
#include "paimon/core/mergetree/sorted_run.h"
#include "paimon/result.h"
namespace paimon {
/// A class which stores all level files of merge tree.
class Levels {
 public:
    struct Level0Comparator {
        bool operator()(const std::shared_ptr<DataFileMeta>& a,
                        const std::shared_ptr<DataFileMeta>& b) const;
    };

    static Result<std::unique_ptr<Levels>> Create(
        const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::vector<std::shared_ptr<DataFileMeta>>& input_files, int32_t num_levels);

    static Result<SortedRun> RunOfLevel(int32_t level, const std::vector<SortedRun>& levels);

    const std::set<std::shared_ptr<DataFileMeta>, Level0Comparator>& GetLevel0() const {
        return level0_;
    }

    const std::vector<SortedRun>& GetLevels() const {
        return levels_;
    }
    int32_t NumberOfLevels() const {
        return levels_.size() + 1;
    }

    int32_t MaxLevel() const {
        return levels_.size();
    }

    int32_t NumberOfSortedRuns() const;

    /// A callback to notify dropping file.
    class DropFileCallback {
     public:
        virtual ~DropFileCallback() = default;
        virtual void NotifyDropFile(const std::string& file) = 0;
    };

    void AddDropFileCallback(DropFileCallback* callback);

    void RemoveDropFileCallback(DropFileCallback* callback);

    Status AddLevel0File(const std::shared_ptr<DataFileMeta>& file);

    /// @return the highest non-empty level or -1 if all levels empty.
    int32_t NonEmptyHighestLevel() const;

    int64_t TotalFileSize() const;

    std::vector<std::shared_ptr<DataFileMeta>> AllFiles() const;

    std::vector<LevelSortedRun> LevelSortedRuns() const;

    Status Update(const std::vector<std::shared_ptr<DataFileMeta>>& before,
                  const std::vector<std::shared_ptr<DataFileMeta>>& after);

 private:
    Levels(const std::shared_ptr<FieldsComparator>& key_comparator,
           const std::set<std::shared_ptr<DataFileMeta>, Level0Comparator>& level0,
           const std::vector<SortedRun>& levels)
        : key_comparator_(key_comparator), level0_(level0), levels_(levels) {}

    static Status UpdateLevel(int32_t level,
                              const std::vector<std::shared_ptr<DataFileMeta>>& before,
                              const std::vector<std::shared_ptr<DataFileMeta>>& after,
                              const std::shared_ptr<FieldsComparator>& key_comparator,
                              std::vector<SortedRun>* levels,
                              std::set<std::shared_ptr<DataFileMeta>, Level0Comparator>* level0);

    static std::map<int32_t, std::vector<std::shared_ptr<DataFileMeta>>> GroupByLevel(
        const std::vector<std::shared_ptr<DataFileMeta>>& files);

 private:
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::set<std::shared_ptr<DataFileMeta>, Level0Comparator> level0_;
    std::vector<SortedRun> levels_;
    std::vector<DropFileCallback*> drop_file_callbacks_;
};
}  // namespace paimon
