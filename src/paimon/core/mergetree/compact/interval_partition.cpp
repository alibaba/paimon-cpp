/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/core/mergetree/compact/interval_partition.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <queue>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/io/data_file_meta.h"

namespace paimon {

IntervalPartition::IntervalPartition(const std::vector<std::shared_ptr<DataFileMeta>>& input_files,
                                     const std::shared_ptr<FieldsComparator>& key_comparator)
    : files_(input_files), key_comparator_(key_comparator) {
    std::stable_sort(
        files_.begin(), files_.end(),
        [this](const std::shared_ptr<DataFileMeta>& o1, const std::shared_ptr<DataFileMeta>& o2) {
            int32_t left_result = key_comparator_->CompareTo(o1->min_key, o2->min_key);
            if (left_result == 0) {
                int32_t right_result = key_comparator_->CompareTo(o1->max_key, o2->max_key);
                return right_result < 0;
            }
            return left_result < 0;
        });
}

std::vector<std::vector<SortedRun>> IntervalPartition::Partition() const {
    std::vector<std::vector<SortedRun>> result;
    std::vector<std::shared_ptr<DataFileMeta>> section;
    BinaryRow bound = BinaryRow::EmptyRow();
    for (const auto& meta : files_) {
        if (!section.empty() && key_comparator_->CompareTo(meta->min_key, bound) > 0) {
            result.push_back(Partition(section));
            section.clear();
            bound = BinaryRow::EmptyRow();
        }
        section.push_back(meta);
        if (bound == BinaryRow::EmptyRow() ||
            key_comparator_->CompareTo(meta->max_key, bound) > 0) {
            bound = section.back()->max_key;
        }
    }

    if (!section.empty()) {
        result.push_back(Partition(section));
    }

    return result;
}

std::vector<SortedRun> IntervalPartition::Partition(
    const std::vector<std::shared_ptr<DataFileMeta>>& metas) const {
    auto comparator = [this](const std::vector<std::shared_ptr<DataFileMeta>>& o1,
                             const std::vector<std::shared_ptr<DataFileMeta>>& o2) {
        int32_t right_result = key_comparator_->CompareTo(o1.back()->max_key, o2.back()->max_key);
        if (right_result == 0) {
            return key_comparator_->CompareTo(o1.back()->min_key, o2.back()->min_key) > 0;
        }
        return right_result > 0;
    };

    std::priority_queue<std::vector<std::shared_ptr<DataFileMeta>>,
                        std::vector<std::vector<std::shared_ptr<DataFileMeta>>>,
                        decltype(comparator)>
        queue(comparator);

    std::vector<std::shared_ptr<DataFileMeta>> first_run = {metas.front()};
    queue.push(first_run);

    for (size_t i = 1; i < metas.size(); ++i) {
        const auto& meta = metas[i];
        auto top = queue.top();
        queue.pop();

        if (key_comparator_->CompareTo(meta->min_key, top.back()->max_key) > 0) {
            top.push_back(meta);
        } else {
            std::vector<std::shared_ptr<DataFileMeta>> new_run = {meta};
            queue.push(new_run);
        }
        queue.push(top);
    }

    std::vector<SortedRun> sorted_runs;
    while (!queue.empty()) {
        sorted_runs.emplace_back(SortedRun::FromSorted(queue.top()));
        queue.pop();
    }
    return sorted_runs;
}

}  // namespace paimon
