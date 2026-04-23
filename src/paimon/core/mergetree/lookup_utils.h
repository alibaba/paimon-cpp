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
#include <optional>

#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/mergetree/levels.h"
#include "paimon/core/mergetree/sorted_run.h"
#include "paimon/result.h"

namespace paimon {
/// Utils for lookup.
class LookupUtils {
 public:
    LookupUtils() = delete;
    ~LookupUtils() = delete;

    template <typename T>
    static Result<std::optional<T>> Lookup(
        const Levels& levels, const std::shared_ptr<InternalRow>& key, int32_t start_level,
        std::function<Result<std::optional<T>>(const std::shared_ptr<InternalRow>&,
                                               const SortedRun&)>
            lookup,
        std::function<Result<std::optional<T>>(
            const std::shared_ptr<InternalRow>&,
            const std::set<std::shared_ptr<DataFileMeta>, Levels::Level0Comparator>&)>
            level0_lookup) {
        std::optional<T> result;
        for (int32_t i = start_level; i < levels.NumberOfLevels(); ++i) {
            if (i == 0) {
                PAIMON_ASSIGN_OR_RAISE(result, level0_lookup(key, levels.GetLevel0()));
            } else {
                PAIMON_ASSIGN_OR_RAISE(SortedRun level, Levels::RunOfLevel(i, levels.GetLevels()));
                PAIMON_ASSIGN_OR_RAISE(result, lookup(key, level));
            }
            if (result) {
                break;
            }
        }
        return result;
    }

    template <typename T>
    static Result<std::optional<T>> LookupLevel0(
        const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::shared_ptr<InternalRow>& target,
        const std::set<std::shared_ptr<DataFileMeta>, Levels::Level0Comparator>& level0,
        std::function<Result<std::optional<T>>(const std::shared_ptr<InternalRow>&,
                                               const std::shared_ptr<DataFileMeta>&)>
            lookup) {
        std::optional<T> result;
        for (const auto& file : level0) {
            if (key_comparator->CompareTo(file->max_key, *target) >= 0 &&
                key_comparator->CompareTo(file->min_key, *target) <= 0) {
                PAIMON_ASSIGN_OR_RAISE(result, lookup(target, file));
                if (result) {
                    break;
                }
            }
        }
        return result;
    }

    template <typename T>
    static Result<std::optional<T>> Lookup(
        const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::shared_ptr<InternalRow>& target, const SortedRun& level,
        std::function<Result<std::optional<T>>(const std::shared_ptr<InternalRow>&,
                                               const std::shared_ptr<DataFileMeta>&)>
            lookup) {
        if (level.IsEmpty()) {
            return std::optional<T>();
        }
        const auto& files = level.Files();
        int32_t left = 0;
        auto right = static_cast<int32_t>(files.size()) - 1;

        // binary search restart positions to find the restart position immediately before the
        // target key
        while (left < right) {
            int32_t mid = (left + right) / 2;
            if (key_comparator->CompareTo(files[mid]->max_key, *target) < 0) {
                // Key at "mid.max" < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            } else {
                // Key at "mid.max" >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        int32_t index = right;
        // if the index is now pointing to the last file, check if the largest key in the block is
        // smaller than the target key.  If so, we need to seek beyond the end of this file
        if (index == static_cast<int32_t>(files.size() - 1) &&
            key_comparator->CompareTo(files[index]->max_key, *target) < 0) {
            index++;
        }

        // if files does not have a next, it means the key does not exist in this level
        return index < static_cast<int32_t>(files.size()) ? lookup(target, files[index])
                                                          : std::optional<T>();
    }
};
}  // namespace paimon
