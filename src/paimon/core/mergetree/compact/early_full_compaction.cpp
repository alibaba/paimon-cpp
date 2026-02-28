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

#include "paimon/core/mergetree/compact/early_full_compaction.h"

#include "paimon/common/utils/date_time_utils.h"
namespace paimon {
std::shared_ptr<EarlyFullCompaction> EarlyFullCompaction::Create(const CoreOptions& options) {
    std::optional<int64_t> interval = options.GetOptimizedCompactionInterval();
    std::optional<int64_t> total_size_threshold = options.GetCompactionTotalSizeThreshold();
    std::optional<int64_t> incremental_size_threshold =
        options.GetCompactionIncrementalSizeThreshold();
    if (!interval && !total_size_threshold && !incremental_size_threshold) {
        return nullptr;
    }
    return std::shared_ptr<EarlyFullCompaction>(
        new EarlyFullCompaction(interval, total_size_threshold, incremental_size_threshold));
}

void EarlyFullCompaction::UpdateLastFullCompaction() {
    last_full_compaction_ = CurrentTimeMillis();
}

int64_t EarlyFullCompaction::CurrentTimeMillis() const {
    return DateTimeUtils::GetCurrentUTCTimeUs() /
           DateTimeUtils::CONVERSION_FACTORS[DateTimeUtils::TimeType::MILLISECOND];
}
EarlyFullCompaction::EarlyFullCompaction(const std::optional<int64_t>& full_compaction_interval,
                                         const std::optional<int64_t>& total_size_threshold,
                                         const std::optional<int64_t>& incremental_size_threshold)
    : full_compaction_interval_(full_compaction_interval),
      total_size_threshold_(total_size_threshold),
      incremental_size_threshold_(incremental_size_threshold) {}

std::optional<CompactUnit> EarlyFullCompaction::TryFullCompact(
    int32_t num_levels, const std::vector<LevelSortedRun>& runs) {
    if (runs.empty() || runs.size() == 1) {
        return std::nullopt;
    }
    int32_t max_level = num_levels - 1;
    if (full_compaction_interval_) {
        if (!last_full_compaction_ || CurrentTimeMillis() - last_full_compaction_.value() >
                                          full_compaction_interval_.value()) {
            UpdateLastFullCompaction();
            return CompactUnit::FromLevelRuns(max_level, runs);
        }
    }
    if (total_size_threshold_) {
        int64_t total_size = 0;
        for (const auto& run : runs) {
            total_size += run.run.TotalSize();
        }
        if (total_size < total_size_threshold_.value()) {
            UpdateLastFullCompaction();
            return CompactUnit::FromLevelRuns(max_level, runs);
        }
    }
    if (incremental_size_threshold_) {
        int64_t incremental_size = 0;
        for (const auto& run : runs) {
            if (run.level != max_level) {
                incremental_size += run.run.TotalSize();
            }
        }
        if (incremental_size > incremental_size_threshold_.value()) {
            UpdateLastFullCompaction();
            return CompactUnit::FromLevelRuns(max_level, runs);
        }
    }
    return std::nullopt;
}
}  // namespace paimon
