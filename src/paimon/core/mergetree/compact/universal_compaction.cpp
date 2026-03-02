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

#include "paimon/core/mergetree/compact/universal_compaction.h"

#include "paimon/common/utils/date_time_utils.h"
namespace paimon {
UniversalCompaction::UniversalCompaction(
    int32_t max_size_amp, int32_t size_ratio, int32_t num_run_compaction_trigger,
    const std::shared_ptr<EarlyFullCompaction>& early_full_compaction,
    const std::shared_ptr<OffPeakHours>& off_peak_hours)
    : max_size_amp_(max_size_amp),
      size_ratio_(size_ratio),
      num_run_compaction_trigger_(num_run_compaction_trigger),
      early_full_compaction_(early_full_compaction),
      off_peak_hours_(off_peak_hours) {
    assert(num_run_compaction_trigger_ >= 1);
}

Result<std::optional<CompactUnit>> UniversalCompaction::Pick(
    int32_t num_levels, const std::vector<LevelSortedRun>& runs) {
    int32_t max_level = num_levels - 1;
    // 0 try full compaction by trigger
    if (early_full_compaction_) {
        std::optional<CompactUnit> compact_unit =
            early_full_compaction_->TryFullCompact(num_levels, runs);
        if (compact_unit) {
            return compact_unit;
        }
    }
    // 1 checking for reducing size amplification
    std::optional<CompactUnit> compact_unit = PickForSizeAmp(max_level, runs);
    if (compact_unit) {
        return compact_unit;
    }
    // 2 checking for size ratio
    PAIMON_ASSIGN_OR_RAISE(compact_unit, PickForSizeRatio(max_level, runs));
    if (compact_unit) {
        return compact_unit;
    }
    // 3 checking for file num
    if (runs.size() > static_cast<size_t>(num_run_compaction_trigger_)) {
        // compacting for file num
        int32_t candidate_count = runs.size() - num_run_compaction_trigger_ + 1;
        PAIMON_ASSIGN_OR_RAISE(std::optional<CompactUnit> compact_unit,
                               PickForSizeRatio(max_level, runs, candidate_count));
        return compact_unit;
    }
    return std::optional<CompactUnit>();
}

Result<std::optional<CompactUnit>> UniversalCompaction::ForcePickL0(
    int32_t num_levels, const std::vector<LevelSortedRun>& runs) {
    // collect all level 0 files
    int32_t candidate_count = 0;
    for (; static_cast<size_t>(candidate_count) < runs.size(); ++candidate_count) {
        if (runs[candidate_count].level > 0) {
            break;
        }
    }
    if (candidate_count == 0) {
        return std::optional<CompactUnit>();
    }
    return PickForSizeRatio(num_levels - 1, runs, candidate_count, /*force_pick=*/true);
}

std::optional<CompactUnit> UniversalCompaction::PickForSizeAmp(
    int32_t max_level, const std::vector<LevelSortedRun>& runs) {
    if (runs.size() < static_cast<size_t>(num_run_compaction_trigger_)) {
        return std::nullopt;
    }
    int64_t candidate_size = 0;
    for (size_t i = 0; i < runs.size() - 1; ++i) {
        candidate_size += runs[i].run.TotalSize();
    }
    int64_t earliest_size = runs[runs.size() - 1].run.TotalSize();

    // size amplification = percentage of additional size
    if (candidate_size * 100 > max_size_amp_ * earliest_size) {
        if (early_full_compaction_) {
            early_full_compaction_->UpdateLastFullCompaction();
        }
        return CompactUnit::FromLevelRuns(max_level, runs);
    }
    return std::nullopt;
}

Result<std::optional<CompactUnit>> UniversalCompaction::PickForSizeRatio(
    int32_t max_level, const std::vector<LevelSortedRun>& runs) {
    if (runs.size() < static_cast<size_t>(num_run_compaction_trigger_)) {
        return std::optional<CompactUnit>();
    }
    return PickForSizeRatio(max_level, runs, /*candidate_count=*/1);
}

Result<std::optional<CompactUnit>> UniversalCompaction::PickForSizeRatio(
    int32_t max_level, const std::vector<LevelSortedRun>& runs, int32_t candidate_count) {
    return PickForSizeRatio(max_level, runs, candidate_count, /*force_pick=*/false);
}

Result<std::optional<CompactUnit>> UniversalCompaction::PickForSizeRatio(
    int32_t max_level, const std::vector<LevelSortedRun>& runs, int32_t candidate_count,
    bool force_pick) {
    int64_t candidate_size = CandidateSize(runs, candidate_count);
    for (size_t i = candidate_count; i < runs.size(); ++i) {
        LevelSortedRun next = runs[i];
        PAIMON_ASSIGN_OR_RAISE(int32_t current_hour_ratio, RatioForOffPeak());
        if (static_cast<double>(candidate_size) * (100.0 + size_ratio_ + current_hour_ratio) /
                100.0 <
            next.run.TotalSize()) {
            break;
        }
        candidate_size += next.run.TotalSize();
        candidate_count++;
    }
    if (force_pick || candidate_count > 1) {
        return std::optional<CompactUnit>(CreateUnit(runs, max_level, candidate_count));
    }
    return std::optional<CompactUnit>();
}

int64_t UniversalCompaction::CandidateSize(const std::vector<LevelSortedRun>& runs,
                                           int32_t candidate_count) {
    int64_t size = 0;
    for (int32_t i = 0; i < candidate_count; ++i) {
        size += runs[i].run.TotalSize();
    }
    return size;
}

Result<int32_t> UniversalCompaction::RatioForOffPeak() const {
    PAIMON_ASSIGN_OR_RAISE(int32_t local_hour, DateTimeUtils::GetCurrentLocalHour());
    return !off_peak_hours_ ? 0 : off_peak_hours_->CurrentRatio(local_hour);
}

CompactUnit UniversalCompaction::CreateUnit(const std::vector<LevelSortedRun>& runs,
                                            int32_t max_level, int32_t run_count) {
    int32_t output_level;
    if (static_cast<size_t>(run_count) == runs.size()) {
        output_level = max_level;
    } else {
        // level of next run - 1
        output_level = std::max(0, runs[run_count].level - 1);
    }

    if (output_level == 0) {
        // do not output level 0
        for (size_t i = run_count; i < runs.size(); ++i) {
            LevelSortedRun next = runs[i];
            run_count++;
            if (next.level != 0) {
                output_level = next.level;
                break;
            }
        }
    }
    if (static_cast<size_t>(run_count) == runs.size()) {
        if (early_full_compaction_) {
            early_full_compaction_->UpdateLastFullCompaction();
        }
        output_level = max_level;
    }
    std::vector<LevelSortedRun> result_runs;
    result_runs.reserve(run_count);
    for (int32_t i = 0; i < run_count; ++i) {
        result_runs.push_back(runs[i]);
    }
    return CompactUnit::FromLevelRuns(output_level, result_runs);
}
}  // namespace paimon
