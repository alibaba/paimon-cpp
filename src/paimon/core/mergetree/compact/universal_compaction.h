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

#include "paimon/core/mergetree/compact/compact_strategy.h"
#include "paimon/core/mergetree/compact/early_full_compaction.h"
#include "paimon/core/mergetree/compact/off_peak_hours.h"

namespace paimon {
/// Universal Compaction Style is a compaction style, targeting the use cases requiring lower write
/// amplification, trading off read amplification and space amplification.
///
/// See RocksDb Universal-Compaction:
/// https://github.com/facebook/rocksdb/wiki/Universal-Compaction.
class UniversalCompaction : public CompactStrategy {
 public:
    UniversalCompaction(int32_t max_size_amp, int32_t size_ratio,
                        int32_t num_run_compaction_trigger,
                        const std::shared_ptr<EarlyFullCompaction>& early_full_compaction,
                        const std::shared_ptr<OffPeakHours>& off_peak_hours);
    Result<std::optional<CompactUnit>> Pick(int32_t num_levels,
                                            const std::vector<LevelSortedRun>& runs) override;

    Result<std::optional<CompactUnit>> ForcePickL0(int32_t num_levels,
                                                   const std::vector<LevelSortedRun>& runs);

 private:
    std::optional<CompactUnit> PickForSizeAmp(int32_t max_level,
                                              const std::vector<LevelSortedRun>& runs);
    Result<std::optional<CompactUnit>> PickForSizeRatio(int32_t max_level,
                                                        const std::vector<LevelSortedRun>& runs);
    Result<std::optional<CompactUnit>> PickForSizeRatio(int32_t max_level,
                                                        const std::vector<LevelSortedRun>& runs,
                                                        int32_t candidate_count);
    Result<std::optional<CompactUnit>> PickForSizeRatio(int32_t max_level,
                                                        const std::vector<LevelSortedRun>& runs,
                                                        int32_t candidate_count, bool force_pick);
    Result<int32_t> RatioForOffPeak() const;
    CompactUnit CreateUnit(const std::vector<LevelSortedRun>& runs, int32_t max_level,
                           int32_t run_count);
    static int64_t CandidateSize(const std::vector<LevelSortedRun>& runs, int32_t candidate_count);

 private:
    int32_t max_size_amp_;
    int32_t size_ratio_;
    int32_t num_run_compaction_trigger_;
    std::shared_ptr<EarlyFullCompaction> early_full_compaction_;
    std::shared_ptr<OffPeakHours> off_peak_hours_;
};
}  // namespace paimon
