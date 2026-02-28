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

#include <atomic>

#include "paimon/core/mergetree/compact/compact_strategy.h"
#include "paimon/core/mergetree/compact/universal_compaction.h"

namespace paimon {
/// A `CompactStrategy` to force compacting level 0 files.
class ForceUpLevel0Compaction : public CompactStrategy {
 public:
    ForceUpLevel0Compaction(const std::shared_ptr<UniversalCompaction>& universal,
                            const std::optional<int32_t>& max_compact_interval)
        : universal_(universal), max_compact_interval_(max_compact_interval) {
        assert(universal_);
        if (max_compact_interval_) {
            compact_trigger_count_ = std::make_unique<std::atomic<int32_t>>(0);
        }
    }

    std::optional<int32_t> MaxCompactInterval() const {
        return max_compact_interval_;
    }

    Result<std::optional<CompactUnit>> Pick(int32_t num_levels,
                                            const std::vector<LevelSortedRun>& runs) override {
        PAIMON_ASSIGN_OR_RAISE(std::optional<CompactUnit> unit, universal_->Pick(num_levels, runs));
        if (unit) {
            return unit;
        }
        if (!max_compact_interval_ || !compact_trigger_count_) {
            return universal_->ForcePickL0(num_levels, runs);
        }

        compact_trigger_count_->fetch_add(1);
        // We must copy max_compact_interval because compare_exchange_strong(T& expected, T desired)
        // modifies 'expected' to the current actual value of the atomic if the comparison fails.
        int32_t expected_compact_interval = max_compact_interval_.value();
        if (compact_trigger_count_->compare_exchange_strong(expected_compact_interval, 0)) {
            // Universal compaction due to max lookup compaction interval
            return universal_->ForcePickL0(num_levels, runs);
        } else {
            // Skip universal compaction due to lookup compaction trigger count is less than the max
            // interval
            return std::optional<CompactUnit>();
        }
    }

 private:
    std::shared_ptr<UniversalCompaction> universal_;
    std::optional<int32_t> max_compact_interval_;
    std::unique_ptr<std::atomic<int32_t>> compact_trigger_count_;
};
}  // namespace paimon
