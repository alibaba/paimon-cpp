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
#include "paimon/core/compact/compact_unit.h"
#include "paimon/core/core_options.h"
namespace paimon {
/// Early trigger full compaction.
class EarlyFullCompaction {
 public:
    virtual ~EarlyFullCompaction() = default;
    /// @return Pointer to `EarlyFullCompaction` if the options contain EarlyFullCompaction
    /// settings; otherwise, nullptr.
    static std::shared_ptr<EarlyFullCompaction> Create(const CoreOptions& options);

    void UpdateLastFullCompaction();

    std::optional<CompactUnit> TryFullCompact(int32_t num_levels,
                                              const std::vector<LevelSortedRun>& runs);

 protected:
    // virtual only for test
    virtual int64_t CurrentTimeMillis() const;

    EarlyFullCompaction(const std::optional<int64_t>& full_compaction_interval,
                        const std::optional<int64_t>& total_size_threshold,
                        const std::optional<int64_t>& incremental_size_threshold);

 private:
    std::optional<int64_t> full_compaction_interval_;
    std::optional<int64_t> total_size_threshold_;
    std::optional<int64_t> incremental_size_threshold_;
    std::optional<int64_t> last_full_compaction_;
};
}  // namespace paimon
