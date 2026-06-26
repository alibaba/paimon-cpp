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

#include <cstdint>
#include <vector>

#include "paimon/common/data/shredding/map_shared_shredding_column_allocator.h"

namespace paimon {

/// Allocator that keeps cross-row column state and evicts least-recently-used columns.
class LruMapSharedShreddingColumnAllocator : public MapSharedShreddingColumnAllocator {
 public:
    /// @param num_columns Number of available physical columns.
    explicit LruMapSharedShreddingColumnAllocator(int32_t num_columns);

    RowAllocation AllocateRow(const std::vector<int32_t>& field_ids) override;

 private:
    int32_t SelectColumn(const std::vector<int32_t>& candidates,
                         const std::vector<int32_t>& planned_col_to_field) const;
    void UpdateLastUsed(const RowAllocation& allocation);

    int64_t lru_clock_ = 0;
    std::vector<int32_t> col_field_;
    std::vector<int64_t> last_used_;
};

}  // namespace paimon
