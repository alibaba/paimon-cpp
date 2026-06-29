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

#include "paimon/common/data/shredding/lru_map_shared_shredding_column_allocator.h"

#include <algorithm>
#include <limits>

namespace paimon {

LruMapSharedShreddingColumnAllocator::LruMapSharedShreddingColumnAllocator(int32_t num_columns)
    : MapSharedShreddingColumnAllocator(num_columns),
      col_field_(num_columns, -1),
      last_used_(num_columns, 0) {}

int32_t LruMapSharedShreddingColumnAllocator::SelectColumn(
    const std::vector<int32_t>& candidates,
    const std::vector<int32_t>& planned_col_to_field) const {
    int32_t selected_col = candidates.front();
    int64_t selected_last_used = std::numeric_limits<int64_t>::max();
    for (int32_t col : candidates) {
        if (planned_col_to_field[col] == -1) {
            return col;
        }
        if (last_used_[col] < selected_last_used) {
            selected_col = col;
            selected_last_used = last_used_[col];
        }
    }
    return selected_col;
}

void LruMapSharedShreddingColumnAllocator::UpdateLastUsed(const RowAllocation& allocation) {
    bool touched = false;
    for (int32_t col = 0; col < num_columns_; ++col) {
        if (allocation.col_to_field[col] != -1) {
            last_used_[col] = lru_clock_;
            touched = true;
        }
    }
    if (touched) {
        ++lru_clock_;
    }
}

RowAllocation LruMapSharedShreddingColumnAllocator::AllocateRow(
    const std::vector<int32_t>& field_ids) {
    std::vector<int32_t> sorted_field_ids = field_ids;
    std::sort(sorted_field_ids.begin(), sorted_field_ids.end());

    RowAllocation allocation;
    allocation.col_to_field.assign(num_columns_, -1);
    std::vector<int32_t> next_col_to_field = col_field_;
    std::vector<bool> used_cols(num_columns_, false);
    std::vector<int32_t> unassigned;

    for (int32_t field_id : sorted_field_ids) {
        auto it = std::find(col_field_.begin(), col_field_.end(), field_id);
        if (it != col_field_.end()) {
            int32_t col = static_cast<int32_t>(it - col_field_.begin());
            used_cols[col] = true;
            allocation.col_to_field[col] = field_id;
        } else {
            unassigned.push_back(field_id);
        }
    }

    for (int32_t field_id : unassigned) {
        std::vector<int32_t> candidates;
        candidates.reserve(num_columns_);
        for (int32_t col = 0; col < num_columns_; ++col) {
            if (!used_cols[col]) {
                candidates.push_back(col);
            }
        }

        if (candidates.empty()) {
            allocation.overflow_fields.push_back(field_id);
            continue;
        }

        int32_t col = SelectColumn(candidates, next_col_to_field);
        used_cols[col] = true;
        allocation.col_to_field[col] = field_id;
        next_col_to_field[col] = field_id;
    }

    UpdateLastUsed(allocation);
    col_field_ = next_col_to_field;
    CommitRow(allocation, sorted_field_ids);
    return allocation;
}

}  // namespace paimon
