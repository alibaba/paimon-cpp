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

#include "paimon/common/data/shredding/map_shared_shredding_column_allocator.h"

#include <algorithm>

namespace paimon {

MapSharedShreddingColumnAllocator::MapSharedShreddingColumnAllocator(int32_t num_columns)
    : num_columns_(num_columns) {}

void MapSharedShreddingColumnAllocator::CommitRow(const RowAllocation& allocation,
                                                  const std::vector<int32_t>& field_ids) {
    max_row_width_ = std::max(max_row_width_, static_cast<int32_t>(field_ids.size()));

    for (int32_t col = 0; col < num_columns_; ++col) {
        int32_t field_id = allocation.col_to_field[col];
        if (field_id != -1) {
            field_to_columns_[field_id].insert(col);
        }
    }

    for (int32_t field_id : allocation.overflow_fields) {
        overflow_field_set_.insert(field_id);
    }
}

const std::map<int32_t, std::set<int32_t>>& MapSharedShreddingColumnAllocator::GetFieldToColumns()
    const {
    return field_to_columns_;
}

const std::set<int32_t>& MapSharedShreddingColumnAllocator::GetOverflowFieldSet() const {
    return overflow_field_set_;
}

int32_t MapSharedShreddingColumnAllocator::GetMaxRowWidth() const {
    return max_row_width_;
}

}  // namespace paimon
