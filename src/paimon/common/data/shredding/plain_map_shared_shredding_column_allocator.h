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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "paimon/common/data/shredding/map_shared_shredding_column_allocator.h"

namespace paimon {

/// Allocator that keeps the input field order and maps it to columns 0..K-1.
class PlainMapSharedShreddingColumnAllocator : public MapSharedShreddingColumnAllocator {
 public:
    explicit PlainMapSharedShreddingColumnAllocator(int32_t num_columns)
        : MapSharedShreddingColumnAllocator(num_columns) {}

    RowAllocation AllocateRow(const std::vector<int32_t>& field_ids) override {
        RowAllocation allocation = AllocateLeadingColumns(field_ids);
        CommitRow(allocation, field_ids);
        return allocation;
    }

 protected:
    RowAllocation AllocateLeadingColumns(const std::vector<int32_t>& field_ids) const {
        RowAllocation allocation;
        allocation.col_to_field.assign(num_columns_, -1);
        for (size_t i = 0; i < field_ids.size(); ++i) {
            int32_t field_id = field_ids[i];
            if (i < static_cast<size_t>(num_columns_)) {
                allocation.col_to_field[static_cast<int32_t>(i)] = field_id;
            } else {
                allocation.overflow_fields.push_back(field_id);
            }
        }
        return allocation;
    }
};

}  // namespace paimon
