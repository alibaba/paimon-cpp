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

#include <algorithm>
#include <cstdint>
#include <vector>

#include "paimon/common/data/shredding/plain_map_shared_shredding_column_allocator.h"

namespace paimon {

/// Allocator that sorts input fields and maps them to columns 0..K-1.
class SequentialMapSharedShreddingColumnAllocator : public PlainMapSharedShreddingColumnAllocator {
 public:
    explicit SequentialMapSharedShreddingColumnAllocator(int32_t num_columns)
        : PlainMapSharedShreddingColumnAllocator(num_columns) {}

    RowAllocation AllocateRow(const std::vector<int32_t>& field_ids) override {
        std::vector<int32_t> sorted_field_ids = field_ids;
        std::sort(sorted_field_ids.begin(), sorted_field_ids.end());
        RowAllocation allocation = AllocateLeadingColumns(sorted_field_ids);
        CommitRow(allocation, sorted_field_ids);
        return allocation;
    }
};

}  // namespace paimon
