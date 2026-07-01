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

#include <set>
#include <vector>

#include "gtest/gtest.h"

namespace paimon::test {
namespace {

void ExpectAllocation(const RowAllocation& allocation, const std::vector<int32_t>& col_to_field,
                      const std::vector<int32_t>& overflow_fields) {
    ASSERT_EQ(col_to_field, allocation.col_to_field);
    ASSERT_EQ(overflow_fields, allocation.overflow_fields);
}

}  // namespace

TEST(LruMapSharedShreddingColumnAllocatorTest, AllocatesWithHitRetainEvictAndOverflow) {
    LruMapSharedShreddingColumnAllocator allocator(3);

    RowAllocation row0 = allocator.AllocateRow({0, 1, 2});
    ExpectAllocation(row0, {0, 1, 2}, {});

    RowAllocation row1 = allocator.AllocateRow({0, 1});
    ExpectAllocation(row1, {0, 1, -1}, {});

    RowAllocation row2 = allocator.AllocateRow({3, 4, 5});
    ExpectAllocation(row2, {4, 5, 3}, {});

    RowAllocation row3 = allocator.AllocateRow({0, 3, 4, 5});
    ExpectAllocation(row3, {4, 5, 3}, {0});

    ASSERT_EQ(4, allocator.GetMaxRowWidth());

    const auto& field_to_columns = allocator.GetFieldToColumns();
    ASSERT_EQ((std::set<int32_t>{0}), field_to_columns.at(0));
    ASSERT_EQ((std::set<int32_t>{1}), field_to_columns.at(1));
    ASSERT_EQ((std::set<int32_t>{2}), field_to_columns.at(2));
    ASSERT_EQ((std::set<int32_t>{2}), field_to_columns.at(3));
    ASSERT_EQ((std::set<int32_t>{0}), field_to_columns.at(4));
    ASSERT_EQ((std::set<int32_t>{1}), field_to_columns.at(5));
    ASSERT_EQ((std::set<int32_t>{0}), allocator.GetOverflowFieldSet());
}

TEST(LruMapSharedShreddingColumnAllocatorTest, HandlesEmptyRows) {
    LruMapSharedShreddingColumnAllocator allocator(2);

    RowAllocation empty_row = allocator.AllocateRow({});
    ExpectAllocation(empty_row, {-1, -1}, {});
    ASSERT_EQ(0, allocator.GetMaxRowWidth());

    RowAllocation row = allocator.AllocateRow({7});
    ExpectAllocation(row, {7, -1}, {});
    ASSERT_EQ(1, allocator.GetMaxRowWidth());
}

}  // namespace paimon::test
