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

#include "gtest/gtest.h"

namespace paimon {

TEST(MapSharedShreddingColumnAllocatorTest, BasicAllocation) {
    MapSharedShreddingColumnAllocator allocator(3);

    // 2 fields, K=3 -> all fit, no overflow
    auto result = allocator.AllocateRow({10, 20});
    ASSERT_EQ(3u, result.col_to_field.size());
    ASSERT_EQ(10, result.col_to_field[0]);
    ASSERT_EQ(20, result.col_to_field[1]);
    ASSERT_EQ(-1, result.col_to_field[2]);
    ASSERT_TRUE(result.overflow_fields.empty());
}

TEST(MapSharedShreddingColumnAllocatorTest, ExactlyKFields) {
    MapSharedShreddingColumnAllocator allocator(3);

    auto result = allocator.AllocateRow({0, 1, 2});
    ASSERT_EQ(3u, result.col_to_field.size());
    ASSERT_EQ(0, result.col_to_field[0]);
    ASSERT_EQ(1, result.col_to_field[1]);
    ASSERT_EQ(2, result.col_to_field[2]);
    ASSERT_TRUE(result.overflow_fields.empty());
}

TEST(MapSharedShreddingColumnAllocatorTest, OverflowWhenExceedK) {
    MapSharedShreddingColumnAllocator allocator(2);

    // 4 fields, K=2 -> first 2 assigned, last 2 overflow
    auto result = allocator.AllocateRow({10, 20, 30, 40});
    ASSERT_EQ(2u, result.col_to_field.size());
    ASSERT_EQ(10, result.col_to_field[0]);
    ASSERT_EQ(20, result.col_to_field[1]);

    ASSERT_EQ(2u, result.overflow_fields.size());
    ASSERT_EQ(30, result.overflow_fields[0]);
    ASSERT_EQ(40, result.overflow_fields[1]);
}

TEST(MapSharedShreddingColumnAllocatorTest, EmptyRow) {
    MapSharedShreddingColumnAllocator allocator(3);

    auto result = allocator.AllocateRow({});
    ASSERT_EQ(3u, result.col_to_field.size());
    ASSERT_EQ(-1, result.col_to_field[0]);
    ASSERT_EQ(-1, result.col_to_field[1]);
    ASSERT_EQ(-1, result.col_to_field[2]);
    ASSERT_TRUE(result.overflow_fields.empty());
}

TEST(MapSharedShreddingColumnAllocatorTest, MaxRowWidthTracked) {
    MapSharedShreddingColumnAllocator allocator(3);

    allocator.AllocateRow({1, 2});
    ASSERT_EQ(2, allocator.GetMaxRowWidth());

    allocator.AllocateRow({1, 2, 3, 4, 5});
    ASSERT_EQ(5, allocator.GetMaxRowWidth());

    allocator.AllocateRow({1});
    ASSERT_EQ(5, allocator.GetMaxRowWidth());
}

TEST(MapSharedShreddingColumnAllocatorTest, FieldToColumnsAccumulated) {
    MapSharedShreddingColumnAllocator allocator(3);

    allocator.AllocateRow({10, 20, 30});
    allocator.AllocateRow({20, 40});

    auto field_to_cols = allocator.GetFieldToColumns();
    // field 10 -> {0}
    ASSERT_EQ(std::set<int32_t>({0}), field_to_cols.at(10));
    // field 20 -> {1, 0} (col 1 in row 0, col 0 in row 1)
    ASSERT_EQ(std::set<int32_t>({0, 1}), field_to_cols.at(20));
    // field 30 -> {2}
    ASSERT_EQ(std::set<int32_t>({2}), field_to_cols.at(30));
    // field 40 -> {1}
    ASSERT_EQ(std::set<int32_t>({1}), field_to_cols.at(40));
}

TEST(MapSharedShreddingColumnAllocatorTest, OverflowFieldSetAccumulated) {
    MapSharedShreddingColumnAllocator allocator(2);

    allocator.AllocateRow({1, 2, 3});     // 3 overflows
    allocator.AllocateRow({4, 5, 6, 7});  // 6, 7 overflow

    auto overflow_set = allocator.GetOverflowFieldSet();
    ASSERT_EQ(std::set<int32_t>({3, 6, 7}), overflow_set);
}

TEST(MapSharedShreddingColumnAllocatorTest, ResetClearsAll) {
    MapSharedShreddingColumnAllocator allocator(2);

    allocator.AllocateRow({1, 2, 3});
    ASSERT_EQ(3, allocator.GetMaxRowWidth());
    ASSERT_FALSE(allocator.GetFieldToColumns().empty());
    ASSERT_FALSE(allocator.GetOverflowFieldSet().empty());

    allocator.Reset();
    ASSERT_EQ(0, allocator.GetMaxRowWidth());
    ASSERT_TRUE(allocator.GetFieldToColumns().empty());
    ASSERT_TRUE(allocator.GetOverflowFieldSet().empty());

    // Works correctly after reset
    auto result = allocator.AllocateRow({10});
    ASSERT_EQ(2u, result.col_to_field.size());
    ASSERT_EQ(10, result.col_to_field[0]);
    ASSERT_EQ(-1, result.col_to_field[1]);
}

TEST(MapSharedShreddingColumnAllocatorTest, GetNumColumns) {
    MapSharedShreddingColumnAllocator allocator(5);
    ASSERT_EQ(5, allocator.GetNumColumns());
}

TEST(MapSharedShreddingColumnAllocatorTest, SingleColumnAllocator) {
    MapSharedShreddingColumnAllocator allocator(1);

    auto result = allocator.AllocateRow({10, 20, 30});
    ASSERT_EQ(1u, result.col_to_field.size());
    ASSERT_EQ(10, result.col_to_field[0]);

    ASSERT_EQ(2u, result.overflow_fields.size());
    ASSERT_EQ(20, result.overflow_fields[0]);
    ASSERT_EQ(30, result.overflow_fields[1]);
}

}  // namespace paimon
