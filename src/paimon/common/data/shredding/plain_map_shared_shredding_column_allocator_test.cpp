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

#include "paimon/common/data/shredding/plain_map_shared_shredding_column_allocator.h"

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

TEST(PlainMapSharedShreddingColumnAllocatorTest, BasicAllocation) {
    PlainMapSharedShreddingColumnAllocator allocator(3);
    // 2 fields, K=3 -> all fit, no overflow
    RowAllocation result = allocator.AllocateRow({10, 20});
    ExpectAllocation(result, {10, 20, -1}, {});
}

TEST(PlainMapSharedShreddingColumnAllocatorTest, ExactlyKFields) {
    PlainMapSharedShreddingColumnAllocator allocator(3);

    RowAllocation result = allocator.AllocateRow({0, 1, 2});
    ExpectAllocation(result, {0, 1, 2}, {});
}

TEST(PlainMapSharedShreddingColumnAllocatorTest, OverflowWhenExceedK) {
    PlainMapSharedShreddingColumnAllocator allocator(2);

    RowAllocation result = allocator.AllocateRow({10, 20, 30, 40});
    ExpectAllocation(result, {10, 20}, {30, 40});
}

TEST(PlainMapSharedShreddingColumnAllocatorTest, EmptyRow) {
    PlainMapSharedShreddingColumnAllocator allocator(3);

    RowAllocation result = allocator.AllocateRow({});
    ExpectAllocation(result, {-1, -1, -1}, {});
}

TEST(PlainMapSharedShreddingColumnAllocatorTest, MaxRowWidthTracked) {
    PlainMapSharedShreddingColumnAllocator allocator(3);

    allocator.AllocateRow({1, 2});
    ASSERT_EQ(2, allocator.GetMaxRowWidth());

    allocator.AllocateRow({1, 2, 3, 4, 5});
    ASSERT_EQ(5, allocator.GetMaxRowWidth());

    allocator.AllocateRow({1});
    ASSERT_EQ(5, allocator.GetMaxRowWidth());
}

TEST(PlainMapSharedShreddingColumnAllocatorTest, FieldToColumnsAccumulated) {
    PlainMapSharedShreddingColumnAllocator allocator(3);

    allocator.AllocateRow({10, 20, 30});
    allocator.AllocateRow({20, 40});

    const auto& field_to_cols = allocator.GetFieldToColumns();
    // field 10 -> {0}
    ASSERT_EQ(std::set<int32_t>({0}), field_to_cols.at(10));
    // field 20 -> {1, 0} (col 1 in row 0, col 0 in row 1)
    ASSERT_EQ(std::set<int32_t>({0, 1}), field_to_cols.at(20));
    // field 30 -> {2}
    ASSERT_EQ(std::set<int32_t>({2}), field_to_cols.at(30));
    // field 40 -> {1}
    ASSERT_EQ(std::set<int32_t>({1}), field_to_cols.at(40));
}

TEST(PlainMapSharedShreddingColumnAllocatorTest, OverflowFieldSetAccumulated) {
    PlainMapSharedShreddingColumnAllocator allocator(2);

    allocator.AllocateRow({1, 2, 3});     // 3 overflows
    allocator.AllocateRow({4, 5, 6, 7});  // 6, 7 overflow

    ASSERT_EQ((std::set<int32_t>{3, 6, 7}), allocator.GetOverflowFieldSet());
}

TEST(PlainMapSharedShreddingColumnAllocatorTest, SingleColumnAllocator) {
    PlainMapSharedShreddingColumnAllocator allocator(1);

    RowAllocation result = allocator.AllocateRow({10, 20, 30});
    ExpectAllocation(result, {10}, {20, 30});
}

TEST(PlainMapSharedShreddingColumnAllocatorTest, UsesInputOrder) {
    PlainMapSharedShreddingColumnAllocator allocator(3);

    RowAllocation row0 = allocator.AllocateRow({2, 0, 1});
    ExpectAllocation(row0, {2, 0, 1}, {});

    RowAllocation row1 = allocator.AllocateRow({4, 3, 5, 6});
    ExpectAllocation(row1, {4, 3, 5}, {6});

    const auto& field_to_columns = allocator.GetFieldToColumns();
    ASSERT_EQ((std::set<int32_t>{1}), field_to_columns.at(0));
    ASSERT_EQ((std::set<int32_t>{2}), field_to_columns.at(1));
    ASSERT_EQ((std::set<int32_t>{0}), field_to_columns.at(2));
    ASSERT_EQ((std::set<int32_t>{1}), field_to_columns.at(3));
    ASSERT_EQ((std::set<int32_t>{0}), field_to_columns.at(4));
    ASSERT_EQ((std::set<int32_t>{2}), field_to_columns.at(5));
    ASSERT_EQ((std::set<int32_t>{6}), allocator.GetOverflowFieldSet());
}

}  // namespace paimon::test
