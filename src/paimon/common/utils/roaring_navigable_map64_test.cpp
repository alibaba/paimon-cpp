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

#include "paimon/common/utils/roaring_navigable_map64.h"

#include <gtest/gtest.h>

#include <climits>
#include <vector>

#include "paimon/utils/range.h"

namespace paimon {

class RoaringNavigableMap64Test : public ::testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RoaringNavigableMap64Test, testAddRangeBasic) {
    RoaringNavigableMap64 bitmap;
    bitmap.AddRange(Range(5, 10));

    // Verify the range [5, 10] is added (inclusive on both ends)
    EXPECT_EQ(bitmap.GetLongCardinality(), 6);
    EXPECT_FALSE(bitmap.Contains(4));
    EXPECT_TRUE(bitmap.Contains(5));
    EXPECT_TRUE(bitmap.Contains(7));
    EXPECT_TRUE(bitmap.Contains(10));
    EXPECT_FALSE(bitmap.Contains(11));
}

TEST_F(RoaringNavigableMap64Test, testAddRangeSingleElement) {
    RoaringNavigableMap64 bitmap;
    bitmap.AddRange(Range(100, 100));

    // A range where from == to should add exactly one element
    EXPECT_EQ(bitmap.GetLongCardinality(), 1);
    EXPECT_FALSE(bitmap.Contains(99));
    EXPECT_TRUE(bitmap.Contains(100));
    EXPECT_FALSE(bitmap.Contains(101));
}

TEST_F(RoaringNavigableMap64Test, testAddRangeMultipleNonOverlapping) {
    RoaringNavigableMap64 bitmap;
    bitmap.AddRange(Range(0, 5));
    bitmap.AddRange(Range(10, 15));
    bitmap.AddRange(Range(20, 25));

    // Verify cardinality: 6 + 6 + 6 = 18
    EXPECT_EQ(bitmap.GetLongCardinality(), 18);

    // Verify gaps are not filled
    EXPECT_FALSE(bitmap.Contains(6));
    EXPECT_FALSE(bitmap.Contains(9));
    EXPECT_FALSE(bitmap.Contains(16));
    EXPECT_FALSE(bitmap.Contains(19));

    // Verify ranges contain expected values
    EXPECT_TRUE(bitmap.Contains(0));
    EXPECT_TRUE(bitmap.Contains(5));
    EXPECT_TRUE(bitmap.Contains(10));
    EXPECT_TRUE(bitmap.Contains(15));
    EXPECT_TRUE(bitmap.Contains(20));
    EXPECT_TRUE(bitmap.Contains(25));

    // Verify ToRangeList reconstructs the ranges correctly
    std::vector<Range> ranges = bitmap.ToRangeList();
    EXPECT_EQ(ranges.size(), 3);
    EXPECT_EQ(ranges[0], Range(0, 5));
    EXPECT_EQ(ranges[1], Range(10, 15));
    EXPECT_EQ(ranges[2], Range(20, 25));
}

TEST_F(RoaringNavigableMap64Test, testAddRangeLargeValues) {
    RoaringNavigableMap64 bitmap;
    // Test with values beyond Integer.MAX_VALUE
    int64_t start = static_cast<int64_t>(INT_MAX) + 100L;
    int64_t end = static_cast<int64_t>(INT_MAX) + 200L;
    bitmap.AddRange(Range(start, end));

    EXPECT_EQ(bitmap.GetLongCardinality(), 101);
    EXPECT_FALSE(bitmap.Contains(start - 1));
    EXPECT_TRUE(bitmap.Contains(start));
    EXPECT_TRUE(bitmap.Contains(start + 50));
    EXPECT_TRUE(bitmap.Contains(end));
    EXPECT_FALSE(bitmap.Contains(end + 1));

    // Verify iteration order
    std::vector<int64_t> values;
    for (auto it = bitmap.begin(); it != bitmap.end(); ++it) {
        values.push_back(*it);
    }
    EXPECT_EQ(values.size(), 101);
    EXPECT_EQ(values[0], start);
    EXPECT_EQ(values[100], end);
}

}  // namespace paimon
