/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/utils/range.h"

#include "gtest/gtest.h"

namespace paimon::test {
TEST(RangeTest, TestSimple) {
    Range range(/*from=*/0, /*to=*/5);
    ASSERT_EQ(range.Count(), 6);
    ASSERT_EQ(range.ToString(), "[0, 5]");
}

TEST(RangeTest, TestHasIntersection) {
    {
        Range r1(10, 20);
        Range r2(15, 25);
        ASSERT_TRUE(Range::HasIntersection(r1, r2));
        ASSERT_TRUE(Range::HasIntersection(r2, r1));
        ASSERT_TRUE(Range::HasIntersection(r1, r1));
        ASSERT_TRUE(Range::HasIntersection(r2, r2));
    }
    {
        Range r1(10, 20);
        Range r2(21, 30);
        ASSERT_FALSE(Range::HasIntersection(r1, r2));
        ASSERT_FALSE(Range::HasIntersection(r2, r1));
    }
    {
        Range r1(10, 20);
        Range r2(20, 30);
        ASSERT_TRUE(Range::HasIntersection(r1, r2));
        ASSERT_TRUE(Range::HasIntersection(r2, r1));
    }
    {
        Range r1(10, 20);
        Range r2(12, 18);
        ASSERT_TRUE(Range::HasIntersection(r1, r2));
        ASSERT_TRUE(Range::HasIntersection(r2, r1));
    }
}

TEST(RangeTest, TestIntersection) {
    {
        Range r1(10, 20);
        Range r2(15, 25);
        ASSERT_EQ(Range::Intersection(r1, r2), Range(15, 20));
        ASSERT_EQ(Range::Intersection(r2, r1), Range(15, 20));
        ASSERT_EQ(Range::Intersection(r1, r1), r1);
        ASSERT_EQ(Range::Intersection(r2, r2), r2);
    }
    {
        Range r1(10, 20);
        Range r2(21, 30);
        ASSERT_FALSE(Range::Intersection(r1, r2));
        ASSERT_FALSE(Range::Intersection(r2, r1));
    }
    {
        Range r1(10, 20);
        Range r2(20, 30);
        ASSERT_EQ(Range::Intersection(r1, r2), Range(20, 20));
        ASSERT_EQ(Range::Intersection(r2, r1), Range(20, 20));
    }
    {
        Range r1(10, 20);
        Range r2(12, 18);
        ASSERT_EQ(Range::Intersection(r1, r2), r2);
        ASSERT_EQ(Range::Intersection(r2, r1), r2);
    }
}

TEST(RangeTest, TestCompare) {
    Range r1(10, 20);
    Range r2(15, 25);
    Range r3(10, 30);
    ASSERT_EQ(r1, r1);
    ASSERT_TRUE(r1 < r2);
    ASSERT_TRUE(r1 < r3);
    ASSERT_TRUE(r3 < r2);
}

TEST(RangeTest, TestSortAndMergeOverlap) {
    {
        // test simple
        std::vector<Range> ranges = {Range(0, 10), Range(5, 15)};
        auto result = Range::SortAndMergeOverlap(ranges, /*adjacent=*/false);
        std::vector<Range> expected = {Range(0, 15)};
        ASSERT_EQ(result, expected);
    }
    {
        // test no overlap with adjacent = true
        std::vector<Range> ranges = {Range(0, 10), Range(11, 20)};
        auto result = Range::SortAndMergeOverlap(ranges, /*adjacent=*/true);
        std::vector<Range> expected = {Range(0, 20)};
        ASSERT_EQ(result, expected);
    }
    {
        // test no overlap with adjacent = false
        std::vector<Range> ranges = {Range(0, 10), Range(11, 20)};
        auto result = Range::SortAndMergeOverlap(ranges, /*adjacent=*/false);
        std::vector<Range> expected = {Range(0, 10), Range(11, 20)};
        ASSERT_EQ(result, expected);
    }
    {
        // test overlap multiple
        std::vector<Range> ranges = {Range(0, 10), Range(5, 15), Range(12, 20)};
        auto result = Range::SortAndMergeOverlap(ranges, /*adjacent=*/false);
        std::vector<Range> expected = {Range(0, 20)};
        ASSERT_EQ(result, expected);
    }
    {
        // test overlap mixed
        std::vector<Range> ranges = {Range(0, 10), Range(5, 15), Range(20, 30), Range(25, 35)};
        auto result = Range::SortAndMergeOverlap(ranges, /*adjacent=*/false);
        std::vector<Range> expected = {Range(0, 15), Range(20, 35)};
        ASSERT_EQ(result, expected);
    }
    {
        // test overlap unsorted
        std::vector<Range> ranges = {Range(20, 30), Range(0, 10), Range(5, 15)};
        auto result = Range::SortAndMergeOverlap(ranges, /*adjacent=*/false);
        std::vector<Range> expected = {Range(0, 15), Range(20, 30)};
        ASSERT_EQ(result, expected);
    }
    {
        // test overlap contained
        std::vector<Range> ranges = {Range(0, 20), Range(5, 10)};
        auto result = Range::SortAndMergeOverlap(ranges, /*adjacent=*/false);
        std::vector<Range> expected = {Range(0, 20)};
        ASSERT_EQ(result, expected);
    }
    {
        // test single
        std::vector<Range> ranges = {Range(0, 10)};
        auto result = Range::SortAndMergeOverlap(ranges, /*adjacent=*/false);
        std::vector<Range> expected = {Range(0, 10)};
        ASSERT_EQ(result, expected);
    }
    {
        // test identical
        std::vector<Range> ranges = {Range(0, 10), Range(0, 10)};
        auto result = Range::SortAndMergeOverlap(ranges, /*adjacent=*/false);
        std::vector<Range> expected = {Range(0, 10)};
        ASSERT_EQ(result, expected);
    }
    {
        // test overlap touching exactly
        std::vector<Range> ranges = {Range(0, 10), Range(10, 20)};
        auto result = Range::SortAndMergeOverlap(ranges, /*adjacent=*/false);
        std::vector<Range> expected = {Range(0, 20)};
        ASSERT_EQ(result, expected);
    }
    {
        // test overlap complex
        std::vector<Range> ranges = {Range(0, 5),   Range(3, 8),   Range(10, 15),
                                     Range(20, 25), Range(22, 28), Range(30, 35)};
        auto result = Range::SortAndMergeOverlap(ranges, /*adjacent=*/false);
        std::vector<Range> expected = {Range(0, 8), Range(10, 15), Range(20, 28), Range(30, 35)};
        ASSERT_EQ(result, expected);
    }
}

TEST(RangeTest, TestAnd) {
    {
        // test and basic
        std::vector<Range> left = {Range(0, 10), Range(20, 30)};
        std::vector<Range> right = {Range(5, 15), Range(25, 35)};
        auto result = Range::And(left, right);
        std::vector<Range> expected = {Range(5, 10), Range(25, 30)};
        ASSERT_EQ(result, expected);
    }
    {
        // test no intersection
        std::vector<Range> left = {Range(0, 10)};
        std::vector<Range> right = {Range(20, 30)};
        auto result = Range::And(left, right);
        ASSERT_TRUE(result.empty());
    }
    {
        // test and same ranges
        std::vector<Range> left = {Range(0, 10)};
        std::vector<Range> right = {Range(0, 10)};
        auto result = Range::And(left, right);
        std::vector<Range> expected = {Range(0, 10)};
        ASSERT_EQ(result, expected);
    }
    {
        // test and partial overlap
        std::vector<Range> left = {Range(0, 10)};
        std::vector<Range> right = {Range(5, 15)};
        auto result = Range::And(left, right);
        std::vector<Range> expected = {Range(5, 10)};
        ASSERT_EQ(result, expected);
    }
    {
        // test and contained
        std::vector<Range> left = {Range(0, 20)};
        std::vector<Range> right = {Range(5, 10)};
        auto result = Range::And(left, right);
        std::vector<Range> expected = {Range(5, 10)};
        ASSERT_EQ(result, expected);
    }
    {
        // test and multiple ranges
        std::vector<Range> left = {Range(0, 10), Range(20, 30), Range(40, 50)};
        std::vector<Range> right = {Range(5, 25), Range(35, 45)};
        auto result = Range::And(left, right);
        std::vector<Range> expected = {Range(5, 10), Range(20, 25), Range(40, 45)};
        ASSERT_EQ(result, expected);
    }
    {
        // test and empty left
        std::vector<Range> left = {};
        std::vector<Range> right = {Range(0, 10)};
        auto result = Range::And(left, right);
        ASSERT_TRUE(result.empty());
    }
    {
        // test and empty right
        std::vector<Range> left = {Range(0, 10)};
        std::vector<Range> right = {};
        auto result = Range::And(left, right);
        ASSERT_TRUE(result.empty());
    }
    {
        // test and touching at boundary
        std::vector<Range> left = {Range(0, 10)};
        std::vector<Range> right = {Range(10, 20)};
        auto result = Range::And(left, right);
        std::vector<Range> expected = {Range(10, 10)};
        ASSERT_EQ(result, expected);
    }
    {
        // test and complex
        std::vector<Range> left = {Range(0, 5), Range(10, 15), Range(20, 25), Range(30, 35)};
        std::vector<Range> right = {Range(3, 12), Range(18, 28), Range(32, 40)};
        auto result = Range::And(left, right);
        std::vector<Range> expected = {Range(3, 5), Range(10, 12), Range(20, 25), Range(32, 35)};
        ASSERT_EQ(result, expected);
    }
}
}  // namespace paimon::test
