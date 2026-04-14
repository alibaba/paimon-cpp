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

#include <cstdint>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/format/parquet/row_ranges.h"

namespace paimon::parquet::test {

class RowRangesTest : public ::testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RowRangesTest, TestCreateSingle) {
    RowRanges ranges = RowRanges::CreateSingle(100);
    EXPECT_FALSE(ranges.IsEmpty());
    EXPECT_EQ(100, ranges.RowCount());
    EXPECT_EQ(1, ranges.GetRanges().size());
    EXPECT_EQ(0, ranges.GetRanges()[0].from);
    EXPECT_EQ(99, ranges.GetRanges()[0].to);
}

TEST_F(RowRangesTest, TestCreateEmpty) {
    RowRanges ranges = RowRanges::CreateEmpty();
    EXPECT_TRUE(ranges.IsEmpty());
    EXPECT_EQ(0, ranges.RowCount());
    EXPECT_EQ(0, ranges.GetRanges().size());
}

TEST_F(RowRangesTest, TestAddRange) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(10, 20));
    EXPECT_FALSE(ranges.IsEmpty());
    EXPECT_EQ(11, ranges.RowCount());
    EXPECT_EQ(1, ranges.GetRanges().size());
}

TEST_F(RowRangesTest, TestAddOverlappingRanges) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(10, 20));
    ranges.Add(RowRanges::Range(15, 25));  // overlaps with [10, 20]
    EXPECT_EQ(1, ranges.GetRanges().size());
    EXPECT_EQ(10, ranges.GetRanges()[0].from);
    EXPECT_EQ(25, ranges.GetRanges()[0].to);
    EXPECT_EQ(16, ranges.RowCount());
}

TEST_F(RowRangesTest, TestAddAdjacentRanges) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(10, 20));
    ranges.Add(RowRanges::Range(21, 30));  // adjacent to [10, 20]
    EXPECT_EQ(1, ranges.GetRanges().size());
    EXPECT_EQ(10, ranges.GetRanges()[0].from);
    EXPECT_EQ(30, ranges.GetRanges()[0].to);
    EXPECT_EQ(21, ranges.RowCount());
}

TEST_F(RowRangesTest, TestAddNonOverlappingRanges) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(10, 20));
    ranges.Add(RowRanges::Range(30, 40));
    EXPECT_EQ(2, ranges.GetRanges().size());
    EXPECT_EQ(10, ranges.GetRanges()[0].from);
    EXPECT_EQ(20, ranges.GetRanges()[0].to);
    EXPECT_EQ(30, ranges.GetRanges()[1].from);
    EXPECT_EQ(40, ranges.GetRanges()[1].to);
    EXPECT_EQ(22, ranges.RowCount());
}

TEST_F(RowRangesTest, TestUnion) {
    RowRanges left;
    left.Add(RowRanges::Range(10, 20));
    left.Add(RowRanges::Range(40, 50));

    RowRanges right;
    right.Add(RowRanges::Range(15, 25));
    right.Add(RowRanges::Range(60, 70));

    RowRanges result = RowRanges::Union(left, right);
    EXPECT_EQ(3, result.GetRanges().size());
    EXPECT_EQ(10, result.GetRanges()[0].from);
    EXPECT_EQ(25, result.GetRanges()[0].to);
    EXPECT_EQ(40, result.GetRanges()[1].from);
    EXPECT_EQ(50, result.GetRanges()[1].to);
    EXPECT_EQ(60, result.GetRanges()[2].from);
    EXPECT_EQ(70, result.GetRanges()[2].to);
}

TEST_F(RowRangesTest, TestUnionWithOverlap) {
    RowRanges left;
    left.Add(RowRanges::Range(10, 30));

    RowRanges right;
    right.Add(RowRanges::Range(20, 40));

    RowRanges result = RowRanges::Union(left, right);
    EXPECT_EQ(1, result.GetRanges().size());
    EXPECT_EQ(10, result.GetRanges()[0].from);
    EXPECT_EQ(40, result.GetRanges()[0].to);
}

TEST_F(RowRangesTest, TestIntersection) {
    RowRanges left;
    left.Add(RowRanges::Range(10, 30));
    left.Add(RowRanges::Range(50, 70));

    RowRanges right;
    right.Add(RowRanges::Range(20, 40));
    right.Add(RowRanges::Range(60, 80));

    RowRanges result = RowRanges::Intersection(left, right);
    EXPECT_EQ(2, result.GetRanges().size());
    EXPECT_EQ(20, result.GetRanges()[0].from);
    EXPECT_EQ(30, result.GetRanges()[0].to);
    EXPECT_EQ(60, result.GetRanges()[1].from);
    EXPECT_EQ(70, result.GetRanges()[1].to);
}

TEST_F(RowRangesTest, TestIntersectionNoOverlap) {
    RowRanges left;
    left.Add(RowRanges::Range(10, 20));

    RowRanges right;
    right.Add(RowRanges::Range(30, 40));

    RowRanges result = RowRanges::Intersection(left, right);
    EXPECT_TRUE(result.IsEmpty());
}

TEST_F(RowRangesTest, TestIntersectionEmptyLeft) {
    RowRanges left = RowRanges::CreateEmpty();

    RowRanges right;
    right.Add(RowRanges::Range(10, 20));

    RowRanges result = RowRanges::Intersection(left, right);
    EXPECT_TRUE(result.IsEmpty());
}

TEST_F(RowRangesTest, TestIsOverlapping) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(10, 20));
    ranges.Add(RowRanges::Range(30, 40));

    EXPECT_TRUE(ranges.IsOverlapping(10, 20));
    EXPECT_TRUE(ranges.IsOverlapping(15, 25));
    EXPECT_TRUE(ranges.IsOverlapping(30, 40));
    EXPECT_FALSE(ranges.IsOverlapping(21, 29));
    EXPECT_FALSE(ranges.IsOverlapping(5, 9));
    EXPECT_FALSE(ranges.IsOverlapping(41, 50));
}

TEST_F(RowRangesTest, TestRowCount) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(0, 9));
    ranges.Add(RowRanges::Range(20, 29));
    EXPECT_EQ(20, ranges.RowCount());

    ranges.Add(RowRanges::Range(10, 19));  // Fill the gap
    EXPECT_EQ(30, ranges.RowCount());
}

TEST_F(RowRangesTest, TestToString) {
    RowRanges ranges;
    ranges.Add(RowRanges::Range(10, 20));
    ranges.Add(RowRanges::Range(30, 40));
    EXPECT_EQ("[[10, 20], [30, 40]]", ranges.ToString());
}

TEST_F(RowRangesTest, TestRangeOperations) {
    RowRanges::Range r1(10, 20);
    RowRanges::Range r2(30, 40);
    RowRanges::Range r3(15, 25);

    EXPECT_TRUE(r1.IsBefore(r2));
    EXPECT_FALSE(r1.IsAfter(r2));
    EXPECT_FALSE(r1.IsBefore(r3));
    EXPECT_FALSE(r1.IsAfter(r3));
    EXPECT_EQ(11, r1.Count());
}

}  // namespace paimon::parquet::test