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

#include "paimon/utils/row_range_index.h"

#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// ======================== Create ========================

TEST(RowRangeIndexTest, CreateWithEmptyRangesReturnsError) {
    ASSERT_NOK_WITH_MSG(RowRangeIndex::Create({}), "Ranges cannot be empty in RowRangeIndex");
}

TEST(RowRangeIndexTest, CreateWithSingleRange) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    ASSERT_EQ(index.Ranges().size(), 1);
    ASSERT_EQ(index.Ranges()[0], Range(10, 20));
}

TEST(RowRangeIndexTest, CreateSortsAndMergesOverlappingRanges) {
    ASSERT_OK_AND_ASSIGN(auto index,
                         RowRangeIndex::Create({Range(20, 30), Range(0, 10), Range(5, 15)}));
    std::vector<Range> expected = {Range(0, 15), Range(20, 30)};
    ASSERT_EQ(index.Ranges(), expected);
}

TEST(RowRangeIndexTest, CreateMergesAdjacentRanges) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(0, 10), Range(11, 20)}));
    std::vector<Range> expected = {Range(0, 20)};
    ASSERT_EQ(index.Ranges(), expected);
}

TEST(RowRangeIndexTest, CreateMergesDuplicateRanges) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(0, 10), Range(0, 10)}));
    std::vector<Range> expected = {Range(0, 10)};
    ASSERT_EQ(index.Ranges(), expected);
}

TEST(RowRangeIndexTest, CreateWithMultipleDisjointRanges) {
    ASSERT_OK_AND_ASSIGN(auto index,
                         RowRangeIndex::Create({Range(0, 5), Range(10, 15), Range(20, 25)}));
    std::vector<Range> expected = {Range(0, 5), Range(10, 15), Range(20, 25)};
    ASSERT_EQ(index.Ranges(), expected);
}

// ======================== Ranges ========================

TEST(RowRangeIndexTest, RangesReturnsUnmodifiableReference) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(0, 10), Range(20, 30)}));
    const auto& ranges1 = index.Ranges();
    const auto& ranges2 = index.Ranges();
    ASSERT_EQ(&ranges1, &ranges2);
}

// ======================== Intersects ========================

TEST(RowRangeIndexTest, IntersectsExactMatch) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    ASSERT_TRUE(index.Intersects(10, 20));
}

TEST(RowRangeIndexTest, IntersectsPartialOverlapFromLeft) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    ASSERT_TRUE(index.Intersects(5, 15));
}

TEST(RowRangeIndexTest, IntersectsPartialOverlapFromRight) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    ASSERT_TRUE(index.Intersects(15, 25));
}

TEST(RowRangeIndexTest, IntersectsContainedRange) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    ASSERT_TRUE(index.Intersects(12, 18));
}

TEST(RowRangeIndexTest, IntersectsContainingRange) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    ASSERT_TRUE(index.Intersects(0, 30));
}

TEST(RowRangeIndexTest, IntersectsTouchingLeftBoundary) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    ASSERT_TRUE(index.Intersects(5, 10));
}

TEST(RowRangeIndexTest, IntersectsTouchingRightBoundary) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    ASSERT_TRUE(index.Intersects(20, 25));
}

TEST(RowRangeIndexTest, IntersectsNoOverlapBefore) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    ASSERT_FALSE(index.Intersects(0, 9));
}

TEST(RowRangeIndexTest, IntersectsNoOverlapAfter) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    ASSERT_FALSE(index.Intersects(21, 30));
}

TEST(RowRangeIndexTest, IntersectsMultipleRangesHitsFirst) {
    ASSERT_OK_AND_ASSIGN(auto index,
                         RowRangeIndex::Create({Range(0, 10), Range(20, 30), Range(40, 50)}));
    ASSERT_TRUE(index.Intersects(5, 8));
}

TEST(RowRangeIndexTest, IntersectsMultipleRangesHitsMiddle) {
    ASSERT_OK_AND_ASSIGN(auto index,
                         RowRangeIndex::Create({Range(0, 10), Range(20, 30), Range(40, 50)}));
    ASSERT_TRUE(index.Intersects(22, 28));
}

TEST(RowRangeIndexTest, IntersectsMultipleRangesHitsLast) {
    ASSERT_OK_AND_ASSIGN(auto index,
                         RowRangeIndex::Create({Range(0, 10), Range(20, 30), Range(40, 50)}));
    ASSERT_TRUE(index.Intersects(42, 48));
}

TEST(RowRangeIndexTest, IntersectsMultipleRangesSpansGap) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(0, 10), Range(20, 30)}));
    ASSERT_TRUE(index.Intersects(8, 22));
}

TEST(RowRangeIndexTest, IntersectsMultipleRangesFallsInGap) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(0, 10), Range(20, 30)}));
    ASSERT_FALSE(index.Intersects(11, 19));
}

TEST(RowRangeIndexTest, IntersectsMultipleRangesBeforeAll) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20), Range(30, 40)}));
    ASSERT_FALSE(index.Intersects(0, 5));
}

TEST(RowRangeIndexTest, IntersectsMultipleRangesAfterAll) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20), Range(30, 40)}));
    ASSERT_FALSE(index.Intersects(50, 60));
}

TEST(RowRangeIndexTest, IntersectsSinglePointMatch) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 10)}));
    ASSERT_TRUE(index.Intersects(10, 10));
}

TEST(RowRangeIndexTest, IntersectsSinglePointNoMatch) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 10)}));
    ASSERT_FALSE(index.Intersects(11, 11));
}

// ======================== IntersectedRanges ========================

TEST(RowRangeIndexTest, IntersectedRangesExactMatch) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    auto intersected = index.IntersectedRanges(10, 20);
    std::vector<Range> expected = {Range(10, 20)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesClippedFromLeft) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    auto intersected = index.IntersectedRanges(5, 15);
    std::vector<Range> expected = {Range(10, 15)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesClippedFromRight) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    auto intersected = index.IntersectedRanges(15, 25);
    std::vector<Range> expected = {Range(15, 20)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesClippedBothSides) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    auto intersected = index.IntersectedRanges(12, 18);
    std::vector<Range> expected = {Range(12, 18)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesContainingRange) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    auto intersected = index.IntersectedRanges(0, 30);
    std::vector<Range> expected = {Range(10, 20)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesNoOverlapBefore) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    auto intersected = index.IntersectedRanges(0, 9);
    ASSERT_TRUE(intersected.empty());
}

TEST(RowRangeIndexTest, IntersectedRangesNoOverlapAfter) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    auto intersected = index.IntersectedRanges(21, 30);
    ASSERT_TRUE(intersected.empty());
}

TEST(RowRangeIndexTest, IntersectedRangesTouchingLeftBoundary) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    auto intersected = index.IntersectedRanges(5, 10);
    std::vector<Range> expected = {Range(10, 10)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesTouchingRightBoundary) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    auto intersected = index.IntersectedRanges(20, 25);
    std::vector<Range> expected = {Range(20, 20)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesMultipleRangesHitsFirst) {
    ASSERT_OK_AND_ASSIGN(auto index,
                         RowRangeIndex::Create({Range(0, 10), Range(20, 30), Range(40, 50)}));
    auto intersected = index.IntersectedRanges(3, 8);
    std::vector<Range> expected = {Range(3, 8)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesMultipleRangesHitsLast) {
    ASSERT_OK_AND_ASSIGN(auto index,
                         RowRangeIndex::Create({Range(0, 10), Range(20, 30), Range(40, 50)}));
    auto intersected = index.IntersectedRanges(42, 48);
    std::vector<Range> expected = {Range(42, 48)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesSpansTwoRanges) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(0, 10), Range(20, 30)}));
    auto intersected = index.IntersectedRanges(5, 25);
    std::vector<Range> expected = {Range(5, 10), Range(20, 25)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesSpansThreeRangesMiddleFullyContained) {
    ASSERT_OK_AND_ASSIGN(auto index,
                         RowRangeIndex::Create({Range(0, 10), Range(20, 30), Range(40, 50)}));
    auto intersected = index.IntersectedRanges(5, 45);
    std::vector<Range> expected = {Range(5, 10), Range(20, 30), Range(40, 45)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesSpansAllRanges) {
    ASSERT_OK_AND_ASSIGN(auto index,
                         RowRangeIndex::Create({Range(0, 10), Range(20, 30), Range(40, 50)}));
    auto intersected = index.IntersectedRanges(0, 50);
    std::vector<Range> expected = {Range(0, 10), Range(20, 30), Range(40, 50)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesSpansAllRangesWider) {
    ASSERT_OK_AND_ASSIGN(auto index,
                         RowRangeIndex::Create({Range(10, 20), Range(30, 40), Range(50, 60)}));
    auto intersected = index.IntersectedRanges(0, 100);
    std::vector<Range> expected = {Range(10, 20), Range(30, 40), Range(50, 60)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesFallsInGap) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(0, 10), Range(20, 30)}));
    auto intersected = index.IntersectedRanges(11, 19);
    ASSERT_TRUE(intersected.empty());
}

TEST(RowRangeIndexTest, IntersectedRangesBeforeAll) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20), Range(30, 40)}));
    auto intersected = index.IntersectedRanges(0, 5);
    ASSERT_TRUE(intersected.empty());
}

TEST(RowRangeIndexTest, IntersectedRangesAfterAll) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20), Range(30, 40)}));
    auto intersected = index.IntersectedRanges(50, 60);
    ASSERT_TRUE(intersected.empty());
}

TEST(RowRangeIndexTest, IntersectedRangesSinglePointMatch) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 20)}));
    auto intersected = index.IntersectedRanges(15, 15);
    std::vector<Range> expected = {Range(15, 15)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesSinglePointRangeMatch) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 10)}));
    auto intersected = index.IntersectedRanges(10, 10);
    std::vector<Range> expected = {Range(10, 10)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesSinglePointRangeNoMatch) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(10, 10)}));
    auto intersected = index.IntersectedRanges(11, 11);
    ASSERT_TRUE(intersected.empty());
}

TEST(RowRangeIndexTest, IntersectedRangesLastRangeStartsBeyondEnd) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(0, 10), Range(30, 40)}));
    auto intersected = index.IntersectedRanges(5, 25);
    std::vector<Range> expected = {Range(5, 10)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesFourRangesClipBothEnds) {
    ASSERT_OK_AND_ASSIGN(auto index, RowRangeIndex::Create({Range(0, 10), Range(20, 30),
                                                            Range(40, 50), Range(60, 70)}));
    auto intersected = index.IntersectedRanges(5, 65);
    std::vector<Range> expected = {Range(5, 10), Range(20, 30), Range(40, 50), Range(60, 65)};
    ASSERT_EQ(intersected, expected);
}

TEST(RowRangeIndexTest, IntersectedRangesFiveRangesThreeMiddleFullyContained) {
    ASSERT_OK_AND_ASSIGN(auto index,
                         RowRangeIndex::Create({Range(0, 10), Range(20, 30), Range(40, 50),
                                                Range(60, 70), Range(80, 90)}));
    auto intersected = index.IntersectedRanges(5, 85);
    std::vector<Range> expected = {Range(5, 10), Range(20, 30), Range(40, 50), Range(60, 70),
                                   Range(80, 85)};
    ASSERT_EQ(intersected, expected);
}

}  // namespace paimon::test
