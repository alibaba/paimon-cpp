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

#include "paimon/core/utils/blob_view_lookup.h"

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/data/blob_view_struct.h"
#include "paimon/utils/range.h"

namespace paimon::test {

class BlobViewLookupTest : public testing::Test {
 public:
    static Identifier MakeIdentifier(const std::string& database, const std::string& table) {
        return Identifier(database, table);
    }

    static BlobViewStruct MakeView(const std::string& database, const std::string& table,
                                   int32_t field_id, int64_t row_id) {
        return BlobViewStruct(MakeIdentifier(database, table), field_id, row_id);
    }
};

TEST_F(BlobViewLookupTest, TestConstruct) {
    BlobViewStruct view = MakeView("db", "t", /*field_id=*/7, /*row_id=*/42);
    BlobViewLookup::TableReadPlan plan(view);

    ASSERT_EQ(plan.identifier_, MakeIdentifier("db", "t"));
    const auto& references = plan.references_by_field_id_;
    ASSERT_EQ(references.size(), 1U);
    auto iter = references.find(7);
    ASSERT_NE(iter, references.end());

    const auto& row_ranges = plan.row_ranges_;
    ASSERT_EQ(row_ranges.size(), 1U);
    ASSERT_EQ(row_ranges[0], 42);

    ASSERT_EQ(plan.GetFieldIds(), std::vector<int32_t>{7});
}

TEST_F(BlobViewLookupTest, TestAdd) {
    BlobViewLookup::TableReadPlan plan(MakeView("db", "t", /*field_id=*/7, /*row_id=*/10));

    plan.Add(MakeView("db", "t", /*field_id=*/7, /*row_id=*/12));
    plan.Add(MakeView("db", "t", /*field_id=*/9, /*row_id=*/11));
    plan.Add(MakeView("db", "t", /*field_id=*/9, /*row_id=*/13));

    const auto& references = plan.references_by_field_id_;
    ASSERT_EQ(references.size(), 2U);
    auto iter = references.find(7);
    ASSERT_NE(iter, references.end());
    iter = references.find(9);
    ASSERT_NE(iter, references.end());

    ASSERT_EQ(plan.row_ranges_, (std::vector<int64_t>{10, 12, 11, 13}));
}

TEST_F(BlobViewLookupTest, TestGetFieldIds) {
    BlobViewLookup::TableReadPlan plan(MakeView("db", "t", /*field_id=*/3, /*row_id=*/0));
    plan.Add(MakeView("db", "t", /*field_id=*/1, /*row_id=*/1));
    plan.Add(MakeView("db", "t", /*field_id=*/3, /*row_id=*/2));
    plan.Add(MakeView("db", "t", /*field_id=*/2, /*row_id=*/3));

    ASSERT_EQ(plan.GetFieldIds(), (std::vector<int32_t>{1, 2, 3}));
}

TEST_F(BlobViewLookupTest, TestGetSortedDistinctRangesMergesTwoAdjacentRowIds) {
    BlobViewLookup::TableReadPlan plan(MakeView("db", "t", /*field_id=*/1, /*row_id=*/5));
    plan.Add(MakeView("db", "t", /*field_id=*/1, /*row_id=*/6));

    auto ranges = plan.GetSortedDistinctRanges();
    ASSERT_EQ(ranges.size(), 1U);
    ASSERT_EQ(ranges[0], Range(5, 6));
}

TEST_F(BlobViewLookupTest, TestGetSortedDistinctRangesMergesContiguousAndGaps) {
    BlobViewLookup::TableReadPlan plan(MakeView("db", "t", /*field_id=*/1, /*row_id=*/5));
    // Out of order, with duplicates and a gap so we get two output ranges.
    plan.Add(MakeView("db", "t", /*field_id=*/1, /*row_id=*/6));
    plan.Add(MakeView("db", "t", /*field_id=*/1, /*row_id=*/5));
    plan.Add(MakeView("db", "t", /*field_id=*/1, /*row_id=*/7));
    plan.Add(MakeView("db", "t", /*field_id=*/1, /*row_id=*/10));
    plan.Add(MakeView("db", "t", /*field_id=*/1, /*row_id=*/11));

    auto ranges = plan.GetSortedDistinctRanges();
    ASSERT_EQ(ranges.size(), 2U);
    ASSERT_EQ(ranges[0], Range(5, 7));
    ASSERT_EQ(ranges[1], Range(10, 11));
}

TEST_F(BlobViewLookupTest, TestGetSortedDistinctRangesWithNonContiguous) {
    BlobViewLookup::TableReadPlan plan(MakeView("db", "t", /*field_id=*/1, /*row_id=*/1));
    plan.Add(MakeView("db", "t", /*field_id=*/1, /*row_id=*/100));
    plan.Add(MakeView("db", "t", /*field_id=*/1, /*row_id=*/50));

    const auto ranges = plan.GetSortedDistinctRanges();
    ASSERT_EQ(ranges.size(), 3U);
    ASSERT_EQ(ranges[0], Range(1, 1));
    ASSERT_EQ(ranges[1], Range(50, 50));
    ASSERT_EQ(ranges[2], Range(100, 100));
}

TEST_F(BlobViewLookupTest, TestEmptyInputProducesEmptyOutput) {
    auto grouped = BlobViewLookup::GroupByIdentifier({});
    ASSERT_TRUE(grouped.empty());
}

TEST_F(BlobViewLookupTest, TestSingleViewStructProducesSingleGroup) {
    std::unordered_set<BlobViewStruct> views;
    views.emplace(MakeView("db", "t", /*field_id=*/3, /*row_id=*/42));

    auto grouped = BlobViewLookup::GroupByIdentifier(views);
    ASSERT_EQ(grouped.size(), 1U);

    auto iter = grouped.find(MakeIdentifier("db", "t"));
    ASSERT_NE(iter, grouped.end());
    const auto& plan = iter->second;
    ASSERT_EQ(plan.GetFieldIds(), std::vector<int32_t>{3});
    ASSERT_EQ(plan.row_ranges_, std::vector<int64_t>{42});
}

TEST_F(BlobViewLookupTest, TestMultipleViewStructsOfSameTableAreMergedIntoOnePlan) {
    std::unordered_set<BlobViewStruct> views;
    views.emplace(MakeView("db", "t", /*field_id=*/3, /*row_id=*/1));
    views.emplace(MakeView("db", "t", /*field_id=*/3, /*row_id=*/2));
    views.emplace(MakeView("db", "t", /*field_id=*/4, /*row_id=*/1));

    auto grouped = BlobViewLookup::GroupByIdentifier(views);
    ASSERT_EQ(grouped.size(), 1U);

    const auto& plan = grouped.at(MakeIdentifier("db", "t"));
    ASSERT_EQ(plan.GetFieldIds(), (std::vector<int32_t>{3, 4}));

    const auto& references = plan.references_by_field_id_;
    ASSERT_EQ(references.size(), 2U);
    auto iter = references.find(3);
    ASSERT_NE(iter, references.end());
    iter = references.find(4);
    ASSERT_NE(iter, references.end());

    ASSERT_EQ(plan.row_ranges_.size(), 3U);
}

TEST_F(BlobViewLookupTest, TestViewStructsOfDifferentTablesAreSplitIntoDistinctPlans) {
    std::unordered_set<BlobViewStruct> views;
    views.emplace(MakeView("db1", "t1", /*field_id=*/3, /*row_id=*/1));
    views.emplace(MakeView("db1", "t2", /*field_id=*/3, /*row_id=*/1));
    views.emplace(MakeView("db2", "t1", /*field_id=*/3, /*row_id=*/1));
    views.emplace(MakeView("db1", "t1", /*field_id=*/4, /*row_id=*/2));

    auto grouped = BlobViewLookup::GroupByIdentifier(views);
    ASSERT_EQ(grouped.size(), 3U);

    const auto& plan_db1_t1 = grouped.at(MakeIdentifier("db1", "t1"));
    ASSERT_EQ(plan_db1_t1.GetFieldIds(), (std::vector<int32_t>{3, 4}));
    ASSERT_EQ(plan_db1_t1.row_ranges_.size(), 2U);

    const auto& plan_db1_t2 = grouped.at(MakeIdentifier("db1", "t2"));
    ASSERT_EQ(plan_db1_t2.GetFieldIds(), std::vector<int32_t>{3});
    ASSERT_EQ(plan_db1_t2.row_ranges_.size(), 1U);

    const auto& plan_db2_t1 = grouped.at(MakeIdentifier("db2", "t1"));
    ASSERT_EQ(plan_db2_t1.GetFieldIds(), std::vector<int32_t>{3});
    ASSERT_EQ(plan_db2_t1.row_ranges_.size(), 1U);
}

TEST_F(BlobViewLookupTest, TestGetIdentifier) {
    BlobViewLookup::TableReadPlan plan(MakeView("db", "t", /*field_id=*/1, /*row_id=*/0));
    ASSERT_EQ(plan.GetIdentifier(), MakeIdentifier("db", "t"));
}

TEST_F(BlobViewLookupTest, TestTargetRowsPerTaskEmptyReturnsMin) {
    std::unordered_map<Identifier, BlobViewLookup::TableReadPlan> empty;
    ASSERT_EQ(BlobViewLookup::TargetRowsPerTask(empty, /*thread_num=*/100),
              BlobViewLookup::MIN_ROW_PER_TASK);
}

TEST_F(BlobViewLookupTest, TestTargetRowsPerTaskSmallTotalReturnsMin) {
    std::unordered_set<BlobViewStruct> views;
    for (int64_t row_id = 0; row_id < 10; ++row_id) {
        views.emplace(MakeView("db", "t", /*field_id=*/1, row_id));
    }
    auto grouped = BlobViewLookup::GroupByIdentifier(views);
    // total_rows (10) is far below thread_num, so the balanced budget is clamped to
    // MIN_ROW_PER_TASK.
    ASSERT_EQ(BlobViewLookup::TargetRowsPerTask(grouped, /*thread_num=*/100),
              BlobViewLookup::MIN_ROW_PER_TASK);
}

TEST_F(BlobViewLookupTest, TestTargetRowsPerTaskLargeTotalBalancesAcrossThreads) {
    std::unordered_set<BlobViewStruct> views;
    const int64_t total_rows = 100001;
    for (int64_t row_id = 0; row_id < total_rows; ++row_id) {
        views.emplace(MakeView("db", "t", /*field_id=*/1, row_id));
    }
    auto grouped = BlobViewLookup::GroupByIdentifier(views);
    // ceil(100001 / 100) = 1001
    ASSERT_EQ(BlobViewLookup::TargetRowsPerTask(grouped, /*thread_num=*/100), 1001);
}

TEST_F(BlobViewLookupTest, TestSplitRowRangesEmptyInput) {
    auto chunks = BlobViewLookup::SplitRowRanges({}, /*target_rows_per_task=*/10);
    ASSERT_TRUE(chunks.empty());
}

TEST_F(BlobViewLookupTest, TestSplitRowRangesSingleRangeFitsInOneChunk) {
    auto chunks = BlobViewLookup::SplitRowRanges({Range(0, 4)}, /*target_rows_per_task=*/10);
    ASSERT_EQ(chunks.size(), 1U);
    ASSERT_EQ(chunks[0].size(), 1U);
    ASSERT_EQ(chunks[0][0], Range(0, 4));
}

TEST_F(BlobViewLookupTest, TestSplitRowRangesSplitsLargeRange) {
    // [0, 9] with target 4 => [0,3], [4,7], [8,9]
    auto chunks = BlobViewLookup::SplitRowRanges({Range(0, 9)}, /*target_rows_per_task=*/4);
    ASSERT_EQ(chunks.size(), 3U);
    ASSERT_EQ(chunks[0], (std::vector<Range>{Range(0, 3)}));
    ASSERT_EQ(chunks[1], (std::vector<Range>{Range(4, 7)}));
    ASSERT_EQ(chunks[2], (std::vector<Range>{Range(8, 9)}));
}

TEST_F(BlobViewLookupTest, TestSplitRowRangesPacksAcrossRanges) {
    // Ranges [0,2] (3 rows) and [10,12] (3 rows) with target 4.
    // chunk0 = [0,2] + part of second range [10,10] (total 4 rows)
    // chunk1 = [11,12] (2 rows)
    auto chunks =
        BlobViewLookup::SplitRowRanges({Range(0, 2), Range(10, 12)}, /*target_rows_per_task=*/4);
    ASSERT_EQ(chunks.size(), 2U);
    ASSERT_EQ(chunks[0], (std::vector<Range>{Range(0, 2), Range(10, 10)}));
    ASSERT_EQ(chunks[1], (std::vector<Range>{Range(11, 12)}));
}

}  // namespace paimon::test
