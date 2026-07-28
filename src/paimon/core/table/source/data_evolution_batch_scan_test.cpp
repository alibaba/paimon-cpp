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

#include "paimon/core/table/source/data_evolution_batch_scan.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/plan_impl.h"
#include "paimon/data/timestamp.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/range.h"
#include "paimon/utils/row_range_index.h"

namespace paimon::test {
namespace {
std::shared_ptr<DataFileMeta> NewAppendFile(const std::string& file_name, int64_t first_row_id,
                                            int64_t row_count) {
    return std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/1024l, row_count, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
        SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), /*min_sequence_number=*/0l,
        /*max_sequence_number=*/first_row_id + row_count - 1, /*schema_id=*/0, /*level=*/0,
        std::vector<std::optional<std::string>>(), Timestamp(0l, 0), /*delete_row_count=*/0,
        /*embedded_index=*/nullptr, FileSource::Append(), std::nullopt, std::nullopt, first_row_id,
        std::nullopt);
}

std::shared_ptr<Plan> NewDataPlan(std::vector<std::shared_ptr<DataFileMeta>> files) {
    DataSplitImpl::Builder builder(
        /*partition=*/BinaryRow::EmptyRow(),
        /*bucket=*/0, /*bucket_path=*/"data/test_table/bucket-0", std::move(files));
    std::shared_ptr<Split> data_split =
        builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build().value();
    return std::make_shared<PlanImpl>(/*snapshot_id=*/1,
                                      std::vector<std::shared_ptr<Split>>({data_split}));
}
}  // namespace

TEST(DataEvolutionBatchScanTest, TestWrapToIndexedSplitsWithUnorderedAndDiscontiguousDataFiles) {
    // The files cover [4650, 4700], [4300, 4450] and [4200, 4407]: unordered, partially
    // overlapping, with a gap [4451, 4649] that must not appear in the wrapped ranges.
    std::shared_ptr<Plan> data_plan =
        NewDataPlan({NewAppendFile("file-1", 4650l, 51l), NewAppendFile("file-2", 4300l, 151l),
                     NewAppendFile("file-3", 4200l, 208l)});
    ASSERT_OK_AND_ASSIGN(RowRangeIndex row_range_index, RowRangeIndex::Create({Range(0, 5000)}));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         DataEvolutionBatchScan::WrapToIndexedSplits(data_plan, row_range_index,
                                                                     /*id_to_score=*/{}));

    ASSERT_EQ(plan->Splits().size(), 1);
    auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(plan->Splits()[0]);
    ASSERT_NE(indexed_split, nullptr);
    ASSERT_EQ(indexed_split->GetDataSplit(), data_plan->Splits()[0]);
    ASSERT_EQ(indexed_split->RowRanges(),
              std::vector<Range>({Range(4200, 4450), Range(4650, 4700)}));
    ASSERT_TRUE(indexed_split->Scores().empty());
}

TEST(DataEvolutionBatchScanTest, TestWrapToIndexedSplitsExcludesRowIdsInFileRangeGaps) {
    // The split covers [0, 9] and [20, 29] while the index hits {5, 15, 25}. Row id 15 lies in
    // the gap between the two files and must be excluded, together with its score.
    std::shared_ptr<Plan> data_plan =
        NewDataPlan({NewAppendFile("file-1", 0l, 10l), NewAppendFile("file-2", 20l, 10l)});
    ASSERT_OK_AND_ASSIGN(RowRangeIndex row_range_index,
                         RowRangeIndex::Create({Range(5, 5), Range(15, 15), Range(25, 25)}));
    std::map<int64_t, float> id_to_score = {{5l, 0.5f}, {15l, 0.7f}, {25l, 0.9f}};

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, DataEvolutionBatchScan::WrapToIndexedSplits(
                                                         data_plan, row_range_index, id_to_score));

    ASSERT_EQ(plan->Splits().size(), 1);
    auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(plan->Splits()[0]);
    ASSERT_NE(indexed_split, nullptr);
    ASSERT_EQ(indexed_split->RowRanges(), std::vector<Range>({Range(5, 5), Range(25, 25)}));
    ASSERT_EQ(indexed_split->Scores(), std::vector<float>({0.5f, 0.9f}));
}

TEST(DataEvolutionBatchScanTest, TestWrapToIndexedSplitsWithoutIntersection) {
    std::shared_ptr<Plan> data_plan = NewDataPlan({NewAppendFile("file-1", 100l, 10l)});
    ASSERT_OK_AND_ASSIGN(RowRangeIndex row_range_index, RowRangeIndex::Create({Range(0, 50)}));

    ASSERT_NOK_WITH_MSG(DataEvolutionBatchScan::WrapToIndexedSplits(data_plan, row_range_index,
                                                                    /*id_to_score=*/{}),
                        "There should be intersected ranges for split with min row id 100 and "
                        "max row id 109.");
}

}  // namespace paimon::test
