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

#include "paimon/core/mergetree/compact/compact_strategy.h"

#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class CompactStrategyTest : public testing::Test {
 public:
    LevelSortedRun CreateLevelSortedRun(int32_t level, int64_t total_size) const {
        auto file_meta = std::make_shared<DataFileMeta>(
            "fake.data", /*file_size=*/total_size, /*row_count=*/1,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/
            BinaryRow::EmptyRow(),
            /*key_stats=*/
            SimpleStats::EmptyStats(),
            /*value_stats=*/
            SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/6, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        return {level, SortedRun::FromSingle(file_meta)};
    }

    std::vector<LevelSortedRun> CreateRunsWithLevelAndSize(
        const std::vector<int32_t>& levels, const std::vector<int64_t>& sizes) const {
        EXPECT_EQ(levels.size(), sizes.size());
        std::vector<LevelSortedRun> runs;
        for (size_t i = 0; i < levels.size(); i++) {
            runs.push_back(CreateLevelSortedRun(levels[i], sizes[i]));
        }
        return runs;
    }
};

TEST_F(CompactStrategyTest, TestPickFullCompaction) {
    {
        // no sorted run, no need to compact
        auto runs = CreateRunsWithLevelAndSize({}, {});
        auto unit =
            CompactStrategy::PickFullCompaction(/*num_levels=*/3, runs, /*dv_maintainer=*/nullptr,
                                                /*force_rewrite_all_files=*/false);
        ASSERT_FALSE(unit);
    }
    {
        // only max level files, not rewrite
        auto runs = CreateRunsWithLevelAndSize(/*levels=*/{3}, /*sizes*/ {10});
        auto unit =
            CompactStrategy::PickFullCompaction(/*num_levels=*/4, runs, /*dv_maintainer=*/nullptr,
                                                /*force_rewrite_all_files=*/false);
        ASSERT_FALSE(unit);
    }
    {
        // only max level files, force rewrite
        auto runs = CreateRunsWithLevelAndSize(/*levels=*/{3}, /*sizes*/ {10});
        auto unit =
            CompactStrategy::PickFullCompaction(/*num_levels=*/4, runs, /*dv_maintainer=*/nullptr,
                                                /*force_rewrite_all_files=*/true);
        ASSERT_TRUE(unit);
        ASSERT_EQ(unit.value().output_level, 3);
        ASSERT_EQ(unit.value().files.size(), 1);
        ASSERT_TRUE(unit.value().file_rewrite);
    }
    {
        // full compaction
        auto runs = CreateRunsWithLevelAndSize(/*levels=*/{0, 3}, /*sizes*/ {1, 10});
        auto unit =
            CompactStrategy::PickFullCompaction(/*num_levels=*/4, runs, /*dv_maintainer=*/nullptr,
                                                /*force_rewrite_all_files=*/false);
        ASSERT_TRUE(unit);
        ASSERT_EQ(unit.value().output_level, 3);
        ASSERT_EQ(unit.value().files.size(), 2);
        ASSERT_FALSE(unit.value().file_rewrite);
    }
    {
        // test with dv maintainer
        std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors = {
            {"fake.data", std::make_shared<BitmapDeletionVector>(RoaringBitmap32())}};

        auto dv_maintainer = std::make_shared<BucketedDvMaintainer>(
            std::make_shared<DeletionVectorsIndexFile>(nullptr, nullptr, /*bitmap64=*/false,
                                                       GetDefaultPool()),
            deletion_vectors);
        auto runs = CreateRunsWithLevelAndSize(/*levels=*/{3}, /*sizes*/ {10});
        auto unit = CompactStrategy::PickFullCompaction(/*num_levels=*/4, runs, dv_maintainer,
                                                        /*force_rewrite_all_files=*/false);
        ASSERT_TRUE(unit);
        ASSERT_EQ(unit.value().output_level, 3);
        ASSERT_EQ(unit.value().files.size(), 1);
        ASSERT_TRUE(unit.value().file_rewrite);
    }
}
}  // namespace paimon::test
