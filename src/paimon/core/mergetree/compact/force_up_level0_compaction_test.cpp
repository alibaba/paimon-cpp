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

#include "paimon/core/mergetree/compact/force_up_level0_compaction.h"

#include "paimon/core/mergetree/compact/universal_compaction.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class ForceUpLevel0CompactionTest : public testing::Test {
 public:
    LevelSortedRun CreateLevelSortedRun(int32_t level, int64_t size) const {
        auto file_meta = std::make_shared<DataFileMeta>(
            "fake.data", /*file_size=*/size, /*row_count=*/1,
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

TEST_F(ForceUpLevel0CompactionTest, TestForceCompaction0) {
    auto universal =
        std::make_shared<UniversalCompaction>(/*max_size_amp=*/200, /*size_ratio=*/1,
                                              /*num_run_compaction_trigger=*/5, nullptr, nullptr);
    ForceUpLevel0Compaction compaction(universal, /*max_compact_interval=*/std::nullopt);

    ASSERT_OK_AND_ASSIGN(
        auto unit, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize({0, 0}, {1, 1})));
    ASSERT_TRUE(unit);
    ASSERT_EQ(unit.value().output_level, 2);

    ASSERT_OK_AND_ASSIGN(
        unit, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize({0, 1}, {1, 10})));
    ASSERT_TRUE(unit);
    ASSERT_EQ(unit.value().output_level, 2);

    ASSERT_OK_AND_ASSIGN(
        unit, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize({0, 0, 2}, {1, 5, 10})));
    ASSERT_TRUE(unit);
    ASSERT_EQ(unit.value().output_level, 1);

    ASSERT_OK_AND_ASSIGN(unit,
                         compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize({2}, {10})));
    ASSERT_FALSE(unit);

    ASSERT_OK_AND_ASSIGN(unit,
                         compaction.Pick(/*num_levels=*/3,
                                         CreateRunsWithLevelAndSize({0, 0, 0, 0}, {1, 5, 10, 20})));
    ASSERT_TRUE(unit);
    ASSERT_EQ(unit.value().output_level, 2);
}

TEST_F(ForceUpLevel0CompactionTest, TestMaxCompactIntervalConfiguration) {
    auto universal =
        std::make_shared<UniversalCompaction>(/*max_size_amp=*/200, /*size_ratio=*/1,
                                              /*num_run_compaction_trigger=*/5, nullptr, nullptr);

    ForceUpLevel0Compaction radical(universal, /*max_compact_interval=*/std::nullopt);
    ASSERT_EQ(radical.MaxCompactInterval(), std::nullopt);

    ForceUpLevel0Compaction gentle(universal, /*max_compact_interval=*/10);
    ASSERT_EQ(gentle.MaxCompactInterval(), 10);
}

TEST_F(ForceUpLevel0CompactionTest, TestGentleIntervalShouldForcePickAfterThreshold) {
    auto universal =
        std::make_shared<UniversalCompaction>(/*max_size_amp=*/200, /*size_ratio=*/1,
                                              /*num_run_compaction_trigger=*/5, nullptr, nullptr);
    ForceUpLevel0Compaction compaction(universal, /*max_compact_interval=*/2);
    auto runs = CreateRunsWithLevelAndSize({0, 0}, {1, 1});

    // First trigger only increases the counter in gentle mode.
    ASSERT_OK_AND_ASSIGN(auto unit, compaction.Pick(/*num_levels=*/3, runs));
    ASSERT_FALSE(unit);

    // Second trigger reaches the threshold and forces a level-0 pick.
    ASSERT_OK_AND_ASSIGN(unit, compaction.Pick(/*num_levels=*/3, runs));
    ASSERT_TRUE(unit);
    ASSERT_EQ(unit.value().output_level, 2);
}
}  // namespace paimon::test
