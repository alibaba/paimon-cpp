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

#include "paimon/core/mergetree/compact/early_full_compaction.h"

#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class EarlyFullCompactionTest : public testing::Test {
 public:
    class TestableEarlyFullCompaction : public EarlyFullCompaction {
        TestableEarlyFullCompaction(const std::optional<int64_t>& full_compaction_interval,
                                    const std::optional<int64_t>& total_size_threshold,
                                    const std::optional<int64_t>& incremental_size_threshold,
                                    const int64_t* current_time)
            : EarlyFullCompaction(full_compaction_interval, total_size_threshold,
                                  incremental_size_threshold),
              current_time_(current_time) {}

        int64_t CurrentTimeMillis() const override {
            return *current_time_;
        }

     private:
        const int64_t* current_time_;
    };

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

    std::vector<LevelSortedRun> CreateRuns(const std::vector<int64_t>& sizes) const {
        std::vector<LevelSortedRun> runs;
        for (const auto& total_size : sizes) {
            runs.push_back(CreateLevelSortedRun(/*level=*/0, total_size));
        }
        return runs;
    }
};

TEST_F(EarlyFullCompactionTest, TestCreateNoOptions) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_FALSE(EarlyFullCompaction::Create(core_options));
}

TEST_F(EarlyFullCompactionTest, TestCreateWithInterval) {
    std::map<std::string, std::string> options = {
        {Options::COMPACTION_OPTIMIZATION_INTERVAL, "1h"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_TRUE(EarlyFullCompaction::Create(core_options));
}

TEST_F(EarlyFullCompactionTest, TestCreateWithThreshold) {
    std::map<std::string, std::string> options = {
        {Options::COMPACTION_TOTAL_SIZE_THRESHOLD, "100MB"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_TRUE(EarlyFullCompaction::Create(core_options));
}

TEST_F(EarlyFullCompactionTest, TestCreateWithBoth) {
    std::map<std::string, std::string> options = {
        {Options::COMPACTION_OPTIMIZATION_INTERVAL, "1h"},
        {Options::COMPACTION_TOTAL_SIZE_THRESHOLD, "100MB"},
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    ASSERT_TRUE(EarlyFullCompaction::Create(core_options));
}

TEST_F(EarlyFullCompactionTest, TestInterval) {
    int64_t current_time = 10000l;
    auto runs = CreateRuns({100l, 200l});
    TestableEarlyFullCompaction early_full_compaction(/*full_compaction_interval=*/1000l,
                                                      /*total_size_threshold=*/std::nullopt,
                                                      /*incremental_size_threshold=*/std::nullopt,
                                                      &current_time);
    // First time, should trigger
    auto compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_TRUE(compact_unit);
    ASSERT_EQ(compact_unit->output_level, 4);
    ASSERT_EQ(compact_unit->files.size(), 2);

    // Last compaction time is now 10000.
    // Advance time, but not enough for interval to trigger.
    current_time += 500;
    ASSERT_FALSE(early_full_compaction.TryFullCompact(/*num_levels=*/5, runs));

    // Advance time to be greater than interval.
    current_time += 501;
    compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_TRUE(compact_unit);
    ASSERT_EQ(compact_unit->output_level, 4);
    ASSERT_EQ(compact_unit->files.size(), 2);
}

TEST_F(EarlyFullCompactionTest, TestTotalSizeThreshold) {
    EarlyFullCompaction early_full_compaction(/*full_compaction_interval=*/std::nullopt,
                                              /*total_size_threshold=*/1000l,
                                              /*incremental_size_threshold=*/std::nullopt);

    // total size 300 < 1000, should trigger
    auto runs = CreateRuns({100l, 200l});
    auto compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_TRUE(compact_unit);
    ASSERT_EQ(compact_unit->output_level, 4);
    ASSERT_EQ(compact_unit->files.size(), 2);
    // total size 1000 == 1000, should not trigger
    runs = CreateRuns({500l, 500l});
    ASSERT_FALSE(early_full_compaction.TryFullCompact(/*num_levels=*/5, runs));
    // total size 1500 > 1000, should not trigger
    runs = CreateRuns({500l, 1000l});
    ASSERT_FALSE(early_full_compaction.TryFullCompact(/*num_levels=*/5, runs));
}

TEST_F(EarlyFullCompactionTest, TestIncrementalSizeThreshold) {
    EarlyFullCompaction early_full_compaction(/*full_compaction_interval=*/std::nullopt,
                                              /*total_size_threshold=*/std::nullopt,
                                              /*incremental_size_threshold=*/500l);

    // trigger, no max level
    auto runs = CreateRuns({400l, 200l});
    auto compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_TRUE(compact_unit);
    ASSERT_EQ(compact_unit->output_level, 4);
    ASSERT_EQ(compact_unit->files.size(), 2);
    // no trigger, no max level
    runs = CreateRuns({100l, 200l});
    ASSERT_FALSE(early_full_compaction.TryFullCompact(/*num_levels=*/5, runs));
    // no trigger, with max level
    runs = {CreateLevelSortedRun(0, 100), CreateLevelSortedRun(0, 300),
            CreateLevelSortedRun(4, 500)};
    ASSERT_FALSE(early_full_compaction.TryFullCompact(/*num_levels=*/5, runs));
    // trigger, with max level
    runs = {CreateLevelSortedRun(0, 100), CreateLevelSortedRun(0, 300),
            CreateLevelSortedRun(0, 300), CreateLevelSortedRun(4, 500)};
    compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_TRUE(compact_unit);
    ASSERT_EQ(compact_unit->output_level, 4);
    ASSERT_EQ(compact_unit->files.size(), 4);
}

TEST_F(EarlyFullCompactionTest, TestIntervalTriggersFirst) {
    int64_t current_time = 10000l;

    // Interval will trigger, but size is > threshold
    TestableEarlyFullCompaction early_full_compaction(/*full_compaction_interval=*/1000l,
                                                      /*total_size_threshold=*/500l,
                                                      /*incremental_size_threshold=*/std::nullopt,
                                                      &current_time);
    // First time, interval should trigger even if size (600) > threshold (500)
    auto runs = CreateRuns({300l, 300l});
    auto compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_TRUE(compact_unit);
    ASSERT_EQ(compact_unit->output_level, 4);
}

TEST_F(EarlyFullCompactionTest, TestThresholdTriggersWhenIntervalFails) {
    int64_t current_time = 10000l;
    TestableEarlyFullCompaction early_full_compaction(/*full_compaction_interval=*/1000l,
                                                      /*total_size_threshold=*/500l,
                                                      /*incremental_size_threshold=*/std::nullopt,
                                                      &current_time);
    // Trigger once to set last compaction time
    auto runs = CreateRuns({10l, 20l});
    auto compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_TRUE(compact_unit);
    // Advance time, but not enough for interval to trigger
    current_time += 500;

    // Size (60) < threshold (500), should trigger
    runs = CreateRuns({30l, 30l});
    compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_TRUE(compact_unit);
    ASSERT_EQ(compact_unit->output_level, 4);

    // Size (600) > threshold (500), should not trigger
    runs = CreateRuns({300l, 300l});
    ASSERT_FALSE(early_full_compaction.TryFullCompact(/*num_levels=*/5, runs));
}

TEST_F(EarlyFullCompactionTest, TestUpdateLastWhenFullCompactIsTriggeredByTotalSize) {
    int64_t current_time = 10000l;

    TestableEarlyFullCompaction early_full_compaction(/*full_compaction_interval=*/1000l,
                                                      /*total_size_threshold=*/500l,
                                                      /*incremental_size_threshold=*/std::nullopt,
                                                      &current_time);
    // First time, interval should trigger even if size (600) > threshold (500)
    auto runs = CreateRuns({300l, 300l});
    auto compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_TRUE(compact_unit);
    ASSERT_EQ(compact_unit->output_level, 4);

    current_time = 10100l;
    // Second time, compaction triggered by total_size_threshold
    runs = CreateRuns({300l, 100l});
    compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_TRUE(compact_unit);
    ASSERT_EQ(compact_unit->output_level, 4);

    current_time = 11001l;
    // Third time, compaction cannot be triggered as 11001 - 10100 < 1000 full_compaction_interval
    runs = CreateRuns({300l, 300l});
    compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_FALSE(compact_unit);
}

TEST_F(EarlyFullCompactionTest, TestUpdateLastWhenFullCompactIsTriggeredByIncSize) {
    int64_t current_time = 10000l;

    TestableEarlyFullCompaction early_full_compaction(/*full_compaction_interval=*/1000l,
                                                      /*total_size_threshold=*/std::nullopt,
                                                      /*incremental_size_threshold=*/500,
                                                      &current_time);
    // First time, interval should trigger even if size (400) < threshold (500)
    std::vector<LevelSortedRun> runs = {CreateLevelSortedRun(0, 300), CreateLevelSortedRun(0, 100)};
    auto compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_TRUE(compact_unit);
    ASSERT_EQ(compact_unit->output_level, 4);

    current_time = 10100l;
    // Second time, compaction triggered by total_size_threshold
    runs = {CreateLevelSortedRun(0, 300), CreateLevelSortedRun(0, 300)};
    compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_TRUE(compact_unit);
    ASSERT_EQ(compact_unit->output_level, 4);

    current_time = 11001l;
    // Third time, compaction cannot be triggered as 11001 - 10100 < 1000 full_compaction_interval
    runs = {CreateLevelSortedRun(0, 300), CreateLevelSortedRun(0, 100)};
    compact_unit = early_full_compaction.TryFullCompact(/*num_levels=*/5, runs);
    ASSERT_FALSE(compact_unit);
}

}  // namespace paimon::test
