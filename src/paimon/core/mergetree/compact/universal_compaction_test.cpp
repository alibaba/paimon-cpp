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

#include "paimon/core/mergetree/compact/universal_compaction.h"

#include "paimon/core/mergetree/compact/force_up_level0_compaction.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class UniversalCompactionTest : public testing::Test {
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

    std::vector<LevelSortedRun> CreateRunsWithLevel(const std::vector<int32_t>& levels) const {
        std::vector<LevelSortedRun> runs;
        for (const auto& level : levels) {
            runs.push_back(CreateLevelSortedRun(level, /*size=*/1));
        }
        return runs;
    }

    std::vector<LevelSortedRun> CreateRunsWithSize(const std::vector<int64_t>& sizes) const {
        std::vector<LevelSortedRun> runs;
        for (const auto& size : sizes) {
            runs.push_back(CreateLevelSortedRun(/*level=*/0, size));
        }
        return runs;
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

    std::vector<int64_t> GetFileSizeVecFromCompactUnit(const CompactUnit& unit) const {
        std::vector<int64_t> sizes;
        for (const auto& file : unit.files) {
            sizes.push_back(file->file_size);
        }
        return sizes;
    }
};

TEST_F(UniversalCompactionTest, TestOutputLevel) {
    UniversalCompaction compaction(/*max_size_amp=*/25, /*size_ratio=*/1,
                                   /*num_run_compaction_trigger=*/3, nullptr, nullptr);
    ASSERT_EQ(
        1, compaction
               .CreateUnit(CreateRunsWithLevel({0, 0, 1, 3, 4}), /*max_level=*/5, /*run_count=*/1)
               .output_level);
    ASSERT_EQ(
        1, compaction
               .CreateUnit(CreateRunsWithLevel({0, 0, 1, 3, 4}), /*max_level=*/5, /*run_count=*/2)
               .output_level);
    ASSERT_EQ(
        2, compaction
               .CreateUnit(CreateRunsWithLevel({0, 0, 1, 3, 4}), /*max_level=*/5, /*run_count=*/3)
               .output_level);
    ASSERT_EQ(
        3, compaction
               .CreateUnit(CreateRunsWithLevel({0, 0, 1, 3, 4}), /*max_level=*/5, /*run_count=*/4)
               .output_level);
    ASSERT_EQ(
        5, compaction
               .CreateUnit(CreateRunsWithLevel({0, 0, 1, 3, 4}), /*max_level=*/5, /*run_count=*/5)
               .output_level);
}

TEST_F(UniversalCompactionTest, TestPick) {
    UniversalCompaction compaction(/*max_size_amp=*/25, /*size_ratio=*/1,
                                   /*num_run_compaction_trigger=*/3, nullptr, nullptr);
    // by size amplification
    ASSERT_OK_AND_ASSIGN(auto pick,
                         compaction.Pick(/*num_levels=*/3, CreateRunsWithSize({1, 2, 3, 3})));
    ASSERT_TRUE(pick);
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(pick.value()), std::vector<int64_t>({1, 2, 3, 3}));

    // by size ratio
    ASSERT_OK_AND_ASSIGN(pick, compaction.Pick(/*num_levels=*/4, CreateRunsWithLevelAndSize(
                                                                     /*levels=*/{0, 1, 2, 3},
                                                                     /*sizes=*/{1, 1, 1, 50})));
    ASSERT_TRUE(pick);
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(pick.value()), std::vector<int64_t>({1, 1, 1}));

    // by file num
    ASSERT_OK_AND_ASSIGN(pick, compaction.Pick(/*num_levels=*/4, CreateRunsWithLevelAndSize(
                                                                     /*levels=*/{0, 1, 2, 3},
                                                                     /*sizes=*/{1, 50, 3, 500})));
    ASSERT_TRUE(pick);
    // 3 should be in the candidate, by size ratio after picking by file num
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(pick.value()), std::vector<int64_t>({1, 50, 3}));
}

TEST_F(UniversalCompactionTest, TestAllLevelRunsInvolved) {
    int64_t current_time = 0;
    auto full_compact_trigger = std::make_shared<TestableEarlyFullCompaction>(
        /*full_compaction_interval=*/std::nullopt,
        /*total_size_threshold=*/std::nullopt,
        /*incremental_size_threshold=*/1000l, &current_time);
    UniversalCompaction compaction(/*max_size_amp=*/100, /*size_ratio=*/1,
                                   /*num_run_compaction_trigger=*/3, full_compact_trigger, nullptr);
    ASSERT_OK_AND_ASSIGN(auto pick, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize(
                                                                          /*levels=*/{0, 0, 0},
                                                                          /*sizes=*/{1, 1, 3})));
    ASSERT_TRUE(pick);
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(pick.value()), std::vector<int64_t>({1, 1, 3}));
}

TEST_F(UniversalCompactionTest, TestOptimizedCompactionInterval) {
    int64_t current_time = 0;
    auto full_compact_trigger = std::make_shared<TestableEarlyFullCompaction>(
        /*full_compaction_interval=*/1000L,
        /*total_size_threshold=*/std::nullopt,
        /*incremental_size_threshold=*/std::nullopt, &current_time);
    UniversalCompaction compaction(/*max_size_amp=*/100, /*size_ratio=*/1,
                                   /*num_run_compaction_trigger=*/3, full_compact_trigger, nullptr);
    // first time, force optimized compaction
    ASSERT_OK_AND_ASSIGN(auto pick, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize(
                                                                          /*levels=*/{0, 1, 2},
                                                                          /*sizes=*/{1, 3, 5})));
    ASSERT_TRUE(pick);
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(pick.value()), std::vector<int64_t>({1, 3, 5}));

    // modify time, optimized compaction
    current_time = 1001L;
    ASSERT_OK_AND_ASSIGN(pick, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize(
                                                                     /*levels=*/{0, 1, 2},
                                                                     /*sizes=*/{1, 3, 5})));
    ASSERT_TRUE(pick);
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(pick.value()), std::vector<int64_t>({1, 3, 5}));

    // third time, no compaction
    ASSERT_OK_AND_ASSIGN(pick, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize(
                                                                     /*levels=*/{0, 1, 2},
                                                                     /*sizes=*/{1, 3, 5})));
    ASSERT_FALSE(pick);

    // 4 time, pickForSizeAmp
    current_time = 1500L;
    ASSERT_OK_AND_ASSIGN(pick, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize(
                                                                     /*levels=*/{0, 1, 2},
                                                                     /*sizes=*/{3, 3, 5})));
    ASSERT_TRUE(pick);
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(pick.value()), std::vector<int64_t>({3, 3, 5}));

    // 5 time, no compaction because pickForSizeAmp already done
    current_time = 2001L;
    ASSERT_OK_AND_ASSIGN(pick, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize(
                                                                     /*levels=*/{0, 1, 2},
                                                                     /*sizes=*/{1, 3, 5})));
    ASSERT_FALSE(pick);
}

TEST_F(UniversalCompactionTest, TestTotalSizeThreshold) {
    auto full_compact_trigger = std::make_shared<EarlyFullCompaction>(
        /*full_compaction_interval=*/std::nullopt,
        /*total_size_threshold=*/10L,
        /*incremental_size_threshold=*/std::nullopt);

    UniversalCompaction compaction(/*max_size_amp=*/100, /*size_ratio=*/1,
                                   /*num_run_compaction_trigger=*/3, full_compact_trigger, nullptr);
    // total size less than threshold
    ASSERT_OK_AND_ASSIGN(auto pick, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize(
                                                                          /*levels=*/{0, 1, 2},
                                                                          /*sizes=*/{1, 3, 5})));
    ASSERT_TRUE(pick);
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(pick.value()), std::vector<int64_t>({1, 3, 5}));

    // total size bigger than threshold
    ASSERT_OK_AND_ASSIGN(pick, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize(
                                                                     /*levels=*/{0, 1, 2},
                                                                     /*sizes=*/{2, 6, 10})));
    ASSERT_FALSE(pick);
    // one sort run, not trigger
    ASSERT_OK_AND_ASSIGN(pick, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize(
                                                                     /*levels=*/{3},
                                                                     /*sizes=*/{5})));
    ASSERT_FALSE(pick);
}

TEST_F(UniversalCompactionTest, TestNoOutputLevel0) {
    UniversalCompaction compaction(/*max_size_amp=*/25, /*size_ratio=*/1,
                                   /*num_run_compaction_trigger=*/3, nullptr, nullptr);
    ASSERT_OK_AND_ASSIGN(auto pick,
                         compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize(
                                                               /*levels=*/{0, 0, 1, 2},
                                                               /*sizes=*/{1, 1, 1, 50})));
    ASSERT_TRUE(pick);
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(pick.value()), std::vector<int64_t>({1, 1, 1}));

    ASSERT_OK_AND_ASSIGN(pick, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize(
                                                                     /*levels=*/{0, 0, 1, 2},
                                                                     /*sizes=*/{1, 2, 3, 50})));
    ASSERT_TRUE(pick);
    // 3 should be in the candidate, by size ratio after picking by file num
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(pick.value()), std::vector<int64_t>({1, 2, 3}));
}

TEST_F(UniversalCompactionTest, TestExtremeCaseNoOutputLevel0) {
    UniversalCompaction compaction(/*max_size_amp=*/200, /*size_ratio=*/1,
                                   /*num_run_compaction_trigger=*/5, nullptr, nullptr);
    ASSERT_OK_AND_ASSIGN(
        auto pick, compaction.Pick(/*num_levels=*/6, CreateRunsWithLevelAndSize(
                                                         /*levels=*/{0, 0, 0, 0, 0},
                                                         /*sizes=*/{1, 1, 1, 1024, 1024 * 1024})));
    ASSERT_TRUE(pick);
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(pick.value()),
              std::vector<int64_t>({1, 1, 1, 1024, 1024 * 1024}));
}

TEST_F(UniversalCompactionTest, TestSizeAmplification) {
    UniversalCompaction compaction(/*max_size_amp=*/25, /*size_ratio=*/0,
                                   /*num_run_compaction_trigger=*/1, nullptr, nullptr);

    std::vector<int64_t> sizes = {1};
    auto append_and_pick = [&](const std::vector<int64_t>& expected_sizes) {
        sizes.insert(sizes.begin(), 1);
        auto unit = compaction.PickForSizeAmp(3, CreateRunsWithSize(sizes));
        if (unit) {
            auto files = GetFileSizeVecFromCompactUnit(unit.value());
            int64_t total_size = std::accumulate(files.begin(), files.end(), 0l);
            sizes = {total_size};
        }
        ASSERT_EQ(sizes, expected_sizes);
    };

    append_and_pick({2});
    append_and_pick({3});
    append_and_pick({4});
    append_and_pick({1, 4});
    append_and_pick({6});
    append_and_pick({1, 6});
    append_and_pick({8});
    append_and_pick({1, 8});
    append_and_pick({1, 1, 8});
    append_and_pick({11});
    append_and_pick({1, 11});
    append_and_pick({1, 1, 11});
    append_and_pick({14});
    append_and_pick({1, 14});
    append_and_pick({1, 1, 14});
    append_and_pick({1, 1, 1, 14});
    append_and_pick({18});
}

TEST_F(UniversalCompactionTest, TestSizeRatio) {
    UniversalCompaction compaction(/*max_size_amp=*/25, /*size_ratio=*/1,
                                   /*num_run_compaction_trigger=*/5, nullptr, nullptr);

    std::vector<int64_t> sizes = {1, 1, 1, 1};
    auto append_and_pick = [&](const std::vector<int64_t>& expected_sizes) {
        sizes.insert(sizes.begin(), 1);
        std::vector<int32_t> levels;
        for (size_t i = 0; i < sizes.size(); i++) {
            levels.push_back(static_cast<int64_t>(i));
        }
        ASSERT_OK_AND_ASSIGN(
            auto unit, compaction.PickForSizeRatio(/*max_level=*/sizes.size(),
                                                   CreateRunsWithLevelAndSize(levels, sizes)));
        if (unit) {
            std::vector<int64_t> compact;
            compact.reserve(unit->files.size());
            for (const auto& file : unit->files) {
                compact.push_back(file->file_size);
            }
            std::vector<int64_t> result = sizes;
            for (int64_t size_val : compact) {
                auto it = std::find(result.begin(), result.end(), size_val);
                if (it != result.end()) {
                    result.erase(it);
                }
            }
            int64_t sum = std::accumulate(compact.begin(), compact.end(), 0l);
            result.insert(result.begin(), sum);
            sizes = result;
        }
        ASSERT_EQ(sizes, expected_sizes);
    };

    append_and_pick({5});
    append_and_pick({1, 5});
    append_and_pick({1, 1, 5});
    append_and_pick({1, 1, 1, 5});
    append_and_pick({4, 5});
    append_and_pick({1, 4, 5});
    append_and_pick({1, 1, 4, 5});
    append_and_pick({3, 4, 5});
    append_and_pick({1, 3, 4, 5});
    append_and_pick({2, 3, 4, 5});
    append_and_pick({1, 2, 3, 4, 5});
    append_and_pick({16});
    append_and_pick({1, 16});
    append_and_pick({1, 1, 16});
    append_and_pick({1, 1, 1, 16});
    append_and_pick({4, 16});
    append_and_pick({1, 4, 16});
    append_and_pick({1, 1, 4, 16});
    append_and_pick({3, 4, 16});
    append_and_pick({1, 3, 4, 16});
    append_and_pick({2, 3, 4, 16});
    append_and_pick({1, 2, 3, 4, 16});
    append_and_pick({11, 16});
}
TEST_F(UniversalCompactionTest, TestSizeRatioThreshold) {
    {
        UniversalCompaction compaction(/*max_size_amp=*/25, /*size_ratio=*/10,
                                       /*num_run_compaction_trigger=*/2, nullptr, nullptr);
        ASSERT_OK_AND_ASSIGN(
            auto unit, compaction.PickForSizeRatio(
                           /*max_level=*/3,
                           CreateRunsWithLevelAndSize(/*levels=*/{0, 1, 2}, /*sizes=*/{8, 9, 10})));
        ASSERT_FALSE(unit);
    }
    {
        UniversalCompaction compaction(/*max_size_amp=*/25, /*size_ratio=*/20,
                                       /*num_run_compaction_trigger=*/2, nullptr, nullptr);
        ASSERT_OK_AND_ASSIGN(
            auto unit, compaction.PickForSizeRatio(
                           /*max_level=*/3,
                           CreateRunsWithLevelAndSize(/*levels=*/{0, 1, 2}, /*sizes=*/{8, 9, 10})));
        ASSERT_TRUE(unit);
        ASSERT_EQ(GetFileSizeVecFromCompactUnit(unit.value()), std::vector<int64_t>({8, 9, 10}));
    }
}

TEST_F(UniversalCompactionTest, TestLookup) {
    auto universal =
        std::make_shared<UniversalCompaction>(/*max_size_amp=*/25, /*size_ratio=*/1,
                                              /*num_run_compaction_trigger=*/3, nullptr, nullptr);
    ForceUpLevel0Compaction compaction(universal, /*max_compact_interval=*/std::nullopt);

    // level 0 to max level
    ASSERT_OK_AND_ASSIGN(auto unit,
                         compaction.Pick(/*num_levels=*/3, CreateRunsWithSize({1, 2, 2, 2})));
    ASSERT_TRUE(unit);
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(unit.value()), std::vector<int64_t>({1, 2, 2, 2}));
    ASSERT_EQ(unit.value().output_level, 2);

    // level 0 force pick
    ASSERT_OK_AND_ASSIGN(
        unit, compaction.Pick(/*num_levels=*/3, CreateRunsWithLevelAndSize(/*levels=*/{0, 1, 2},
                                                                           /*sizes=*/{1, 2, 2})));
    ASSERT_TRUE(unit);
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(unit.value()), std::vector<int64_t>({1, 2, 2}));
    ASSERT_EQ(unit.value().output_level, 2);

    // level 0 to empty level
    ASSERT_OK_AND_ASSIGN(
        unit, compaction.Pick(/*num_levels=*/3,
                              CreateRunsWithLevelAndSize(/*levels=*/{0, 2}, /*sizes=*/{1, 2})));
    ASSERT_TRUE(unit);
    ASSERT_EQ(GetFileSizeVecFromCompactUnit(unit.value()), std::vector<int64_t>({1}));
    ASSERT_EQ(unit.value().output_level, 1);
}

TEST_F(UniversalCompactionTest, TestForcePickL0) {
    int32_t max_compact_interval = 5;
    auto universal =
        std::make_shared<UniversalCompaction>(/*max_size_amp=*/25, /*size_ratio=*/1,
                                              /*num_run_compaction_trigger=*/5, nullptr, nullptr);
    ForceUpLevel0Compaction compaction(universal, max_compact_interval);

    // level 0 to max level
    auto level0_to_max = CreateRunsWithSize({1, 2, 2, 2});
    std::optional<CompactUnit> unit;
    for (int32_t i = 1; i <= max_compact_interval; i++) {
        // level 0 to max level triggered
        ASSERT_OK_AND_ASSIGN(unit, compaction.Pick(/*num_levels=*/3, level0_to_max));
        if (i == max_compact_interval) {
            ASSERT_TRUE(unit);
            ASSERT_EQ(GetFileSizeVecFromCompactUnit(unit.value()),
                      std::vector<int64_t>({1, 2, 2, 2}));
            ASSERT_EQ(unit.value().output_level, 2);
        } else {
            // compact skipped
            ASSERT_FALSE(unit);
        }
    }

    // level 0 force pick
    auto level0_force_pick = CreateRunsWithLevelAndSize(/*levels=*/{0, 1, 2}, /*sizes=*/{2, 2, 2});
    for (int32_t i = 1; i <= max_compact_interval; i++) {
        ASSERT_OK_AND_ASSIGN(unit, compaction.Pick(/*num_levels=*/3, level0_force_pick));
        if (i == max_compact_interval) {
            // level 0 force pick triggered
            ASSERT_TRUE(unit);
            ASSERT_EQ(GetFileSizeVecFromCompactUnit(unit.value()), std::vector<int64_t>({2, 2, 2}));
            ASSERT_EQ(unit.value().output_level, 2);
        } else {
            // compact skipped
            ASSERT_FALSE(unit);
        }
    }

    // level 0 to empty level
    auto level0_to_empty = CreateRunsWithLevelAndSize(/*levels=*/{0, 2}, /*sizes=*/{1, 2});
    for (int32_t i = 1; i <= max_compact_interval; i++) {
        ASSERT_OK_AND_ASSIGN(unit, compaction.Pick(/*num_levels=*/3, level0_to_empty));
        if (i == max_compact_interval) {
            // level 0 force pick triggered
            ASSERT_TRUE(unit);
            ASSERT_EQ(GetFileSizeVecFromCompactUnit(unit.value()), std::vector<int64_t>({1}));
            ASSERT_EQ(unit.value().output_level, 1);
        } else {
            // compact skipped
            ASSERT_FALSE(unit);
        }
    }
}
}  // namespace paimon::test
