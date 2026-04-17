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

#include "paimon/core/mergetree/compact/merge_tree_compact_manager.h"

#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/mergetree/level_sorted_run.h"
#include "paimon/core/mergetree/levels.h"
#include "paimon/core/mergetree/sorted_run.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

class InlineExecutor final : public Executor {
 public:
    void Add(std::function<void()> func) override {
        func();
    }

    void ShutdownNow() override {}
};

class QueuedExecutor final : public Executor {
 public:
    void Add(std::function<void()> func) override {
        tasks_.push_back(std::move(func));
    }

    void ShutdownNow() override {}

    void RunAll() {
        for (auto& task : tasks_) {
            task();
        }
        tasks_.clear();
    }

 private:
    std::vector<std::function<void()>> tasks_;
};

class FunctionalCompactStrategy final : public CompactStrategy {
 public:
    using PickFunc =
        std::function<std::optional<CompactUnit>(int32_t, const std::vector<LevelSortedRun>&)>;

    explicit FunctionalCompactStrategy(PickFunc func) : func_(std::move(func)) {}

    Result<std::optional<CompactUnit>> Pick(int32_t num_levels,
                                            const std::vector<LevelSortedRun>& runs) override {
        return func_(num_levels, runs);
    }

 private:
    PickFunc func_;
};

class TestRewriter final : public CompactRewriter {
 public:
    explicit TestRewriter(bool expected_drop_delete)
        : expected_drop_delete_(expected_drop_delete) {}

    Result<CompactResult> Rewrite(int32_t output_level, bool drop_delete,
                                  const std::vector<std::vector<SortedRun>>& sections) override {
        EXPECT_EQ(drop_delete, expected_drop_delete_);

        int32_t min_key = std::numeric_limits<int32_t>::max();
        int32_t max_key = std::numeric_limits<int32_t>::min();
        int64_t min_sequence = std::numeric_limits<int64_t>::max();
        int64_t max_sequence = 0;
        std::vector<std::shared_ptr<DataFileMeta>> before;

        for (const auto& section : sections) {
            for (const auto& run : section) {
                for (const auto& file : run.Files()) {
                    before.push_back(file);
                    min_key = std::min(min_key, file->min_key.GetInt(0));
                    max_key = std::max(max_key, file->max_key.GetInt(0));
                    min_sequence = std::min(min_sequence, file->min_sequence_number);
                    max_sequence = std::max(max_sequence, file->max_sequence_number);
                }
            }
        }

        if (before.empty()) {
            return CompactResult({}, {});
        }

        auto after = std::make_shared<DataFileMeta>(
            "rewrite-" + std::to_string(rewrite_id_++),
            /*file_size=*/max_key - min_key + 1,
            /*row_count=*/0, BinaryRowGenerator::GenerateRow({min_key}, pool_.get()),
            BinaryRowGenerator::GenerateRow({max_key}, pool_.get()), SimpleStats::EmptyStats(),
            SimpleStats::EmptyStats(),
            /*min_sequence_number=*/min_sequence,
            /*max_sequence_number=*/max_sequence,
            /*schema_id=*/0, output_level, std::vector<std::optional<std::string>>(),
            Timestamp(1, 0), std::nullopt, nullptr, FileSource::Append(), std::nullopt,
            std::nullopt, std::nullopt, std::nullopt);

        return CompactResult(before, {after});
    }

    Result<CompactResult> Upgrade(int32_t output_level,
                                  const std::shared_ptr<DataFileMeta>& file) override {
        PAIMON_ASSIGN_OR_RAISE(auto upgraded, file->Upgrade(output_level));
        return CompactResult({file}, {upgraded});
    }

    Status Close() override {
        return Status::OK();
    }

 private:
    bool expected_drop_delete_;
    mutable int64_t rewrite_id_ = 0;
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

struct LevelMinMax {
    int32_t level;
    int32_t min;
    int32_t max;

    LevelMinMax(int32_t level_value, int32_t min_value, int32_t max_value)
        : level(level_value), min(min_value), max(max_value) {}

    explicit LevelMinMax(const std::shared_ptr<DataFileMeta>& file)
        : level(file->level), min(file->min_key.GetInt(0)), max(file->max_key.GetInt(0)) {}

    bool operator==(const LevelMinMax& other) const {
        return level == other.level && min == other.min && max == other.max;
    }
};

}  // namespace

class MergeTreeCompactManagerTest : public testing::Test {
 protected:
    using StrategyFn =
        std::function<std::optional<CompactUnit>(int32_t, const std::vector<LevelSortedRun>&)>;

    void SetUp() override {
        pool_ = GetDefaultPool();
        std::vector<DataField> data_fields;
        data_fields.emplace_back(/*id=*/0, arrow::field("f0", arrow::int32()));
        ASSERT_OK_AND_ASSIGN(auto comparator,
                             FieldsComparator::Create(data_fields, /*is_ascending_order=*/true));
        comparator_ = std::shared_ptr<FieldsComparator>(std::move(comparator));
        file_id_ = 0;
    }

    std::shared_ptr<DataFileMeta> ToFile(const LevelMinMax& minmax, int64_t max_sequence) {
        const int64_t file_size = minmax.max - minmax.min + 1;
        return std::make_shared<DataFileMeta>(
            "f-" + std::to_string(file_id_++),
            /*file_size=*/file_size,
            /*row_count=*/0, BinaryRowGenerator::GenerateRow({minmax.min}, pool_.get()),
            BinaryRowGenerator::GenerateRow({minmax.max}, pool_.get()), SimpleStats::EmptyStats(),
            SimpleStats::EmptyStats(),
            /*min_sequence_number=*/minmax.min,
            /*max_sequence_number=*/max_sequence,
            /*schema_id=*/0, minmax.level, std::vector<std::optional<std::string>>(),
            Timestamp(1, 0), std::nullopt, nullptr, FileSource::Append(), std::nullopt,
            std::nullopt, std::nullopt, std::nullopt);
    }

    StrategyFn TestStrategy() {
        return [](int32_t num_levels,
                  const std::vector<LevelSortedRun>& runs) -> std::optional<CompactUnit> {
            return CompactUnit::FromLevelRuns(num_levels - 1, runs);
        };
    }

    Result<std::shared_ptr<Levels>> CreateLevels(
        const std::vector<std::shared_ptr<DataFileMeta>>& files) {
        PAIMON_ASSIGN_OR_RAISE(auto levels, Levels::Create(comparator_, files, /*num_levels=*/3));
        return std::shared_ptr<Levels>(std::move(levels));
    }

    void InnerTest(const std::vector<LevelMinMax>& inputs,
                   const std::vector<LevelMinMax>& expected) {
        InnerTest(inputs, expected, TestStrategy(), /*expected_drop_delete=*/true);
    }

    void InnerTest(const std::vector<LevelMinMax>& inputs, const std::vector<LevelMinMax>& expected,
                   const StrategyFn& strategy, bool expected_drop_delete) {
        std::vector<std::shared_ptr<DataFileMeta>> files;
        files.reserve(inputs.size());
        for (size_t i = 0; i < inputs.size(); ++i) {
            files.push_back(ToFile(inputs[i], static_cast<int64_t>(i)));
        }

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels, CreateLevels(files));

        auto manager = std::make_shared<MergeTreeCompactManager>(
            levels, std::make_shared<FunctionalCompactStrategy>(strategy), comparator_,
            /*compaction_file_size=*/2,
            /*num_sorted_run_stop_trigger=*/std::numeric_limits<int32_t>::max(),
            std::make_shared<TestRewriter>(expected_drop_delete),
            /*metrics_reporter=*/nullptr,
            /*dv_maintainer=*/nullptr,
            /*lazy_gen_deletion_file=*/false,
            /*need_lookup=*/false,
            /*force_rewrite_all_files=*/false,
            /*force_keep_delete=*/false, std::make_shared<CancellationController>(),
            std::make_shared<InlineExecutor>());

        ASSERT_OK(manager->TriggerCompaction(/*full_compaction=*/false));
        ASSERT_OK_AND_ASSIGN(auto compact_result, manager->GetCompactionResult(/*blocking=*/true));
        (void)compact_result;

        std::vector<LevelMinMax> outputs;
        for (const auto& file : levels->AllFiles()) {
            outputs.emplace_back(file);
        }

        ASSERT_EQ(outputs.size(), expected.size());
        EXPECT_EQ(outputs, expected);
    }

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FieldsComparator> comparator_;
    int64_t file_id_ = 0;
};

TEST_F(MergeTreeCompactManagerTest, TestOutputToZeroLevel) {
    InnerTest(
        {LevelMinMax(0, 1, 3), LevelMinMax(0, 1, 5), LevelMinMax(0, 1, 8)},
        {LevelMinMax(0, 1, 8), LevelMinMax(0, 1, 3)},
        [](int32_t, const std::vector<LevelSortedRun>& runs) -> std::optional<CompactUnit> {
            return CompactUnit::FromLevelRuns(0, {runs[0], runs[1]});
        },
        /*expected_drop_delete=*/false);
}

TEST_F(MergeTreeCompactManagerTest, TestCompactToPenultimateLayer) {
    InnerTest(
        {LevelMinMax(0, 1, 3), LevelMinMax(0, 1, 5), LevelMinMax(2, 1, 7)},
        {LevelMinMax(1, 1, 5), LevelMinMax(2, 1, 7)},
        [](int32_t, const std::vector<LevelSortedRun>& runs) -> std::optional<CompactUnit> {
            return CompactUnit::FromLevelRuns(1, {runs[0], runs[1]});
        },
        /*expected_drop_delete=*/false);
}

TEST_F(MergeTreeCompactManagerTest, TestNoCompaction) {
    InnerTest(
        {LevelMinMax(3, 1, 3)}, {LevelMinMax(3, 1, 3)},
        [](int32_t, const std::vector<LevelSortedRun>&) -> std::optional<CompactUnit> {
            return std::nullopt;
        },
        /*expected_drop_delete=*/true);
}

TEST_F(MergeTreeCompactManagerTest, TestNormal) {
    InnerTest({LevelMinMax(0, 1, 3), LevelMinMax(1, 1, 5), LevelMinMax(1, 6, 7)},
              {LevelMinMax(2, 1, 5), LevelMinMax(2, 6, 7)});
}

TEST_F(MergeTreeCompactManagerTest, TestUpgrade) {
    InnerTest({LevelMinMax(0, 1, 3), LevelMinMax(0, 1, 5), LevelMinMax(0, 6, 8)},
              {LevelMinMax(2, 1, 5), LevelMinMax(2, 6, 8)});
}

TEST_F(MergeTreeCompactManagerTest, TestSmallFiles) {
    InnerTest({LevelMinMax(0, 1, 1), LevelMinMax(0, 2, 2)}, {LevelMinMax(2, 1, 2)});
}

TEST_F(MergeTreeCompactManagerTest, TestSmallFilesNoCompact) {
    InnerTest(
        {LevelMinMax(0, 1, 5), LevelMinMax(0, 6, 6), LevelMinMax(1, 7, 8), LevelMinMax(1, 9, 10)},
        {LevelMinMax(2, 1, 5), LevelMinMax(2, 6, 6), LevelMinMax(2, 7, 8), LevelMinMax(2, 9, 10)});
}

TEST_F(MergeTreeCompactManagerTest, TestSmallFilesCrossLevel) {
    InnerTest(
        {LevelMinMax(0, 1, 5), LevelMinMax(0, 6, 6), LevelMinMax(1, 7, 7), LevelMinMax(1, 9, 10)},
        {LevelMinMax(2, 1, 5), LevelMinMax(2, 6, 7), LevelMinMax(2, 9, 10)});
}

TEST_F(MergeTreeCompactManagerTest, TestComplex) {
    InnerTest(
        {LevelMinMax(0, 1, 5), LevelMinMax(0, 6, 6), LevelMinMax(1, 1, 4), LevelMinMax(1, 6, 8),
         LevelMinMax(1, 10, 11), LevelMinMax(2, 1, 3), LevelMinMax(2, 4, 6)},
        {LevelMinMax(2, 1, 8), LevelMinMax(2, 10, 11)});
}

TEST_F(MergeTreeCompactManagerTest, TestSmallInComplex) {
    InnerTest(
        {LevelMinMax(0, 1, 5), LevelMinMax(0, 6, 6), LevelMinMax(1, 1, 4), LevelMinMax(1, 6, 8),
         LevelMinMax(1, 10, 10), LevelMinMax(2, 1, 3), LevelMinMax(2, 4, 6)},
        {LevelMinMax(2, 1, 10)});
}

TEST_F(MergeTreeCompactManagerTest, TestIsCompacting) {
    std::vector<LevelMinMax> inputs = {LevelMinMax(0, 1, 3), LevelMinMax(1, 1, 5),
                                       LevelMinMax(1, 6, 7)};
    std::vector<std::shared_ptr<DataFileMeta>> files;
    files.reserve(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
        files.push_back(ToFile(inputs[i], static_cast<int64_t>(i)));
    }

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> lookup_levels, CreateLevels(files));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> default_levels, CreateLevels(files));

    auto strategy = std::make_shared<FunctionalCompactStrategy>(TestStrategy());

    auto lookup_manager = std::make_shared<MergeTreeCompactManager>(
        lookup_levels, strategy, comparator_,
        /*compaction_file_size=*/2,
        /*num_sorted_run_stop_trigger=*/std::numeric_limits<int32_t>::max(),
        std::make_shared<TestRewriter>(/*expected_drop_delete=*/true),
        /*metrics_reporter=*/nullptr,
        /*dv_maintainer=*/nullptr,
        /*lazy_gen_deletion_file=*/false,
        /*need_lookup=*/true,
        /*force_rewrite_all_files=*/false,
        /*force_keep_delete=*/false, std::make_shared<CancellationController>(),
        std::make_shared<InlineExecutor>());

    auto default_manager = std::make_shared<MergeTreeCompactManager>(
        default_levels, strategy, comparator_,
        /*compaction_file_size=*/2,
        /*num_sorted_run_stop_trigger=*/std::numeric_limits<int32_t>::max(),
        std::make_shared<TestRewriter>(/*expected_drop_delete=*/true),
        /*metrics_reporter=*/nullptr,
        /*dv_maintainer=*/nullptr,
        /*lazy_gen_deletion_file=*/false,
        /*need_lookup=*/false,
        /*force_rewrite_all_files=*/false,
        /*force_keep_delete=*/false, std::make_shared<CancellationController>(),
        std::make_shared<InlineExecutor>());

    EXPECT_TRUE(lookup_manager->CompactNotCompleted());
    EXPECT_FALSE(default_manager->CompactNotCompleted());
}

TEST_F(MergeTreeCompactManagerTest, TestTriggerFullCompaction) {
    std::vector<LevelMinMax> inputs = {LevelMinMax(0, 1, 3), LevelMinMax(1, 2, 5),
                                       LevelMinMax(1, 6, 7)};
    std::vector<std::shared_ptr<DataFileMeta>> files;
    files.reserve(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
        files.push_back(ToFile(inputs[i], static_cast<int64_t>(i)));
    }

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels, CreateLevels(files));

    auto manager = std::make_shared<MergeTreeCompactManager>(
        levels, std::make_shared<FunctionalCompactStrategy>(TestStrategy()), comparator_,
        /*compaction_file_size=*/2,
        /*num_sorted_run_stop_trigger=*/std::numeric_limits<int32_t>::max(),
        std::make_shared<TestRewriter>(/*expected_drop_delete=*/true),
        /*metrics_reporter=*/nullptr,
        /*dv_maintainer=*/nullptr,
        /*lazy_gen_deletion_file=*/false,
        /*need_lookup=*/false,
        /*force_rewrite_all_files=*/false,
        /*force_keep_delete=*/false, std::make_shared<CancellationController>(),
        std::make_shared<InlineExecutor>());

    ASSERT_OK(manager->TriggerCompaction(/*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(auto compact_result, manager->GetCompactionResult(/*blocking=*/true));
    ASSERT_TRUE(compact_result.has_value());
    ASSERT_FALSE(compact_result.value()->Before().empty());
    ASSERT_FALSE(compact_result.value()->After().empty());
    for (const auto& after : compact_result.value()->After()) {
        EXPECT_EQ(after->level, 2);
    }
}

TEST_F(MergeTreeCompactManagerTest, TestRejectReentrantFullCompaction) {
    std::vector<LevelMinMax> inputs = {LevelMinMax(0, 1, 3), LevelMinMax(1, 2, 5),
                                       LevelMinMax(1, 6, 7)};
    std::vector<std::shared_ptr<DataFileMeta>> files;
    files.reserve(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
        files.push_back(ToFile(inputs[i], static_cast<int64_t>(i)));
    }

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Levels> levels, CreateLevels(files));

    auto queued_executor = std::make_shared<QueuedExecutor>();
    auto manager = std::make_shared<MergeTreeCompactManager>(
        levels, std::make_shared<FunctionalCompactStrategy>(TestStrategy()), comparator_,
        /*compaction_file_size=*/2,
        /*num_sorted_run_stop_trigger=*/std::numeric_limits<int32_t>::max(),
        std::make_shared<TestRewriter>(/*expected_drop_delete=*/true),
        /*metrics_reporter=*/nullptr,
        /*dv_maintainer=*/nullptr,
        /*lazy_gen_deletion_file=*/false,
        /*need_lookup=*/false,
        /*force_rewrite_all_files=*/false,
        /*force_keep_delete=*/false, std::make_shared<CancellationController>(), queued_executor);

    ASSERT_OK(manager->TriggerCompaction(/*full_compaction=*/true));
    ASSERT_NOK_WITH_MSG(
        manager->TriggerCompaction(/*full_compaction=*/true),
        "A compaction task is still running while the user forces a new compaction.");
    queued_executor->RunAll();
    ASSERT_OK_AND_ASSIGN(auto compact_result, manager->GetCompactionResult(/*blocking=*/true));
    ASSERT_TRUE(compact_result.has_value());
}

}  // namespace paimon::test
