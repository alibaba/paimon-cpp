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

#include "paimon/core/mergetree/levels.h"

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class LevelsTest : public testing::Test {
 public:
    std::shared_ptr<DataFileMeta> CreateDataFileMeta(int32_t level, int64_t min_sequence_number,
                                                     int64_t max_sequence_number,
                                                     int64_t ts_second) const {
        std::string uuid;
        EXPECT_TRUE(UUID::Generate(&uuid));
        return std::make_shared<DataFileMeta>(
            /*file_name=*/uuid, /*file_size=*/1,
            /*row_count=*/max_sequence_number - min_sequence_number + 1,
            BinaryRowGenerator::GenerateRow({min_sequence_number}, pool_.get()),
            BinaryRowGenerator::GenerateRow({max_sequence_number}, pool_.get()),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), min_sequence_number,
            max_sequence_number,
            /*schema_id=*/0, level, std::vector<std::optional<std::string>>(),
            Timestamp(ts_second, 0l), std::nullopt, nullptr, FileSource::Append(), std::nullopt,
            std::nullopt, std::nullopt, std::nullopt);
    }

    std::shared_ptr<FieldsComparator> CreateComparator() const {
        std::vector<DataField> data_fields;
        data_fields.emplace_back(/*id=*/0, arrow::field("f0", arrow::int32()));
        EXPECT_OK_AND_ASSIGN(auto cmp,
                             FieldsComparator::Create(data_fields, /*is_ascending_order=*/true));
        return cmp;
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(LevelsTest, TestNonEmptyHighestLevelNo) {
    std::vector<std::shared_ptr<DataFileMeta>> input_files;
    ASSERT_OK_AND_ASSIGN(auto levels,
                         Levels::Create(CreateComparator(), input_files, /*num_levels=*/3));
    ASSERT_EQ(levels->NonEmptyHighestLevel(), -1);
}

TEST_F(LevelsTest, TestInvalidNumberLevels) {
    std::vector<std::shared_ptr<DataFileMeta>> input_files;
    ASSERT_NOK_WITH_MSG(Levels::Create(CreateComparator(), input_files, /*num_levels=*/1),
                        "Number of levels must be at least 2.");
}

TEST_F(LevelsTest, TestAddLevel0FileInvalid) {
    std::vector<std::shared_ptr<DataFileMeta>> input_files = {CreateDataFileMeta(0, 0, 1, 0),
                                                              CreateDataFileMeta(0, 2, 3, 0)};
    ASSERT_OK_AND_ASSIGN(auto levels,
                         Levels::Create(CreateComparator(), input_files, /*num_levels=*/3));
    std::vector<std::shared_ptr<DataFileMeta>> new_files = {CreateDataFileMeta(1, 0, 1, 0)};
    ASSERT_NOK_WITH_MSG(levels->AddLevel0File(new_files[0]),
                        "must add level0 file in AddLevel0File");
}

TEST_F(LevelsTest, TestNonEmptyHighestLevel0) {
    std::vector<std::shared_ptr<DataFileMeta>> input_files = {CreateDataFileMeta(0, 0, 1, 0),
                                                              CreateDataFileMeta(0, 2, 3, 0)};
    ASSERT_OK_AND_ASSIGN(auto levels,
                         Levels::Create(CreateComparator(), input_files, /*num_levels=*/3));
    ASSERT_EQ(levels->NonEmptyHighestLevel(), 0);
}

TEST_F(LevelsTest, TestNonEmptyHighestLevel1) {
    std::vector<std::shared_ptr<DataFileMeta>> input_files = {CreateDataFileMeta(0, 0, 1, 0),
                                                              CreateDataFileMeta(1, 2, 3, 0)};
    ASSERT_OK_AND_ASSIGN(auto levels,
                         Levels::Create(CreateComparator(), input_files, /*num_levels=*/3));
    ASSERT_EQ(levels->NonEmptyHighestLevel(), 1);
    ASSERT_EQ(levels->NumberOfLevels(), 3);
    ASSERT_EQ(levels->MaxLevel(), 2);
}

TEST_F(LevelsTest, TestNonEmptyHighestLevel2) {
    std::vector<std::shared_ptr<DataFileMeta>> input_files = {
        CreateDataFileMeta(0, 0, 100, 0), CreateDataFileMeta(0, 100, 200, 0),
        CreateDataFileMeta(0, 0, 200, 0), CreateDataFileMeta(0, 0, 200, 10),
        CreateDataFileMeta(1, 0, 500, 0), CreateDataFileMeta(2, 0, 1000, 0)};
    ASSERT_OK_AND_ASSIGN(auto levels,
                         Levels::Create(CreateComparator(), input_files, /*num_levels=*/3));
    ASSERT_EQ(levels->NonEmptyHighestLevel(), 2);
    ASSERT_EQ(levels->TotalFileSize(), 6);

    std::vector<LevelSortedRun> expected_sorted_run = {
        LevelSortedRun(0, SortedRun::FromSingle(input_files[2])),
        LevelSortedRun(0, SortedRun::FromSingle(input_files[3])),
        LevelSortedRun(0, SortedRun::FromSingle(input_files[1])),
        LevelSortedRun(0, SortedRun::FromSingle(input_files[0])),
        LevelSortedRun(1, SortedRun::FromSingle(input_files[4])),
        LevelSortedRun(2, SortedRun::FromSingle(input_files[5])),
    };

    ASSERT_EQ(levels->LevelSortedRuns(), expected_sorted_run);
    ASSERT_EQ(levels->NumberOfSortedRuns(), 6);

    std::vector<std::shared_ptr<DataFileMeta>> expected_all_files = {
        input_files[2], input_files[3], input_files[1],
        input_files[0], input_files[4], input_files[5]};
    ASSERT_EQ(levels->AllFiles(), expected_all_files);
}

TEST_F(LevelsTest, TestAddLevel0File) {
    std::vector<std::shared_ptr<DataFileMeta>> input_files = {
        CreateDataFileMeta(0, 100, 200, 0), CreateDataFileMeta(0, 0, 200, 0),
        CreateDataFileMeta(0, 0, 200, 10), CreateDataFileMeta(1, 0, 500, 0),
        CreateDataFileMeta(2, 0, 1000, 0)};
    ASSERT_OK_AND_ASSIGN(auto levels,
                         Levels::Create(CreateComparator(), input_files, /*num_levels=*/3));
    ASSERT_EQ(levels->TotalFileSize(), 5);

    auto new_level0 = CreateDataFileMeta(0, 0, 100, 0);
    ASSERT_OK(levels->AddLevel0File(new_level0));
    ASSERT_EQ(levels->TotalFileSize(), 6);
    std::vector<LevelSortedRun> expected_sorted_run = {
        LevelSortedRun(0, SortedRun::FromSingle(input_files[1])),
        LevelSortedRun(0, SortedRun::FromSingle(input_files[2])),
        LevelSortedRun(0, SortedRun::FromSingle(input_files[0])),
        LevelSortedRun(0, SortedRun::FromSingle(new_level0)),
        LevelSortedRun(1, SortedRun::FromSingle(input_files[3])),
        LevelSortedRun(2, SortedRun::FromSingle(input_files[4])),
    };

    ASSERT_EQ(levels->LevelSortedRuns(), expected_sorted_run);
    ASSERT_EQ(levels->NumberOfSortedRuns(), 6);
}

TEST_F(LevelsTest, TestUpdate) {
    std::vector<std::shared_ptr<DataFileMeta>> input_files = {
        CreateDataFileMeta(0, 100, 200, 0), CreateDataFileMeta(0, 0, 200, 0),
        CreateDataFileMeta(0, 0, 200, 10), CreateDataFileMeta(1, 0, 500, 0),
        CreateDataFileMeta(1, 600, 1000, 0)};

    ASSERT_OK_AND_ASSIGN(auto levels,
                         Levels::Create(CreateComparator(), input_files, /*num_levels=*/3));
    ASSERT_EQ(levels->TotalFileSize(), 5);
    ASSERT_EQ(levels->NumberOfSortedRuns(), 4);

    std::vector<std::shared_ptr<DataFileMeta>> before = {
        input_files[1],
        input_files[3],
    };

    std::vector<std::shared_ptr<DataFileMeta>> after = {CreateDataFileMeta(0, 0, 100, 0),
                                                        CreateDataFileMeta(1, 0, 550, 0)};

    ASSERT_OK(levels->Update(before, after));

    std::vector<LevelSortedRun> expected_sorted_run = {
        LevelSortedRun(0, SortedRun::FromSingle(input_files[2])),
        LevelSortedRun(0, SortedRun::FromSingle(input_files[0])),
        LevelSortedRun(0, SortedRun::FromSingle(after[0])),
        LevelSortedRun(1, SortedRun::FromSorted({after[1], input_files[4]})),
    };

    ASSERT_EQ(levels->LevelSortedRuns(), expected_sorted_run);
    ASSERT_EQ(levels->NumberOfSortedRuns(), 4);
}

TEST_F(LevelsTest, TestRunOfLevelInvalidLevel) {
    std::vector<std::shared_ptr<DataFileMeta>> input_files = {CreateDataFileMeta(2, 0, 1, 0),
                                                              CreateDataFileMeta(1, 2, 3, 1)};
    ASSERT_OK_AND_ASSIGN(auto levels,
                         Levels::Create(CreateComparator(), input_files, /*num_levels=*/3));

    // Test invalid level 0
    auto result = Levels::RunOfLevel(0, levels->GetLevels());
    ASSERT_NOK_WITH_MSG(result, "Level0 does not have one single sorted run.");

    // Test invalid negative level
    auto result_neg = Levels::RunOfLevel(-1, levels->GetLevels());
    ASSERT_NOK_WITH_MSG(result_neg, "Level0 does not have one single sorted run.");
}

/// A simple test callback that records dropped file names.
class TestDropFileCallback : public Levels::DropFileCallback {
 public:
    void NotifyDropFile(const std::string& file) override {
        dropped_files.push_back(file);
    }
    std::vector<std::string> dropped_files;
};

TEST_F(LevelsTest, TestUpdateDropFileCallback) {
    std::vector<std::shared_ptr<DataFileMeta>> input_files = {
        CreateDataFileMeta(0, 100, 200, 0), CreateDataFileMeta(0, 0, 200, 0),
        CreateDataFileMeta(1, 0, 500, 0), CreateDataFileMeta(1, 600, 1000, 0)};

    ASSERT_OK_AND_ASSIGN(auto levels,
                         Levels::Create(CreateComparator(), input_files, /*num_levels=*/3));

    TestDropFileCallback callback;
    levels->AddDropFileCallback(&callback);

    // Remove input_files[0] from level0 and input_files[2] from level1,
    // add new files to level0 and level1.
    std::vector<std::shared_ptr<DataFileMeta>> before = {input_files[0], input_files[2]};
    std::vector<std::shared_ptr<DataFileMeta>> after = {CreateDataFileMeta(0, 0, 100, 0),
                                                        CreateDataFileMeta(1, 0, 550, 0)};
    ASSERT_OK(levels->Update(before, after));

    // Both files in before are replaced by new files, so both should be dropped.
    ASSERT_EQ(callback.dropped_files.size(), 2);
    std::set<std::string> dropped_set(callback.dropped_files.begin(), callback.dropped_files.end());
    ASSERT_TRUE(dropped_set.count(input_files[0]->file_name));
    ASSERT_TRUE(dropped_set.count(input_files[2]->file_name));
}

TEST_F(LevelsTest, TestUpdateDropFileCallbackExcludesUpgradeFiles) {
    auto file_level0 = CreateDataFileMeta(0, 0, 100, 0);
    auto file_level1 = CreateDataFileMeta(1, 200, 300, 0);

    std::vector<std::shared_ptr<DataFileMeta>> input_files = {file_level0, file_level1};
    ASSERT_OK_AND_ASSIGN(auto levels,
                         Levels::Create(CreateComparator(), input_files, /*num_levels=*/3));

    TestDropFileCallback callback;
    levels->AddDropFileCallback(&callback);

    // Simulate an upgrade: file_level0 appears in both before and after (same file_name).
    // Create a new version of the file at level 1 with the same file_name.
    auto upgraded_file = std::make_shared<DataFileMeta>(
        /*file_name=*/file_level0->file_name, /*file_size=*/file_level0->file_size,
        /*row_count=*/file_level0->row_count, file_level0->min_key, file_level0->max_key,
        file_level0->key_stats, file_level0->value_stats, file_level0->min_sequence_number,
        file_level0->max_sequence_number, file_level0->schema_id, /*level=*/1,
        file_level0->extra_files, file_level0->creation_time, std::nullopt, nullptr,
        FileSource::Append(), std::nullopt, std::nullopt, std::nullopt, std::nullopt);

    std::vector<std::shared_ptr<DataFileMeta>> before = {file_level0};
    std::vector<std::shared_ptr<DataFileMeta>> after = {upgraded_file};
    ASSERT_OK(levels->Update(before, after));

    // The file was upgraded (same file_name in before and after), so it should NOT be dropped.
    ASSERT_TRUE(callback.dropped_files.empty());
}

TEST_F(LevelsTest, TestUpdateNoCallbackWhenNoDroppedFiles) {
    std::vector<std::shared_ptr<DataFileMeta>> input_files = {CreateDataFileMeta(0, 0, 100, 0)};
    ASSERT_OK_AND_ASSIGN(auto levels,
                         Levels::Create(CreateComparator(), input_files, /*num_levels=*/3));

    TestDropFileCallback callback;
    levels->AddDropFileCallback(&callback);

    // Update with empty before and after.
    ASSERT_OK(levels->Update({}, {}));
    ASSERT_TRUE(callback.dropped_files.empty());

    // Update with only after (adding new files).
    std::vector<std::shared_ptr<DataFileMeta>> after = {CreateDataFileMeta(0, 200, 300, 0)};
    ASSERT_OK(levels->Update({}, after));
    ASSERT_TRUE(callback.dropped_files.empty());
}

}  // namespace paimon::test
