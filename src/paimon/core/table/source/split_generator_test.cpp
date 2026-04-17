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

#include "paimon/core/table/source/split_generator.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/options/merge_engine.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/source/append_only_split_generator.h"
#include "paimon/core/table/source/merge_tree_split_generator.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class SplitGeneratorTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        key_comparator_ =
            FieldsComparator::Create({DataField(0, arrow::field("f0", arrow::int32(), false))},
                                     /*is_ascending_order=*/true)
                .value();
    }
    void TearDown() override {}

    std::shared_ptr<DataFileMeta> CreateDataFileMeta(const std::string& file_name,
                                                     int64_t file_size, int64_t min_sequence_number,
                                                     int64_t max_sequence_number) {
        return std::make_shared<DataFileMeta>(
            file_name, file_size, /*row_count=*/1, /*min_key=*/BinaryRow::EmptyRow(),
            /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
            /*value_stats=*/SimpleStats::EmptyStats(), min_sequence_number, max_sequence_number,
            /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0), /*delete_row_count=*/0,
            /*embedded_index=*/nullptr, FileSource::Append(), /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::optional<std::string>(),
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    }

    std::shared_ptr<DataFileMeta> CreateDataFileMeta(const std::string& file_name, int32_t level,
                                                     int32_t min_key, int32_t max_key,
                                                     int64_t max_sequence_number) {
        return CreateDataFileMeta(file_name, level, min_key, max_key, max_sequence_number, 0l);
    }

    std::shared_ptr<DataFileMeta> CreateDataFileMeta(const std::string& file_name, int32_t level,
                                                     int32_t min_key, int32_t max_key,
                                                     int64_t max_sequence_number,
                                                     std::optional<int64_t> delete_row_count) {
        return std::make_shared<DataFileMeta>(
            file_name, max_key - min_key + 1, /*row_count=*/max_key - min_key + 1,
            BinaryRowGenerator::GenerateRow({min_key}, pool_.get()),
            BinaryRowGenerator::GenerateRow({max_key}, pool_.get()),
            /*key_stats=*/SimpleStats::EmptyStats(),
            /*value_stats=*/SimpleStats::EmptyStats(), 0, max_sequence_number,
            /*schema_id=*/0,
            /*level=*/level, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0), /*delete_row_count=*/delete_row_count,
            /*embedded_index=*/nullptr, FileSource::Append(),
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    }

    std::shared_ptr<DataFileMeta> CreateDataFileMeta(const std::string& file_name, int32_t min_key,
                                                     int32_t max_key) {
        return std::make_shared<DataFileMeta>(
            file_name, /*file_size=*/max_key - min_key + 1,
            /*row_count=*/max_key - min_key + 1, /*min_key=*/
            BinaryRowGenerator::GenerateRow({min_key}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({max_key}, pool_.get()),
            /*key_stats=*/SimpleStats::EmptyStats(),
            /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
            /*max_sequence_number=*/0,
            /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0), /*delete_row_count=*/0,
            /*embedded_index=*/nullptr, FileSource::Append(), /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    }

    static void CheckResult(const std::vector<SplitGenerator::SplitGroup>& result_groups,
                            const std::vector<std::vector<std::string>>& expected_file_names,
                            const std::vector<bool>& expected_raw_convertible) {
        std::vector<bool> result_raw_convertible;
        for (const auto& group : result_groups) {
            result_raw_convertible.push_back(group.raw_convertible);
        }
        ASSERT_EQ(result_raw_convertible, expected_raw_convertible);
        CheckResult(result_groups, expected_file_names);
    }

    static void CheckResult(const std::vector<SplitGenerator::SplitGroup>& result_groups,
                            const std::vector<std::vector<std::string>>& expected_file_names) {
        std::vector<std::vector<std::string>> expected = expected_file_names;
        std::vector<std::vector<std::string>> result;
        for (const auto& group : result_groups) {
            std::vector<std::string> one_group_files;
            for (const auto& file : group.files) {
                one_group_files.push_back(file->file_name);
            }
            result.push_back(std::move(one_group_files));
        }
        for (auto& re : result) {
            std::sort(re.begin(), re.end());
        }
        for (auto& ep : expected) {
            std::sort(ep.begin(), ep.end());
        }
        std::sort(result.begin(), result.end());
        std::sort(expected.begin(), expected.end());
        ASSERT_EQ(result, expected);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FieldsComparator> key_comparator_;
};

TEST_F(SplitGeneratorTest, TestAppend) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateDataFileMeta("1", 11, 0, 20),  CreateDataFileMeta("2", 13, 21, 30),
        CreateDataFileMeta("3", 46, 31, 40), CreateDataFileMeta("4", 23, 41, 50),
        CreateDataFileMeta("5", 4, 51, 60),  CreateDataFileMeta("6", 101, 61, 100)};
    {
        auto tmp_files = files;
        AppendOnlySplitGenerator split_generator(/*target_split_size=*/40, /*open_file_cost=*/2,
                                                 BucketMode::HASH_FIXED);
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForBatch(std::move(tmp_files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2"}, {"3"}, {"4", "5"}, {"6"}};
        CheckResult(split_groups, expected);
    }
    {
        auto tmp_files = files;
        AppendOnlySplitGenerator split_generator(/*target_split_size=*/70, /*open_file_cost=*/2,
                                                 BucketMode::HASH_FIXED);
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForBatch(std::move(tmp_files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2", "3"}, {"4", "5"}, {"6"}};
        CheckResult(split_groups, expected);
    }
    {
        auto tmp_files = files;
        AppendOnlySplitGenerator split_generator(/*target_split_size=*/40, /*open_file_cost=*/20,
                                                 BucketMode::HASH_FIXED);
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForBatch(std::move(tmp_files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2"}, {"3"}, {"4"}, {"5"}, {"6"}};
        CheckResult(split_groups, expected);
    }
    {
        auto tmp_files = files;
        AppendOnlySplitGenerator split_generator(/*target_split_size=*/40, /*open_file_cost=*/2,
                                                 BucketMode::BUCKET_UNAWARE);
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForStreaming(std::move(tmp_files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2"}, {"3"}, {"4", "5"}, {"6"}};
        CheckResult(split_groups, expected);
    }
    {
        auto tmp_files = files;
        AppendOnlySplitGenerator split_generator(/*target_split_size=*/40, /*open_file_cost=*/2,
                                                 BucketMode::HASH_FIXED);
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForStreaming(std::move(tmp_files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2", "3", "4", "5", "6"}};
        CheckResult(split_groups, expected);
    }
}

TEST_F(SplitGeneratorTest, TestMergeTree) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateDataFileMeta("1", 0, 10),  CreateDataFileMeta("2", 0, 12),
        CreateDataFileMeta("3", 15, 60), CreateDataFileMeta("4", 18, 40),
        CreateDataFileMeta("5", 82, 85), CreateDataFileMeta("6", 100, 200)};
    {
        auto tmp_files = files;
        MergeTreeSplitGenerator split_generator(/*target_split_size=*/100, /*open_file_cost=*/2,
                                                /*deletion_vectors_enabled=*/false,
                                                MergeEngine::DEDUPLICATE, key_comparator_);
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForBatch(std::move(tmp_files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2", "3", "4", "5"}, {"6"}};
        CheckResult(split_groups, expected);
    }
    {
        auto tmp_files = files;
        MergeTreeSplitGenerator split_generator(/*target_split_size=*/100, /*open_file_cost=*/30,
                                                /*deletion_vectors_enabled=*/false,
                                                MergeEngine::DEDUPLICATE, key_comparator_);
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForBatch(std::move(tmp_files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2", "3", "4"}, {"5"}, {"6"}};
        CheckResult(split_groups, expected);
    }
}

TEST_F(SplitGeneratorTest, TestSplitRawConvertible) {
    MergeTreeSplitGenerator split_generator(/*target_split_size=*/100, /*open_file_cost=*/2,
                                            /*deletion_vectors_enabled=*/false,
                                            MergeEngine::DEDUPLICATE, key_comparator_);
    {
        // When level0 exists, should not be rawConvertible
        std::vector<std::shared_ptr<DataFileMeta>> files = {
            CreateDataFileMeta("1", 0, 0, 10, 10l), CreateDataFileMeta("2", 0, 10, 20, 20l)};
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForBatch(std::move(files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2"}};
        std::vector<bool> expected_raw_convertible = {false};
        CheckResult(split_groups, expected, expected_raw_convertible);
    }
    {
        // When deleteRowCount > 0, should not be rawConvertible
        std::vector<std::shared_ptr<DataFileMeta>> files = {
            CreateDataFileMeta("1", 1, 0, 10, 10l, /*delete_row_count=*/1l),
            CreateDataFileMeta("2", 1, 10, 20, 20l)};
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForBatch(std::move(files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2"}};
        std::vector<bool> expected_raw_convertible = {false};
        CheckResult(split_groups, expected, expected_raw_convertible);
    }
    {
        // No level0 and deleteRowCount == 0:
        // All in one level, should be rawConvertible
        std::vector<std::shared_ptr<DataFileMeta>> files = {
            CreateDataFileMeta("1", 1, 0, 10, 10l), CreateDataFileMeta("2", 1, 10, 20, 20l)};
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForBatch(std::move(files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2"}};
        std::vector<bool> expected_raw_convertible = {true};
        CheckResult(split_groups, expected, expected_raw_convertible);
    }
    {
        // Not all in one level, should not be rawConvertible
        std::vector<std::shared_ptr<DataFileMeta>> files = {
            CreateDataFileMeta("1", 1, 0, 10, 10l), CreateDataFileMeta("2", 2, 10, 20, 20l)};
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForBatch(std::move(files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2"}};
        std::vector<bool> expected_raw_convertible = {false};
        CheckResult(split_groups, expected, expected_raw_convertible);
    }
    {
        // Not all in one level but with deletion vectors enabled, should be rawConvertible
        std::vector<std::shared_ptr<DataFileMeta>> files = {
            CreateDataFileMeta("1", 1, 0, 10, 10l), CreateDataFileMeta("2", 2, 10, 20, 20l)};
        MergeTreeSplitGenerator split_generator_dv(/*target_split_size=*/100, /*open_file_cost=*/2,
                                                   /*deletion_vectors_enabled=*/true,
                                                   MergeEngine::DEDUPLICATE, key_comparator_);
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator_dv.SplitForBatch(std::move(files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2"}};
        std::vector<bool> expected_raw_convertible = {true};
        CheckResult(split_groups, expected, expected_raw_convertible);
    }
    {
        // Not all in one level but with first row merge engine, should be rawConvertible
        std::vector<std::shared_ptr<DataFileMeta>> files = {
            CreateDataFileMeta("1", 1, 0, 10, 10l), CreateDataFileMeta("2", 2, 10, 20, 20l)};
        MergeTreeSplitGenerator split_generator_dv(/*target_split_size=*/100, /*open_file_cost=*/2,
                                                   /*deletion_vectors_enabled=*/false,
                                                   MergeEngine::FIRST_ROW, key_comparator_);
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator_dv.SplitForBatch(std::move(files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2"}};
        std::vector<bool> expected_raw_convertible = {true};
        CheckResult(split_groups, expected, expected_raw_convertible);
    }
    {
        // Split with one file should be rawConvertible
        std::vector<std::shared_ptr<DataFileMeta>> files = {
            CreateDataFileMeta("1", 1, 0, 10, 10L),  CreateDataFileMeta("2", 2, 0, 12, 12L),
            CreateDataFileMeta("3", 3, 15, 60, 60L), CreateDataFileMeta("4", 4, 18, 40, 40L),
            CreateDataFileMeta("5", 5, 82, 85, 85L), CreateDataFileMeta("6", 6, 100, 200, 200L)};
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForBatch(std::move(files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2", "3", "4", "5"}, {"6"}};
        std::vector<bool> expected_raw_convertible = {false, true};
        CheckResult(split_groups, expected, expected_raw_convertible);
    }
    {
        // test convertible for old version
        std::vector<std::shared_ptr<DataFileMeta>> files = {
            CreateDataFileMeta("1", 1, 0, 10, 10l, std::nullopt),
            CreateDataFileMeta("2", 1, 10, 20, 20l, std::nullopt)};
        ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                             split_generator.SplitForBatch(std::move(files)));
        std::vector<std::vector<std::string>> expected = {{"1", "2"}};
        std::vector<bool> expected_raw_convertible = {true};
        CheckResult(split_groups, expected, expected_raw_convertible);
    }
}

TEST_F(SplitGeneratorTest, TestMergeTreeSplitRawConvertible) {
    MergeTreeSplitGenerator split_generator(/*target_split_size=*/100, /*open_file_cost=*/2,
                                            /*deletion_vectors_enabled=*/false,
                                            MergeEngine::DEDUPLICATE, key_comparator_);
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateDataFileMeta("1", 0, 0, 10, 10L),     CreateDataFileMeta("2", 0, 0, 12, 12L),
        CreateDataFileMeta("3", 0, 13, 20, 20L),    CreateDataFileMeta("4", 0, 21, 200, 200L),
        CreateDataFileMeta("5", 0, 201, 210, 210L), CreateDataFileMeta("6", 0, 211, 220, 220L)};
    ASSERT_OK_AND_ASSIGN(std::vector<SplitGenerator::SplitGroup> split_groups,
                         split_generator.SplitForBatch(std::move(files)));
    std::vector<std::vector<std::string>> expected = {{"1", "2", "3"}, {"4"}, {"5", "6"}};
    std::vector<bool> expected_raw_convertible = {false, true, false};
    CheckResult(split_groups, expected, expected_raw_convertible);
}

}  // namespace paimon::test
