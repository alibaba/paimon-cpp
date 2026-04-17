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

#include "paimon/core/mergetree/compact/interval_partition.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <regex>
#include <string>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class IntervalPartitionTest : public testing::Test {
 public:
    void SetUp() override {
        ASSERT_OK_AND_ASSIGN(comparator_, FieldsComparator::Create(
                                              {DataField(0, arrow::field("test", arrow::int32()))},
                                              /*is_ascending_order=*/true));
        pool_ = GetDefaultPool();
    }

 private:
    void RunTest(const std::string& in, const std::string& expected_str) const {
        IntervalPartition algorithm(ParseMetas(in), comparator_);
        std::vector<std::vector<SortedRun>> expected;
        for (const auto& line : StringUtils::Split(expected_str, "\n")) {
            expected.push_back(ParseSortedRuns(line));
        }

        std::vector<std::vector<SortedRun>> actual = algorithm.Partition();
        for (const auto& section : actual) {
            for (const auto& sorted_run : section) {
                ASSERT_TRUE(sorted_run.IsValid(comparator_));
            }
        }
        ASSERT_EQ(expected.size(), actual.size());
        for (size_t i = 0; i < expected.size(); i++) {
            auto& expected_sorted_runs = expected[i];
            auto& actual_sorted_runs = actual[i];
            ASSERT_EQ(expected_sorted_runs.size(), actual_sorted_runs.size());
            for (size_t j = 0; j < expected_sorted_runs.size(); j++) {
                auto& expected_files = expected_sorted_runs[j].Files();
                auto& actual_files = actual_sorted_runs[j].Files();
                ASSERT_EQ(expected_files.size(), actual_files.size());
                for (size_t m = 0; m < expected_files.size(); m++) {
                    ASSERT_EQ(*expected_files[m], *actual_files[m]);
                }
            }
        }
    }

    std::vector<SortedRun> ParseSortedRuns(const std::string& in) const {
        std::vector<SortedRun> sorted_runs;
        for (const auto& s : StringUtils::Split(in, "|")) {
            sorted_runs.push_back(SortedRun::FromSorted(ParseMetas(s)));
        }
        return sorted_runs;
    }

    std::vector<std::shared_ptr<DataFileMeta>> ParseMetas(const std::string& in) const {
        std::vector<std::shared_ptr<DataFileMeta>> metas;
        std::regex pattern(R"(\[(\d+?), (\d+?)\])");
        std::smatch matches;

        std::string::const_iterator search_start(in.cbegin());
        while (std::regex_search(search_start, in.cend(), matches, pattern)) {
            int32_t start = std::stoi(matches[1].str());
            int32_t end = std::stoi(matches[2].str());
            metas.push_back(MakeInterval(start, end));

            // Move to the next position in the string to continue searching
            search_start = matches.suffix().first;
        }
        return metas;
    }

    std::shared_ptr<DataFileMeta> MakeInterval(int32_t left, int32_t right) const {
        BinaryRow min_key(1);
        BinaryRowWriter min_writer(&min_key, /*initial_size=*/20, pool_.get());
        min_writer.WriteInt(0, left);
        min_writer.Complete();

        BinaryRow max_key(1);
        BinaryRowWriter max_writer(&max_key, /*initial_size=*/20, pool_.get());
        max_writer.WriteInt(0, right);
        max_writer.Complete();

        return std::make_shared<DataFileMeta>(
            "DUMMY", /*file_size=*/100, /*row_count=*/25, min_key, max_key,
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/24, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(), Timestamp(0, 0),
            /*delete_row_count=*/std::nullopt, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FieldsComparator> comparator_;
};

TEST_F(IntervalPartitionTest, TestSameMinKey) {
    RunTest("[100, 200], [100, 400], [100, 300], [100, 500]",
            "[100, 200] | [100, 300] | [100, 400] | [100, 500]");
}

TEST_F(IntervalPartitionTest, TestSameMaxKey) {
    RunTest("[100, 500], [300, 500], [200, 500], [400, 500]",
            "[100, 500] | [200, 500] | [300, 500] | [400, 500]");
}

TEST_F(IntervalPartitionTest, TestSectionPartitioning) {
    // 0    5    10   15   20   25   30
    // |--------|
    //      |-|
    //          |-----|
    //                 |-----|
    //                 |-----------|
    //                         |-------|
    // 0    5    10   15   20   25   30
    RunTest("[0, 9], [5, 7], [9, 15], [16, 22], [16, 28], [24, 32]",
            "[0, 9] | [5, 7], [9, 15]\n[16, 28] | [16, 22], [24, 32]");
}

TEST_F(IntervalPartitionTest, TestSectionPartitioning2) {
    // 0    5    10   15   20   25   30
    // |--------|
    //      |-|
    //           |----|
    //                 |-----|
    //                 |-----------|
    //                         |-------|
    // 0    5    10   15   20   25   30
    RunTest("[0, 9], [5, 7], [10, 15], [16, 22], [16, 28], [24, 32]",
            "[5, 7] | [0, 9]\n[10, 15] \n[16, 28] | [16, 22], [24, 32]");
}

TEST_F(IntervalPartitionTest, TestSectionPartitioning3) {
    RunTest("[0, 10], [2, 3], [4, 5], [15, 20]", "[2, 3], [4, 5] | [0, 10]\n[15, 20]");
}
}  // namespace paimon::test
