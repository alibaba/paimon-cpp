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

#include "paimon/core/mergetree/sorted_run.h"

#include <optional>
#include <string>
#include <variant>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/mergetree/level_sorted_run.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
class SortedRunTest : public testing::Test {
 public:
    std::shared_ptr<DataFileMeta> CreateDataFileMeta(int32_t min_key, int32_t max_key) {
        auto pool = GetDefaultPool();
        return std::make_shared<DataFileMeta>(
            "fake.orc", /*file_size=*/1165, /*row_count=*/1,
            /*min_key=*/BinaryRowGenerator::GenerateRow({min_key}, pool.get()), /*max_key=*/
            BinaryRowGenerator::GenerateRow({max_key}, pool.get()),
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
    }
};

TEST_F(SortedRunTest, TestSortedRunIsValid) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FieldsComparator> comparator,
        FieldsComparator::Create({DataField(0, arrow::field("test", arrow::int32()))},
                                 /*is_ascending_order=*/true));

    // m1 [10, 20]
    auto m1 = CreateDataFileMeta(10, 20);

    // m2 [30, 40]
    auto m2 = CreateDataFileMeta(30, 40);

    // m3 [15, 35]
    auto m3 = CreateDataFileMeta(15, 35);
    {
        auto sorted_run = SortedRun::FromSingle(m1);
        ASSERT_TRUE(sorted_run.IsValid(comparator));
    }
    {
        auto sorted_run = SortedRun::FromSorted({m1, m2});
        ASSERT_TRUE(sorted_run.IsValid(comparator));
    }
    {
        auto sorted_run = SortedRun::FromSorted({m1, m3});
        ASSERT_FALSE(sorted_run.IsValid(comparator));
    }
    {
        auto sorted_run = SortedRun::FromSorted({m3, m2});
        ASSERT_FALSE(sorted_run.IsValid(comparator));
    }
}

TEST_F(SortedRunTest, TestFromUnsorted) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FieldsComparator> comparator,
        FieldsComparator::Create({DataField(0, arrow::field("test", arrow::int32()))},
                                 /*is_ascending_order=*/true));

    // m1 [10, 20]
    auto m1 = CreateDataFileMeta(10, 20);

    // m2 [30, 40]
    auto m2 = CreateDataFileMeta(30, 40);

    // m3 [15, 35]
    auto m3 = CreateDataFileMeta(15, 35);

    ASSERT_OK_AND_ASSIGN(auto run1, SortedRun::FromUnsorted({m2, m1}, comparator));
    auto run2 = SortedRun::FromSorted({m1, m2});
    ASSERT_EQ(run1, run2);

    ASSERT_NOK_WITH_MSG(SortedRun::FromUnsorted({m2, m1, m3}, comparator),
                        "from unsorted validate failed");
}

TEST_F(SortedRunTest, TestEqual) {
    auto empty = SortedRun::Empty();
    auto m1 = CreateDataFileMeta(10, 20);
    auto run1 = SortedRun::FromSingle({m1});
    auto other_run1 = SortedRun::FromSingle({m1});

    auto m2 = CreateDataFileMeta(100, 200);
    auto run2 = SortedRun::FromSingle({m2});

    ASSERT_EQ(run1, run1);
    ASSERT_EQ(run1, other_run1);
    ASSERT_FALSE(run1 == run2);
    ASSERT_FALSE(empty == run2);
}

TEST_F(SortedRunTest, TestSortedRunToString) {
    auto m1 = CreateDataFileMeta(10, 20);
    auto m2 = CreateDataFileMeta(30, 40);
    auto sorted_run = SortedRun::FromSorted({m1, m2});
    auto sorted_run_str = sorted_run.ToString();
    LevelSortedRun level_sorted_run(/*level=*/10, sorted_run);
    auto level_sorted_run_str = level_sorted_run.ToString();
    ASSERT_TRUE(level_sorted_run_str.find("LevelSortedRun{ level=10, run={fileName:") !=
                std::string::npos);
    ASSERT_TRUE(level_sorted_run_str.find(sorted_run_str) != std::string::npos);
}

}  // namespace paimon::test
