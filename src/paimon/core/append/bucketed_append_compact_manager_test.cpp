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

#include "paimon/core/append/bucketed_append_compact_manager.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/executor.h"
#include "paimon/result.h"

namespace paimon::test {

class BucketedAppendCompactManagerTest : public testing::Test {
 public:
    void SetUp() override {
        executor_ = CreateDefaultExecutor();
    }

    std::vector<std::shared_ptr<DataFileMeta>> GenerateDataFileMeta() {
        std::vector<std::shared_ptr<DataFileMeta>> metas;
        metas.push_back(DataFileMeta::ForAppend("file1", 100, 100, SimpleStats::EmptyStats(),
                                                /*min_sequence_number=*/1,
                                                /*max_sequence_number=*/10, 0, FileSource::Append(),
                                                std::nullopt, std::nullopt, std::nullopt,
                                                std::nullopt)
                            .value());
        metas.push_back(DataFileMeta::ForAppend("file2", 200, 200, SimpleStats::EmptyStats(),
                                                /*min_sequence_number=*/5,
                                                /*max_sequence_number=*/15, 0, FileSource::Append(),
                                                std::nullopt, std::nullopt, std::nullopt,
                                                std::nullopt)
                            .value());
        metas.push_back(DataFileMeta::ForAppend("file3", 200, 200, SimpleStats::EmptyStats(),
                                                /*min_sequence_number=*/20,
                                                /*max_sequence_number=*/30, 0, FileSource::Append(),
                                                std::nullopt, std::nullopt, std::nullopt,
                                                std::nullopt)
                            .value());
        return metas;
    }

 private:
    void InnerTest(const std::vector<std::shared_ptr<DataFileMeta>>& to_compact_before_pick,
                   bool expected_present,
                   const std::vector<std::shared_ptr<DataFileMeta>>& expected_compact_before,
                   const std::vector<std::shared_ptr<DataFileMeta>>& to_compact_after_pick) {
        int min_file_num = 4;
        int64_t target_file_size = 1024;
        int64_t threshold = target_file_size / 10 * 7;
        BucketedAppendCompactManager manager(
            executor_, to_compact_before_pick,
            /*dv_maintainer=*/nullptr, min_file_num, target_file_size, threshold,
            /*force_rewrite_all_files=*/false, /*compact_rewriter=*/nullptr);
        auto actual = manager.PickCompactBefore();
        if (expected_present) {
            ASSERT_TRUE(actual.has_value());
            ExpectVectorsEqual(actual.value(), expected_compact_before);
        } else {
            ASSERT_FALSE(actual.has_value());
        }
        auto pq = manager.GetToCompact();
        std::vector<std::shared_ptr<DataFileMeta>> to_compact;
        while (!pq.empty()) {
            to_compact.push_back(pq.top());
            pq.pop();
        }
        ExpectVectorsEqual(to_compact, to_compact_after_pick);
    }

    std::shared_ptr<DataFileMeta> NewFile(int64_t min_sequence_number,
                                          int64_t max_sequence_number) {
        return std::make_shared<DataFileMeta>(
            /*file_name=*/"", /*file_size=*/max_sequence_number - min_sequence_number + 1,
            /*row_count=*/0,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            /*value_stats=*/SimpleStats::EmptyStats(), min_sequence_number, max_sequence_number,
            /*schema_id=*/0,
            /*level=*/DataFileMeta::DUMMY_LEVEL,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1724090888706ll, 0),
            /*delete_row_count=*/max_sequence_number - min_sequence_number + 1,
            /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    }

    void ExpectVectorsEqual(const std::vector<std::shared_ptr<DataFileMeta>>& actual,
                            const std::vector<std::shared_ptr<DataFileMeta>>& expected) {
        EXPECT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            EXPECT_EQ(*actual[i], *expected[i]);
        }
    }

    std::shared_ptr<Executor> executor_;
};

TEST_F(BucketedAppendCompactManagerTest, TestFileComparatorWithoutOverlap) {
    auto files = GenerateDataFileMeta();
    auto& file1 = files[0];
    auto& file2 = files[1];
    auto& file3 = files[2];

    auto comparator = BucketedAppendCompactManager::FileComparator(false);
    EXPECT_TRUE(comparator(file1, file2));
    EXPECT_FALSE(comparator(file2, file1));
    EXPECT_TRUE(comparator(file1, file3));
    EXPECT_FALSE(comparator(file3, file1));
}

TEST_F(BucketedAppendCompactManagerTest, TestFileComparatorWithOverlap) {
    auto files = GenerateDataFileMeta();
    auto& file1 = files[0];
    auto& file2 = files[1];
    auto& file3 = files[2];

    auto comparator = BucketedAppendCompactManager::FileComparator(true);
    EXPECT_TRUE(comparator(file1, file2));
    EXPECT_FALSE(comparator(file2, file1));
    EXPECT_TRUE(comparator(file1, file3));
    EXPECT_FALSE(comparator(file3, file1));
}

TEST_F(BucketedAppendCompactManagerTest, TestIsOverlap) {
    auto files = GenerateDataFileMeta();
    auto& file1 = files[0];
    auto& file2 = files[1];
    auto& file3 = files[2];

    EXPECT_TRUE(BucketedAppendCompactManager::IsOverlap(file1, file2));
    EXPECT_FALSE(BucketedAppendCompactManager::IsOverlap(file1, file3));
    EXPECT_FALSE(BucketedAppendCompactManager::IsOverlap(file2, file3));
}

TEST_F(BucketedAppendCompactManagerTest, TestPickEmptyAndNotRelease) {
    // 1~50 is small enough, so hold it
    std::vector<std::shared_ptr<DataFileMeta>> to_compact = {NewFile(1, 50)};
    InnerTest(to_compact, /*expected_present=*/false, /*expected_compact_before=*/{}, to_compact);
}

TEST_F(BucketedAppendCompactManagerTest, TestPickEmptyAndRelease) {
    // large file, release
    InnerTest(/*to_compact_before_pick=*/{NewFile(1, 2048)}, /*expected_present=*/false,
              /*expected_compact_before=*/{}, /*to_compact_after_pick=*/{});

    // small file at last, release previous
    InnerTest(/*to_compact_before_pick=*/{NewFile(1, 2048), NewFile(2049, 2100)},
              /*expected_present=*/false,
              /*expected_compact_before=*/{}, /*to_compact_after_pick=*/{NewFile(2049, 2100)});
    InnerTest(
        /*to_compact_before_pick=*/{NewFile(1, 2048), NewFile(2049, 2100), NewFile(2101, 2110)},
        /*expected_present=*/false,
        /*expected_compact_before=*/{},
        /*to_compact_after_pick=*/{NewFile(2049, 2100), NewFile(2101, 2110)});
    InnerTest(
        /*to_compact_before_pick=*/{NewFile(1, 2048), NewFile(2049, 4096), NewFile(4097, 5000)},
        /*expected_present=*/false,
        /*expected_compact_before=*/{}, /*to_compact_after_pick=*/{NewFile(4097, 5000)});
    InnerTest(
        /*to_compact_before_pick=*/{NewFile(1, 1024), NewFile(1025, 2049), NewFile(2050, 2500),
                                    NewFile(2501, 4096), NewFile(4097, 6000), NewFile(6001, 7000),
                                    NewFile(7001, 7600)},
        /*expected_present=*/false,
        /*expected_compact_before=*/{},
        /*to_compact_after_pick=*/{NewFile(6001, 7000), NewFile(7001, 7600)});

    // ignore single small file (in the middle)
    InnerTest(
        /*to_compact_before_pick=*/{NewFile(1, 2048), NewFile(2049, 4096), NewFile(4097, 4100),
                                    NewFile(4101, 6150)},
        /*expected_present=*/false,
        /*expected_compact_before=*/{},
        /*to_compact_after_pick=*/{NewFile(4101, 6150)});
    InnerTest(
        /*to_compact_before_pick=*/{NewFile(1, 2048), NewFile(2049, 4096), NewFile(4097, 5000),
                                    NewFile(5001, 6144), NewFile(6145, 7048)},
        /*expected_present=*/false,
        /*expected_compact_before=*/{}, /*to_compact_after_pick=*/{NewFile(6145, 7048)});

    // wait for more file
    InnerTest(/*to_compact_before_pick=*/{NewFile(1, 500), NewFile(501, 1000)},
              /*expected_present=*/false,
              /*expected_compact_before=*/{},
              /*to_compact_after_pick=*/{NewFile(1, 500), NewFile(501, 1000)});

    InnerTest(/*to_compact_before_pick=*/{NewFile(1, 500), NewFile(501, 1000), NewFile(1001, 2048)},
              /*expected_present=*/false,
              /*expected_compact_before=*/{},
              /*to_compact_after_pick=*/{NewFile(501, 1000), NewFile(1001, 2048)});
    InnerTest(
        /*to_compact_before_pick=*/{NewFile(1, 2050), NewFile(2051, 2100), NewFile(2101, 2110)},
        /*expected_present=*/false,
        /*expected_compact_before=*/{},
        /*to_compact_after_pick=*/{NewFile(2051, 2100), NewFile(2101, 2110)});
}

}  // namespace paimon::test
