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

#include "paimon/core/partition/partition_statistics.h"

#include "gtest/gtest.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class PartitionStatisticsTest : public testing::Test {
 public:
    std::string ReplaceAll(const std::string& str) {
        std::string replaced_str = StringUtils::Replace(str, " ", "");
        replaced_str = StringUtils::Replace(replaced_str, "\t", "");
        replaced_str = StringUtils::Replace(replaced_str, "\n", "");
        return replaced_str;
    }
};

TEST_F(PartitionStatisticsTest, TestJsonizable) {
    std::string json_str = R"({
        "spec": {
            "f1": "10",
            "f2": "20"
        },
        "recordCount": 4,
        "fileSizeInBytes": 1118,
        "fileCount": 2,
        "lastFileCreationTime": 1724090888727,
        "totalBuckets": 1
    })";

    ASSERT_OK_AND_ASSIGN(PartitionStatistics partition_statistics,
                         PartitionStatistics::FromJsonString(json_str));

    PartitionStatistics expected_partition_statistics(
        /*spec=*/{{"f1", "10"}, {"f2", "20"}}, /*record_count=*/4, /*file_size_in_bytes=*/1118,
        /*file_count=*/2, /*last_file_creation_time=*/1724090888727, /*total_buckets=*/1);
    ASSERT_EQ(expected_partition_statistics, partition_statistics);

    ASSERT_OK_AND_ASSIGN(std::string new_json_str, partition_statistics.ToJsonString());
    ASSERT_EQ(ReplaceAll(json_str), ReplaceAll(new_json_str));
}

}  // namespace paimon::test
