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

#include "paimon/core/catalog/commit_table_request.h"

#include <cstdint>
#include <map>
#include <optional>

#include "gtest/gtest.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(CommitTableRequestTest, TestSimple) {
    Snapshot snapshot(
        /*version=*/3, /*id=*/1, /*schema_id=*/0,
        /*base_manifest_list=*/"manifest-list-3879e56f-2f27-49ae-a2f3-3dcbb8eb0beb-0",
        /*base_manifest_list_size=*/291,
        /*delta_manifest_list=*/"manifest-list-3879e56f-2f27-49ae-a2f3-3dcbb8eb0beb-1",
        /*delta_manifest_list_size=*/1342, /*changelog_manifest_list=*/std::nullopt,
        /*changelog_manifest_list_size=*/std::nullopt, /*index_manifest=*/std::nullopt,
        /*commit_user=*/"commit_user_1", /*commit_identifier=*/9223372036854775807,
        /*commit_kind=*/Snapshot::CommitKind::Append(), /*time_millis=*/1758097357597,
        /*log_offsets=*/std::map<int32_t, int64_t>(), /*total_record_count=*/5,
        /*delta_record_count=*/5, /*changelog_record_count=*/0, /*watermark=*/std::nullopt,
        /*statistics=*/std::nullopt, /*properties=*/std::nullopt, /*next_row_id=*/0);
    std::vector<PartitionStatistics> partition_statistics = {
        PartitionStatistics(/*spec=*/{{"f1", "20"}}, /*record_count=*/1, /*file_size_in_bytes=*/541,
                            /*file_count=*/1, /*last_file_creation_time=*/1724090888743,
                            /*total_buckets=*/-1),
        PartitionStatistics(/*spec=*/{{"f1", "10"}}, /*record_count=*/4,
                            /*file_size_in_bytes=*/1118, /*file_count=*/2,
                            /*last_file_creation_time=*/1724090888727, /*total_buckets=*/-1)};
    std::string expected_request_str = R"({
    "snapshot": {
        "version": 3,
        "id": 1,
        "schemaId": 0,
        "baseManifestList": "manifest-list-3879e56f-2f27-49ae-a2f3-3dcbb8eb0beb-0",
        "baseManifestListSize": 291,
        "deltaManifestList": "manifest-list-3879e56f-2f27-49ae-a2f3-3dcbb8eb0beb-1",
        "deltaManifestListSize": 1342,
        "changelogManifestList": null,
        "commitUser": "commit_user_1",
        "commitIdentifier": 9223372036854775807,
        "commitKind": "APPEND",
        "timeMillis": 1758097357597,
        "logOffsets": {},
        "totalRecordCount": 5,
        "deltaRecordCount": 5,
        "changelogRecordCount": 0,
        "nextRowId": 0
    },
    "statistics": [
        {
            "spec": {
                "f1": "20"
            },
            "recordCount": 1,
            "fileSizeInBytes": 541,
            "fileCount": 1,
            "lastFileCreationTime": 1724090888743,
            "totalBuckets": -1
        },
        {
            "spec": {
                "f1": "10"
            },
            "recordCount": 4,
            "fileSizeInBytes": 1118,
            "fileCount": 2,
            "lastFileCreationTime": 1724090888727,
            "totalBuckets": -1
        }
    ]
})";

    CommitTableRequest request1(snapshot, partition_statistics);
    ASSERT_OK_AND_ASSIGN(std::string request_str1, request1.ToJsonString());
    ASSERT_EQ(request_str1, expected_request_str);
    ASSERT_OK_AND_ASSIGN(CommitTableRequest request2,
                         CommitTableRequest::FromJsonString(request_str1));
    ASSERT_EQ(request1, request2);
    ASSERT_OK_AND_ASSIGN(std::string request_str2, request2.ToJsonString());
    ASSERT_EQ(request_str1, request_str2);
}

}  // namespace paimon::test
