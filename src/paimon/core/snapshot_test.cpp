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

#include "paimon/core/snapshot.h"

#include "gtest/gtest.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/result.h"
#include "paimon/snapshot/snapshot_info.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class SnapshotTest : public testing::Test {
 public:
    std::string ReplaceAll(const std::string& str) {
        std::string replaced_str = StringUtils::Replace(str, " ", "");
        replaced_str = StringUtils::Replace(replaced_str, "\t", "");
        replaced_str = StringUtils::Replace(replaced_str, "\n", "");
        return replaced_str;
    }
};

TEST_F(SnapshotTest, TestSimple) {
    std::map<int32_t, int64_t> log_offset = {{25, 30}};
    std::map<std::string, std::string> properties = {{"key1", "value1"}, {"key2", "value2"}};
    Snapshot snapshot(
        /*version=*/5, /*id=*/10, /*schema_id=*/15, /*base_manifest_list=*/"base_manifest_list", 10,
        /*delta_manifest_list=*/"delta_manifest_list", 20,
        /*changelog_manifest_list=*/"changelog_manifest_list", 30,
        /*index_manifest=*/"index_manifest",
        /*commit_user=*/"commit_user_01", /*commit_identifier=*/20,
        /*commit_kind=*/Snapshot::CommitKind::Compact(), /*time_millis=*/1234, log_offset,
        /*total_record_count=*/35,
        /*delta_record_count=*/40, /*changelog_record_count=*/45, /*watermark=*/50,
        /*statistics=*/"statistic_test", properties, /*next_row_id=*/0);
    ASSERT_EQ(5, snapshot.Version());
    ASSERT_EQ(10, snapshot.Id());
    ASSERT_EQ(15, snapshot.SchemaId());
    ASSERT_EQ("base_manifest_list", snapshot.BaseManifestList());
    ASSERT_EQ(10, snapshot.BaseManifestListSize().value());
    ASSERT_EQ("delta_manifest_list", snapshot.DeltaManifestList());
    ASSERT_EQ(20, snapshot.DeltaManifestListSize().value());
    ASSERT_EQ("changelog_manifest_list", snapshot.ChangelogManifestList().value());
    ASSERT_EQ(30, snapshot.ChangelogManifestListSize().value());
    ASSERT_EQ("index_manifest", snapshot.IndexManifest().value());
    ASSERT_EQ("commit_user_01", snapshot.CommitUser());
    ASSERT_EQ(20, snapshot.CommitIdentifier());
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot.GetCommitKind());
    ASSERT_EQ(1234, snapshot.TimeMillis());
    ASSERT_EQ(log_offset, snapshot.LogOffsets().value());
    ASSERT_EQ(35, snapshot.TotalRecordCount().value());
    ASSERT_EQ(40, snapshot.DeltaRecordCount().value());
    ASSERT_EQ(45, snapshot.ChangelogRecordCount().value());
    ASSERT_EQ(50, snapshot.Watermark().value());
    ASSERT_EQ("statistic_test", snapshot.Statistics().value());
    ASSERT_EQ(properties, snapshot.Properties().value());
    ASSERT_EQ(0, snapshot.NextRowId().value());
}

TEST_F(SnapshotTest, TestFromPath) {
    std::string data_path =
        paimon::test::GetDataDir() + "/orc/append_09.db/append_09/snapshot/snapshot-1";
    auto fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, Snapshot::FromPath(fs, data_path));
    ASSERT_EQ(3, snapshot.Version());
    ASSERT_EQ(1, snapshot.Id());
    ASSERT_EQ(0, snapshot.SchemaId());
    ASSERT_EQ("manifest-list-616d1847-a02c-495f-9cca-2c8b7def0fec-0", snapshot.BaseManifestList());
    ASSERT_EQ(std::nullopt, snapshot.BaseManifestListSize());
    ASSERT_EQ("manifest-list-616d1847-a02c-495f-9cca-2c8b7def0fec-1", snapshot.DeltaManifestList());
    ASSERT_EQ(std::nullopt, snapshot.DeltaManifestListSize());
    ASSERT_EQ(std::nullopt, snapshot.ChangelogManifestList());
    ASSERT_EQ(std::nullopt, snapshot.ChangelogManifestListSize());
    ASSERT_EQ(std::nullopt, snapshot.IndexManifest());
    ASSERT_EQ("b02e4322-9c5f-41e1-a560-c0156fdf7b9c", snapshot.CommitUser());
    ASSERT_EQ(9223372036854775807ll, snapshot.CommitIdentifier());
    ASSERT_EQ(Snapshot::CommitKind::Append(), snapshot.GetCommitKind());
    ASSERT_EQ(1721614343270ll, snapshot.TimeMillis());
    ASSERT_EQ((std::map<int32_t, int64_t>()), snapshot.LogOffsets().value());
    ASSERT_EQ(5, snapshot.TotalRecordCount().value());
    ASSERT_EQ(5, snapshot.DeltaRecordCount().value());
    ASSERT_EQ(0, snapshot.ChangelogRecordCount().value());
    ASSERT_EQ(std::nullopt, snapshot.Watermark());
    ASSERT_EQ(std::nullopt, snapshot.Statistics());
    ASSERT_EQ(std::nullopt, snapshot.Properties());
    ASSERT_EQ(std::nullopt, snapshot.NextRowId());
}

TEST_F(SnapshotTest, TestJsonizable) {
    std::string json_str = R"({
        "version" : 3,
        "id" : 1,
        "schemaId" : 0,
        "baseManifestList" : "manifest-list-d96fcc30-99e8-4f45-962b-a1157c56f378-0",
        "baseManifestListSize" : 20,
        "deltaManifestList" : "manifest-list-d96fcc30-99e8-4f45-962b-a1157c56f378-1",
        "deltaManifestListSize" : 50,
        "changelogManifestList" : null,
        "commitUser" : "0e4d92f7-53b0-40d6-a7c0-102bf3801e6a",
        "commitIdentifier" : 9223372036854775807,
        "commitKind" : "OVERWRITE",
        "timeMillis" : 1711692199281,
        "logOffsets" : { },
        "totalRecordCount" : 3,
        "deltaRecordCount" : 3,
        "changelogRecordCount" : 0
    })";

    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, Snapshot::FromJsonString(json_str));

    Snapshot expected_snapshot(
        /*version=*/3, /*id=*/1, /*schema_id=*/0, /*base_manifest_list=*/
        "manifest-list-d96fcc30-99e8-4f45-962b-a1157c56f378-0", /*base_manifest_list_size=*/20,
        /*delta_manifest_list=*/"manifest-list-d96fcc30-99e8-4f45-962b-a1157c56f378-1",
        /*delta_manifest_list_size=*/50, /*changelog_manifest_list=*/std::nullopt,
        /*changelog_manifest_list_size=*/std::nullopt, /*index_manifest=*/std::nullopt,
        /*commit_user=*/"0e4d92f7-53b0-40d6-a7c0-102bf3801e6a",
        /*commit_identifier=*/9223372036854775807ll,
        /*commit_kind=*/Snapshot::CommitKind::Overwrite(), /*time_millis=*/1711692199281ll,
        /*log_offsets=*/std::map<int32_t, int64_t>(),
        /*total_record_count=*/3, /*delta_record_count=*/3, /*changelog_record_count=*/0,
        /*watermark=*/std::nullopt, /*statistics=*/std::nullopt, /*properties=*/std::nullopt,
        /*next_row_id=*/std::nullopt);
    ASSERT_EQ(expected_snapshot, snapshot);

    ASSERT_OK_AND_ASSIGN(std::string new_json_str, snapshot.ToJsonString());
    ASSERT_EQ(ReplaceAll(json_str), ReplaceAll(new_json_str));
}

TEST_F(SnapshotTest, TestSerializeAndDeserialize) {
    auto se_and_de = [&](const std::string& data_path) {
        auto fs = std::make_shared<LocalFileSystem>();
        std::string json_str;
        ASSERT_OK(fs->ReadFile(data_path, &json_str));
        ASSERT_OK_AND_ASSIGN(Snapshot snapshot, Snapshot::FromPath(fs, data_path));
        ASSERT_EQ(snapshot, snapshot);
        ASSERT_OK_AND_ASSIGN(std::string se_json_str, snapshot.ToJsonString());
        ASSERT_EQ(ReplaceAll(json_str), ReplaceAll(se_json_str));
    };
    auto se_and_de_from_str = [&](const std::string& json_str) {
        ASSERT_OK_AND_ASSIGN(Snapshot snapshot, Snapshot::FromJsonString(json_str));
        ASSERT_EQ(snapshot, snapshot);
        ASSERT_OK_AND_ASSIGN(std::string se_json_str, snapshot.ToJsonString());
        ASSERT_EQ(ReplaceAll(json_str), ReplaceAll(se_json_str));
    };

    {
        // without indexManifest
        std::string data_path =
            paimon::test::GetDataDir() +
            "/orc/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/snapshot/snapshot-1";
        se_and_de(data_path);
    }
    {
        // with indexManifest
        std::string data_path =
            paimon::test::GetDataDir() +
            "/orc/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/snapshot/snapshot-6";
        se_and_de(data_path);
    }
    {
        // with ManifestListSize
        std::string data_path = paimon::test::GetDataDir() +
                                "/orc/append_with_bsi_bitmap_bloomfilter.db/"
                                "append_with_bsi_bitmap_bloomfilter/snapshot/snapshot-1";
        se_and_de(data_path);
    }
    {
        // with properties
        std::string json_str = R"({
          "version" : 3,
          "id" : 10,
          "schemaId" : 2,
          "baseManifestList" : "base-manifest-list-1",
          "baseManifestListSize" : 100,
          "deltaManifestList" : "delta-manifest-list-2",
          "deltaManifestListSize" : 200,
          "changelogManifestList" : null,
          "commitUser" : "commit-usr-3",
          "commitIdentifier" : 12,
          "commitKind" : "APPEND",
          "timeMillis" : 1749724197266,
          "logOffsets" : {
              "0" : 1,
              "1" : 3
          },
          "totalRecordCount" : 1024,
          "deltaRecordCount" : 4096,
          "watermark" : 1749724196266,
          "statistics" : "statistics-4",
          "properties" : {
             "key0" : "value0",
             "key1" : "value1"
          }
        })";
        se_and_de_from_str(json_str);
    }
    {
        // with next_row_id
        std::string json_str = R"({
          "version" : 3,
          "id" : 10,
          "schemaId" : 2,
          "baseManifestList" : "base-manifest-list-1",
          "baseManifestListSize" : 100,
          "deltaManifestList" : "delta-manifest-list-2",
          "deltaManifestListSize" : 200,
          "changelogManifestList" : null,
          "commitUser" : "commit-usr-3",
          "commitIdentifier" : 12,
          "commitKind" : "APPEND",
          "timeMillis" : 1749724197266,
          "logOffsets" : {
              "0" : 1,
              "1" : 3
          },
          "totalRecordCount" : 1024,
          "deltaRecordCount" : 4096,
          "watermark" : 1749724196266,
          "statistics" : "statistics-4",
          "properties" : {
             "key0" : "value0",
             "key1" : "value1"
          },
          "nextRowId" : 0
        })";
        se_and_de_from_str(json_str);
    }
}
TEST_F(SnapshotTest, TestCommitKindAnalyze) {
    // Test constructing a Snapshot with CommitKind::Analyze
    Snapshot snapshot(
        /*version=*/3, /*id=*/20, /*schema_id=*/5,
        /*base_manifest_list=*/"base-manifest-analyze",
        /*base_manifest_list_size=*/100,
        /*delta_manifest_list=*/"delta-manifest-analyze",
        /*delta_manifest_list_size=*/200,
        /*changelog_manifest_list=*/std::nullopt,
        /*changelog_manifest_list_size=*/std::nullopt,
        /*index_manifest=*/std::nullopt,
        /*commit_user=*/"analyze-user",
        /*commit_identifier=*/42,
        /*commit_kind=*/Snapshot::CommitKind::Analyze(),
        /*time_millis=*/1700000000000ll,
        /*log_offsets=*/std::map<int32_t, int64_t>(),
        /*total_record_count=*/0,
        /*delta_record_count=*/0,
        /*changelog_record_count=*/0,
        /*watermark=*/std::nullopt,
        /*statistics=*/"test-statistics",
        /*properties=*/std::nullopt,
        /*next_row_id=*/std::nullopt);

    ASSERT_EQ(Snapshot::CommitKind::Analyze(), snapshot.GetCommitKind());
    ASSERT_EQ("ANALYZE", Snapshot::CommitKind::ToString(snapshot.GetCommitKind()));
}

TEST_F(SnapshotTest, TestCommitKindAnalyzeSerializeAndDeserialize) {
    std::string json_str = R"({
        "version" : 3,
        "id" : 20,
        "schemaId" : 5,
        "baseManifestList" : "base-manifest-analyze",
        "baseManifestListSize" : 100,
        "deltaManifestList" : "delta-manifest-analyze",
        "deltaManifestListSize" : 200,
        "changelogManifestList" : null,
        "commitUser" : "analyze-user",
        "commitIdentifier" : 42,
        "commitKind" : "ANALYZE",
        "timeMillis" : 1700000000000,
        "logOffsets" : { },
        "totalRecordCount" : 0,
        "deltaRecordCount" : 0,
        "changelogRecordCount" : 0,
        "statistics" : "test-statistics"
    })";

    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, Snapshot::FromJsonString(json_str));

    // Verify deserialization
    ASSERT_EQ(20, snapshot.Id());
    ASSERT_EQ(5, snapshot.SchemaId());
    ASSERT_EQ(Snapshot::CommitKind::Analyze(), snapshot.GetCommitKind());
    ASSERT_EQ("test-statistics", snapshot.Statistics().value());

    // Verify round-trip serialization
    ASSERT_OK_AND_ASSIGN(std::string serialized, snapshot.ToJsonString());
    ASSERT_EQ(ReplaceAll(json_str), ReplaceAll(serialized));

    // Verify re-deserialization produces equal snapshot
    ASSERT_OK_AND_ASSIGN(Snapshot deserialized, Snapshot::FromJsonString(serialized));
    ASSERT_EQ(snapshot, deserialized);
}

TEST_F(SnapshotTest, TestCommitKindToStringAndFromString) {
    // Verify all CommitKind values round-trip through ToString/FromString
    ASSERT_EQ("APPEND", Snapshot::CommitKind::ToString(Snapshot::CommitKind::Append()));
    ASSERT_EQ("COMPACT", Snapshot::CommitKind::ToString(Snapshot::CommitKind::Compact()));
    ASSERT_EQ("OVERWRITE", Snapshot::CommitKind::ToString(Snapshot::CommitKind::Overwrite()));
    ASSERT_EQ("ANALYZE", Snapshot::CommitKind::ToString(Snapshot::CommitKind::Analyze()));

    ASSERT_EQ(Snapshot::CommitKind::Append(), Snapshot::CommitKind::FromString("APPEND"));
    ASSERT_EQ(Snapshot::CommitKind::Compact(), Snapshot::CommitKind::FromString("COMPACT"));
    ASSERT_EQ(Snapshot::CommitKind::Overwrite(), Snapshot::CommitKind::FromString("OVERWRITE"));
    ASSERT_EQ(Snapshot::CommitKind::Analyze(), Snapshot::CommitKind::FromString("ANALYZE"));

    // Verify equality/inequality
    ASSERT_FALSE(Snapshot::CommitKind::Analyze() == Snapshot::CommitKind::Append());
    ASSERT_FALSE(Snapshot::CommitKind::Analyze() == Snapshot::CommitKind::Compact());
    ASSERT_FALSE(Snapshot::CommitKind::Analyze() == Snapshot::CommitKind::Overwrite());
    ASSERT_TRUE(Snapshot::CommitKind::Analyze() == Snapshot::CommitKind::Analyze());
}

TEST_F(SnapshotTest, TestSnapshotInfoCommitKindToString) {
    ASSERT_EQ("APPEND", SnapshotInfo::CommitKindToString(SnapshotInfo::CommitKind::APPEND));
    ASSERT_EQ("COMPACT", SnapshotInfo::CommitKindToString(SnapshotInfo::CommitKind::COMPACT));
    ASSERT_EQ("OVERWRITE", SnapshotInfo::CommitKindToString(SnapshotInfo::CommitKind::OVERWRITE));
    ASSERT_EQ("ANALYZE", SnapshotInfo::CommitKindToString(SnapshotInfo::CommitKind::ANALYZE));
    ASSERT_EQ("UNKNOWN", SnapshotInfo::CommitKindToString(SnapshotInfo::CommitKind::UNKNOWN));
}

TEST_F(SnapshotTest, TestChangelogManifestListSerialization) {
    // Test with changelog_manifest_list set to a non-null value
    {
        std::string json_str = R"({
            "version" : 3,
            "id" : 1,
            "schemaId" : 0,
            "baseManifestList" : "base-manifest-list",
            "deltaManifestList" : "delta-manifest-list",
            "changelogManifestList" : "changelog-manifest-list",
            "changelogManifestListSize" : 42,
            "commitUser" : "user-01",
            "commitIdentifier" : 100,
            "commitKind" : "APPEND",
            "timeMillis" : 1700000000000,
            "logOffsets" : { },
            "totalRecordCount" : 10,
            "deltaRecordCount" : 5,
            "changelogRecordCount" : 3
        })";

        ASSERT_OK_AND_ASSIGN(Snapshot snapshot, Snapshot::FromJsonString(json_str));
        ASSERT_EQ("changelog-manifest-list", snapshot.ChangelogManifestList().value());
        ASSERT_EQ(42, snapshot.ChangelogManifestListSize().value());

        ASSERT_OK_AND_ASSIGN(std::string serialized, snapshot.ToJsonString());
        ASSERT_EQ(ReplaceAll(json_str), ReplaceAll(serialized));

        // Verify round-trip
        ASSERT_OK_AND_ASSIGN(Snapshot deserialized, Snapshot::FromJsonString(serialized));
        ASSERT_EQ(snapshot, deserialized);
    }

    // Test with changelog_manifest_list set to null
    {
        std::string json_str = R"({
            "version" : 3,
            "id" : 2,
            "schemaId" : 0,
            "baseManifestList" : "base-manifest-list",
            "deltaManifestList" : "delta-manifest-list",
            "changelogManifestList" : null,
            "commitUser" : "user-02",
            "commitIdentifier" : 200,
            "commitKind" : "COMPACT",
            "timeMillis" : 1700000001000,
            "logOffsets" : { },
            "totalRecordCount" : 20,
            "deltaRecordCount" : 10,
            "changelogRecordCount" : 0
        })";

        ASSERT_OK_AND_ASSIGN(Snapshot snapshot, Snapshot::FromJsonString(json_str));
        ASSERT_EQ(std::nullopt, snapshot.ChangelogManifestList());
        ASSERT_EQ(std::nullopt, snapshot.ChangelogManifestListSize());

        ASSERT_OK_AND_ASSIGN(std::string serialized, snapshot.ToJsonString());
        ASSERT_EQ(ReplaceAll(json_str), ReplaceAll(serialized));

        // Verify round-trip
        ASSERT_OK_AND_ASSIGN(Snapshot deserialized, Snapshot::FromJsonString(serialized));
        ASSERT_EQ(snapshot, deserialized);
    }
}

}  // namespace paimon::test
