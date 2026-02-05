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

#include "paimon/core/tag/tag.h"

#include "gtest/gtest.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class TagTest : public testing::Test {
 public:
    static std::string ReplaceAll(const std::string& str, const bool serialized) {
        std::string replaced_str = StringUtils::Replace(str, " ", "");
        replaced_str = StringUtils::Replace(replaced_str, "\t", "");
        replaced_str = StringUtils::Replace(replaced_str, "\n", "");
        if (serialized) {
            replaced_str = StringUtils::Replace(replaced_str, ".0", ".000000000");
        }
        return replaced_str;
    }
};

TEST_F(TagTest, TestSimple) {
    const std::map<int32_t, int64_t> log_offset = {{25, 30}};
    const std::map<std::string, std::string> properties = {{"key1", "value1"}, {"key2", "value2"}};
    const auto tag_create_time = std::vector<int64_t>({2026, 1, 2, 3, 4, 5, 6});
    const Tag tag(
        /*version=*/5, /*id=*/10, /*schema_id=*/15, /*base_manifest_list=*/"base_manifest_list", 10,
        /*delta_manifest_list=*/"delta_manifest_list", 20,
        /*changelog_manifest_list=*/"changelog_manifest_list", 30,
        /*index_manifest=*/"index_manifest",
        /*commit_user=*/"commit_user_01", /*commit_identifier=*/20,
        /*commit_kind=*/Snapshot::CommitKind::Compact(), /*time_millis=*/1234, log_offset,
        /*total_record_count=*/35,
        /*delta_record_count=*/40, /*changelog_record_count=*/45, /*watermark=*/50,
        /*statistics=*/"statistic_test", properties, /*next_row_id=*/0,
        /*tag_create_time=*/tag_create_time, /*tag_time_retained=*/5.0);
    ASSERT_EQ(5, tag.Version());
    ASSERT_EQ(10, tag.Id());
    ASSERT_EQ(15, tag.SchemaId());
    ASSERT_EQ("base_manifest_list", tag.BaseManifestList());
    ASSERT_EQ(10, tag.BaseManifestListSize().value());
    ASSERT_EQ("delta_manifest_list", tag.DeltaManifestList());
    ASSERT_EQ(20, tag.DeltaManifestListSize().value());
    ASSERT_EQ("changelog_manifest_list", tag.ChangelogManifestList().value());
    ASSERT_EQ(30, tag.ChangelogManifestListSize().value());
    ASSERT_EQ("index_manifest", tag.IndexManifest().value());
    ASSERT_EQ("commit_user_01", tag.CommitUser());
    ASSERT_EQ(20, tag.CommitIdentifier());
    ASSERT_EQ(Snapshot::CommitKind::Compact(), tag.GetCommitKind());
    ASSERT_EQ(1234, tag.TimeMillis());
    ASSERT_EQ(log_offset, tag.LogOffsets().value());
    ASSERT_EQ(35, tag.TotalRecordCount().value());
    ASSERT_EQ(40, tag.DeltaRecordCount().value());
    ASSERT_EQ(45, tag.ChangelogRecordCount().value());
    ASSERT_EQ(50, tag.Watermark().value());
    ASSERT_EQ("statistic_test", tag.Statistics().value());
    ASSERT_EQ(properties, tag.Properties().value());
    ASSERT_EQ(0, tag.NextRowId().value());
    ASSERT_EQ(tag_create_time, tag.TagCreateTime().value());
    ASSERT_EQ(5.0, tag.TagTimeRetained().value());
}

TEST_F(TagTest, TestFromPath) {
    const std::string data_path = paimon::test::GetDataDir() +
                                  "/orc/append_table_with_tag.db/append_table_with_tag/tag/tag-1";
    const auto fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(Tag tag, Tag::FromPath(fs, data_path));
    ASSERT_EQ(3, tag.Version());
    ASSERT_EQ(1, tag.Id());
    ASSERT_EQ(0, tag.SchemaId());
    ASSERT_EQ("manifest-list-616d1847-a02c-495f-9cca-2c8b7def0fec-0", tag.BaseManifestList());
    ASSERT_EQ(std::nullopt, tag.BaseManifestListSize());
    ASSERT_EQ("manifest-list-616d1847-a02c-495f-9cca-2c8b7def0fec-1", tag.DeltaManifestList());
    ASSERT_EQ(std::nullopt, tag.DeltaManifestListSize());
    ASSERT_EQ(std::nullopt, tag.ChangelogManifestList());
    ASSERT_EQ(std::nullopt, tag.ChangelogManifestListSize());
    ASSERT_EQ(std::nullopt, tag.IndexManifest());
    ASSERT_EQ("b02e4322-9c5f-41e1-a560-c0156fdf7b9c", tag.CommitUser());
    ASSERT_EQ(9223372036854775807ll, tag.CommitIdentifier());
    ASSERT_EQ(Snapshot::CommitKind::Append(), tag.GetCommitKind());
    ASSERT_EQ(1721614343270ll, tag.TimeMillis());
    ASSERT_EQ((std::map<int32_t, int64_t>()), tag.LogOffsets().value());
    ASSERT_EQ(5, tag.TotalRecordCount().value());
    ASSERT_EQ(5, tag.DeltaRecordCount().value());
    ASSERT_EQ(0, tag.ChangelogRecordCount().value());
    ASSERT_EQ(std::nullopt, tag.Watermark());
    ASSERT_EQ(std::nullopt, tag.Statistics());
    ASSERT_EQ(std::nullopt, tag.Properties());
    ASSERT_EQ(std::nullopt, tag.NextRowId());
    ASSERT_EQ(std::vector<int64_t>({2026, 2, 4, 6, 8, 10, 12}), tag.TagCreateTime());
    ASSERT_EQ(3.0, tag.TagTimeRetained());
}

TEST_F(TagTest, TestJsonizable) {
    const std::string json_str = R"({
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
        "changelogRecordCount" : 0,
        "tagCreateTime" : [ 2026, 1, 3, 5, 7, 9, 11 ],
        "tagTimeRetained" : 4.000000000
    })";

    Tag tag;
    ASSERT_OK(RapidJsonUtil::FromJsonString(json_str, &tag));

    const Tag expected_tag(
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
        /*next_row_id=*/std::nullopt,
        /*tag_create_time=*/std::vector<int64_t>({2026, 1, 3, 5, 7, 9, 11}),
        /*tag_time_retained=*/4.0);
    ASSERT_EQ(expected_tag, tag);

    ASSERT_OK_AND_ASSIGN(std::string new_json_str, tag.ToJsonString());
    ASSERT_EQ(ReplaceAll(json_str, false), ReplaceAll(new_json_str, true));
}

TEST_F(TagTest, TestSerializeAndDeserialize) {
    const auto se_and_de = [&](const std::string& data_path) {
        auto fs = std::make_shared<LocalFileSystem>();
        std::string json_str;
        ASSERT_OK(fs->ReadFile(data_path, &json_str));
        ASSERT_OK_AND_ASSIGN(Tag tag, Tag::FromPath(fs, data_path));
        ASSERT_OK_AND_ASSIGN(std::string se_json_str, tag.ToJsonString());
        ASSERT_EQ(ReplaceAll(json_str, false), ReplaceAll(se_json_str, true));
    };
    auto se_and_de_from_str = [&](const std::string& json_str) {
        Tag tag;
        ASSERT_OK(RapidJsonUtil::FromJsonString(json_str, &tag));
        ASSERT_OK_AND_ASSIGN(std::string se_json_str, tag.ToJsonString());
        ASSERT_EQ(ReplaceAll(json_str, false), ReplaceAll(se_json_str, true));
    };
    {
        const std::string data_path =
            paimon::test::GetDataDir() +
            "/orc/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/tag/tag-1";
        se_and_de(data_path);
    }
    {
        // with tagCreateTime
        const std::string json_str = R"({
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
          "tagCreateTime": [ 2026, 2, 4, 6, 8, 10, 12 ]
        })";
        se_and_de_from_str(json_str);
    }
    {
        // with tagTimeRetained
        const std::string json_str = R"({
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
          "tagTimeRetained" : 2.000000000
        })";
        se_and_de_from_str(json_str);
    }
}

}  // namespace paimon::test
