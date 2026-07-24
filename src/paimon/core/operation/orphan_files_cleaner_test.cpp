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

#include "paimon/orphan_files_cleaner.h"

#include <filesystem>
#include <limits>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/operation/orphan_files_cleaner_impl.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(OrphanFilesCleanerTest, TestSupportToClean) {
    ASSERT_TRUE(
        OrphanFilesCleanerImpl::SupportToClean("data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.orc"));
    ASSERT_TRUE(OrphanFilesCleanerImpl::SupportToClean(
        "data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.parquet"));
    ASSERT_TRUE(
        OrphanFilesCleanerImpl::SupportToClean("data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.avro"));
    ASSERT_TRUE(OrphanFilesCleanerImpl::SupportToClean(
        "data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.parquet"));
    ASSERT_TRUE(
        OrphanFilesCleanerImpl::SupportToClean("manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-0"));
    ASSERT_TRUE(OrphanFilesCleanerImpl::SupportToClean(
        "manifest-list-469f3a0f-f6f1-4027-91bf-d1e897e8ea23-1"));
    ASSERT_TRUE(OrphanFilesCleanerImpl::SupportToClean(
        ".snapshot-2.13c988c3-784d-493d-8884-016ddddb1fc2.tmp"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean("tmp"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean("snapshot-1"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean("schema-0"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean("bucket-0"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean(
        "changelog-ce64d06d-c4cd-456b-a1b3-ae570042620f-0.parquet"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean(
        "data-5515726b-0f0f-4556-a942-e795e9f94c4a-0.orc.index"));
    ASSERT_FALSE(
        OrphanFilesCleanerImpl::SupportToClean("index-aa60193d-d7cd-434f-bc1a-c1adb210e1f7-0"));
    ASSERT_FALSE(
        OrphanFilesCleanerImpl::SupportToClean("data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.json"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean(
        "some_data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.orc"));
}

TEST(OrphanFilesCleanerTest, TestPkTable) {
    std::string table_path =
        paimon::test::GetDataDir() + "/orc/pk_table_with_mor.db/pk_table_with_mor/";
    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.AddOption(Options::FILE_SYSTEM, "local").Finish());
    ASSERT_NOK_WITH_MSG(OrphanFilesCleaner::Create(std::move(clean_context)),
                        "orphan files cleaner only support append table");
}

TEST(OrphanFilesCleanerTest, TestTableWithTag) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    auto file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK(file_system->WriteFile(PathUtil::JoinPath(table_path, "tag/tag-1"), " ", true));

    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.AddOption(Options::FILE_SYSTEM, "local").Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_NOK_WITH_MSG(cleaner->Clean(),
                        "OrphanFilesCleaner do not support cleaning table with tag");
}

TEST(OrphanFilesCleanerTest, TestTableWithBranch) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    auto file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK(file_system->WriteFile(PathUtil::JoinPath(table_path, "branch/branch-1"), " ", true));

    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.AddOption(Options::FILE_SYSTEM, "local").Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_NOK_WITH_MSG(cleaner->Clean(),
                        "OrphanFilesCleaner do not support cleaning table with branch");
}

TEST(OrphanFilesCleanerTest, TestTableWithIndex) {
    std::string test_data_path =
        paimon::test::GetDataDir() + "/orc/append_with_bsi.db/append_with_bsi/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    auto file_system = std::make_shared<LocalFileSystem>();

    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                             .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
    ASSERT_TRUE(cleaned_paths.empty());
}

TEST(OrphanFilesCleanerTest, TestTableWithBrokenSnapshot) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto check_result = [](const std::set<std::string>& actual,
                           const std::set<std::string>& expected) -> bool {
        std::set<std::string> file_names;
        for (const auto& file_path : actual) {
            file_names.insert(PathUtil::GetName(file_path));
        }
        return file_names == expected;
    };

    // test with non-exist manifest list, which manifest has reference by other manifest-list, so it
    // will not be cleaned
    {
        auto dir = UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
        ASSERT_OK(file_system->Delete(PathUtil::JoinPath(
            table_path, "manifest/manifest-list-616d1847-a02c-495f-9cca-2c8b7def0fec-1")));

        CleanContextBuilder clean_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                             clean_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                                 .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                                 .Finish());
        ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
        ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
        ASSERT_TRUE(cleaned_paths.empty());
    }
    // test with non-exist manifest list, which manifest has no other reference, so it will be
    // cleaned
    {
        auto dir = UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
        ASSERT_OK(file_system->Delete(PathUtil::JoinPath(
            table_path, "manifest/manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-3")));

        CleanContextBuilder clean_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                             clean_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                                 .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                                 .Finish());
        ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
        ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
        ASSERT_TRUE(
            check_result(cleaned_paths, {"data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc",
                                         "manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-1"}));
    }
    // test with non-exist manifest
    {
        auto dir = UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
        ASSERT_OK(file_system->Delete(PathUtil::JoinPath(
            table_path, "manifest/manifest-f8b15cfc-437a-4d21-a6a0-e45b639ae7ed-0")));

        CleanContextBuilder clean_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                             clean_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                                 .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                                 .Finish());
        ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
        ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
        ASSERT_TRUE(
            check_result(cleaned_paths, {"data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc",
                                         "data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc"}));
    }
    // test with non-exist data file
    {
        std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
        auto dir = UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
        auto file_system = std::make_shared<LocalFileSystem>();
        ASSERT_OK(file_system->Delete(PathUtil::JoinPath(
            table_path, "f1=10/bucket-0/data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc")));

        CleanContextBuilder clean_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                             clean_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                                 .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                                 .Finish());
        ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
        ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
        ASSERT_TRUE(cleaned_paths.empty());
    }
}

TEST(OrphanFilesCleanerTest, TestTableWithChangelog) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    auto file_system = std::make_shared<LocalFileSystem>();
    auto snapshot_str = R"({
  "version" : 3,
  "id" : 6,
  "schemaId" : 0,
  "baseManifestList" : "manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-0",
  "deltaManifestList" : "manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-1",
  "changelogManifestList" : "manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-2",
  "commitUser" : "febb1e71-79fc-4abc-9b9d-464ecbc198f7",
  "commitIdentifier" : 9223372036854775807,
  "commitKind" : "APPEND",
  "timeMillis" : 1721615035363,
  "totalRecordCount" : 11,
  "deltaRecordCount" : 1,
  "changelogRecordCount" : 0
})";
    ASSERT_OK(file_system->WriteFile(PathUtil::JoinPath(table_path, "snapshot/snapshot-6"),
                                     snapshot_str, true));

    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.AddOption(Options::FILE_SYSTEM, "local").Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_NOK_WITH_MSG(cleaner->Clean(), "OrphanFilesCleaner do not support clean changelog");
}

TEST(OrphanFilesCleanerTest, TestTableWithIndexManifest) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    auto file_system = std::make_shared<LocalFileSystem>();
    auto snapshot_str = R"({
  "version" : 3,
  "id" : 6,
  "schemaId" : 0,
  "baseManifestList" : "manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-0",
  "deltaManifestList" : "manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-1",
  "changelogManifestList" : null,
  "indexManifest" : "index-manifest-bd43150e-cce1-4231-bfc1-8fdc2b0b5994-0",
  "commitUser" : "febb1e71-79fc-4abc-9b9d-464ecbc198f7",
  "commitIdentifier" : 9223372036854775807,
  "commitKind" : "APPEND",
  "timeMillis" : 1721615035363,
  "totalRecordCount" : 11,
  "deltaRecordCount" : 1,
  "changelogRecordCount" : 0
})";
    ASSERT_OK(file_system->WriteFile(PathUtil::JoinPath(table_path, "snapshot/snapshot-6"),
                                     snapshot_str, true));

    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.AddOption(Options::FILE_SYSTEM, "local").Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_NOK_WITH_MSG(cleaner->Clean(), "OrphanFilesCleaner do not support clean index manifest");
}

}  // namespace paimon::test
