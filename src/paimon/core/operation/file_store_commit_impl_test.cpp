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

#include "paimon/core/operation/file_store_commit_impl.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <set>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/commit_message.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/fs/resolving_file_system.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/catalog/commit_table_request.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/manifest_committable.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/metrics/commit_metrics.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/file_utils.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/fs/local/local_file_system_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/string_builder.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"

namespace paimon::test {
class GmockFileSystem : public LocalFileSystem {
 public:
    MOCK_METHOD(Status, ReadFile, (const std::string& path, std::string* content), (override));
    MOCK_METHOD(Status, ListDir,
                (const std::string& directory,
                 std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list),
                (const, override));
    MOCK_METHOD(Status, AtomicStore, (const std::string& path, const std::string& content),
                (override));
};

class GmockFileSystemFactory : public LocalFileSystemFactory {
 public:
    const char* Identifier() const override {
        return "gmock_fs";
    }

    Result<std::unique_ptr<FileSystem>> Create(
        const std::string& path, const std::map<std::string, std::string>& options) const override {
        auto fs = std::make_unique<GmockFileSystem>();
        using ::testing::A;
        using ::testing::Invoke;

        ON_CALL(*fs, ListDir(A<const std::string&>(),
                             A<std::vector<std::unique_ptr<BasicFileStatus>>*>()))
            .WillByDefault(
                Invoke([&](const std::string& directory,
                           std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) {
                    return fs->LocalFileSystem::ListDir(directory, file_status_list);
                }));

        ON_CALL(*fs, ReadFile(A<const std::string&>(), A<std::string*>()))
            .WillByDefault(Invoke([&](const std::string& path, std::string* content) {
                return fs->FileSystem::ReadFile(path, content);
            }));

        ON_CALL(*fs, AtomicStore(A<const std::string&>(), A<const std::string&>()))
            .WillByDefault(Invoke([&](const std::string& path, const std::string& content) {
                return fs->FileSystem::AtomicStore(path, content);
            }));

        return fs;
    }
};

class FileStoreCommitImplTest : public testing::Test {
 public:
    void SetUp() override {
        auto factory_creator = paimon::FactoryCreator::GetInstance();
        factory_creator->Register("gmock_fs", (new GmockFileSystemFactory));
        dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        test_root_ = dir_->Str();
        file_system_ = std::make_shared<LocalFileSystem>();
        fields_ = {arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
                   arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(test_root_, {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        arrow::Schema typed_schema(fields_);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                       /*partition_keys=*/{"f1"},
                                       /*primary_keys=*/{}, {},
                                       /*ignore_if_exists=*/false));
        table_path_ = PathUtil::JoinPath(test_root_, "foo.db/bar");
    }
    void TearDown() override {
        auto factory_creator = paimon::FactoryCreator::GetInstance();
        factory_creator->TEST_Unregister("gmock_fs");
    }

    void Print(const std::vector<ManifestEntry>& entries) {
        for (const auto& entry : entries) {
            std::cout << entry.FileName() << " ";
        }
        std::cout << std::endl;
    }

    ManifestEntry CreateManifestEntry(const std::string& file_name, const FileKind& kind) const {
        int32_t arity = 1;
        BinaryRow row(arity);
        BinaryRowWriter writer(&row, 20, GetDefaultPool().get());
        writer.WriteInt(0, 10);
        writer.Complete();
        return CreateManifestEntry(file_name, row, kind);
    }

    ManifestEntry CreateManifestEntry(const std::string& file_name, const BinaryRow& partition,
                                      const FileKind& kind) const {
        auto data_file_meta = std::make_shared<DataFileMeta>(
            file_name, 1024, 8, DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), /*min_seq_no=*/16,
            /*max_seq_no=*/32,
            /*schema_id=*/1, /*level=*/2,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/3,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        return ManifestEntry(kind, partition, 0, 2, data_file_meta);
    }

    ManifestEntry CreateManifestEntryWithNoPartition(const std::string& file_name,
                                                     const FileKind& kind) const {
        auto data_file_meta = std::make_shared<DataFileMeta>(
            file_name, 1024, 8, DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), /*min_seq_no=*/16,
            /*max_seq_no=*/32,
            /*schema_id=*/1, /*level=*/2,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/3,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        return ManifestEntry(kind, BinaryRow::EmptyRow(), 0, 2, data_file_meta);
    }

    std::set<std::string> CollectFileNames(const std::vector<ManifestEntry>& entries) {
        std::set<std::string> result;
        for (const auto& entry : entries) {
            result.insert(entry.FileName());
        }
        return result;
    }

    bool IsStringInSet(const std::set<std::string>& strSet, const std::string& target) {
        return strSet.find(target) != strSet.end();
    }

    bool HitIOHook(const Status& status) {
        if (status.ToString().find("io hook triggered io error") != std::string::npos) {
            return true;
        }
        return false;
    }

    bool HitIOHookInCommitHint(const Status& status) {
        if (status.ToString().find("io hook triggered io error at position") != std::string::npos &&
            (status.ToString().find("snapshot/LATEST") != std::string::npos ||
             status.ToString().find("snapshot/EARLIEST") != std::string::npos)) {
            return true;
        }
        return false;
    }

 private:
    Status PrepareFakeFiles(const std::vector<std::string>& files) {
        for (const auto& file : files) {
            auto path = PathUtil::JoinPath(table_path_, file);
            PAIMON_RETURN_NOT_OK(
                file_system_->WriteFile(path, /*content=*/"", /*overwrite=*/false));
        }
        return Status::OK();
    }

    bool IsEqualMsgs(const std::vector<std::shared_ptr<CommitMessage>>& expected_msgs,
                     const std::vector<std::shared_ptr<CommitMessage>>& actual_msgs) {
        if (expected_msgs.size() != actual_msgs.size()) {
            return false;
        }
        for (size_t i = 0; i < expected_msgs.size(); i++) {
            auto actual = std::dynamic_pointer_cast<CommitMessageImpl>(actual_msgs[i]);
            auto expected = std::dynamic_pointer_cast<CommitMessageImpl>(expected_msgs[i]);
            if (*actual == *expected) {
                continue;
            } else {
                return false;
            }
        }
        return true;
    }

    std::vector<std::shared_ptr<CommitMessage>> GetCommitMessages(const std::string& path,
                                                                  int32_t version) const {
        auto file_system = GetFileSystem();
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<FileStatus> file, file_system->GetFileStatus(path));
        std::vector<uint8_t> buffer(file->GetLen(), 0);

        EXPECT_OK_AND_ASSIGN(std::unique_ptr<InputStream> in_stream, file_system->Open(path));
        EXPECT_TRUE(in_stream);
        EXPECT_OK_AND_ASSIGN([[maybe_unused]] int32_t length,
                             in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()))
        EXPECT_OK(in_stream->Close());
        auto pool = GetDefaultPool();

        EXPECT_OK_AND_ASSIGN(
            std::vector<std::shared_ptr<CommitMessage>> ret,
            CommitMessage::DeserializeList(version, reinterpret_cast<char*>(buffer.data()),
                                           buffer.size(), pool));
        return ret;
    }

    std::shared_ptr<FileSystem> GetFileSystem() const {
        return file_system_;
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::string test_root_;
    std::string table_path_;
    std::shared_ptr<FileSystem> file_system_;
    arrow::FieldVector fields_;
};

TEST_F(FileStoreCommitImplTest, TestCommit) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));

    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);
    ASSERT_GT(msgs.size(), 0);
    ASSERT_OK(commit->Commit(msgs));
    ASSERT_NOK_WITH_MSG(commit->GetLastCommitTableRequest(),
                        "renaming snapshot commit do not support get last commit table request");
    std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
    ASSERT_TRUE(metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(1u, counter);
    ASSERT_OK_AND_ASSIGN(
        bool exist, file_system_->Exists(PathUtil::JoinPath(table_path_, "snapshot/snapshot-1")));
    ASSERT_TRUE(exist);
}

TEST_F(FileStoreCommitImplTest, TestRESTCatalogCommit) {
    TimezoneGuard guard("Asia/Shanghai");
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .UseRESTCatalogCommit(true)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));

    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);
    ASSERT_GT(msgs.size(), 0);
    ASSERT_NOK_WITH_MSG(commit->GetLastCommitTableRequest(),
                        "Should call Commit first before GetLastCommitTableRequest.");
    ASSERT_OK(commit->Commit(msgs));

    ASSERT_OK_AND_ASSIGN(std::string commit_table_request_str, commit->GetLastCommitTableRequest());
    ASSERT_OK_AND_ASSIGN(CommitTableRequest commit_table_request,
                         CommitTableRequest::FromJsonString(commit_table_request_str));
    Snapshot expected_snapshot(
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
    std::vector<PartitionStatistics> expected_partition_statistics = {
        PartitionStatistics(/*spec=*/{{"f1", "20"}}, /*record_count=*/1, /*file_size_in_bytes=*/541,
                            /*file_count=*/1,
                            /*last_file_creation_time=*/1724090888743l - 28800000l),
        PartitionStatistics(/*spec=*/{{"f1", "10"}}, /*record_count=*/4,
                            /*file_size_in_bytes=*/1118, /*file_count=*/2,
                            /*last_file_creation_time=*/1724090888727l - 28800000l)};
    CommitTableRequest expected_commit_table_request(expected_snapshot,
                                                     expected_partition_statistics);
    ASSERT_TRUE(commit_table_request.TEST_Equal(expected_commit_table_request));

    std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
    ASSERT_TRUE(metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(1u, counter);
    ASSERT_OK_AND_ASSIGN(
        bool exist, file_system_->Exists(PathUtil::JoinPath(table_path_, "snapshot/snapshot-1")));
    ASSERT_FALSE(exist);
}

// TODO(jinli.zjw): fix disabled case
TEST_F(FileStoreCommitImplTest, DISABLED_TestCommitWithConflictSnapshotAndRetryTenTimes) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    CommitContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::COMMIT_MAX_RETRIES, "10")
                             .AddOption(Options::FILE_SYSTEM, "gmock_fs")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    std::string latest_hint = PathUtil::JoinPath(table_path, "snapshot/LATEST");

    auto* resolving_fs =
        dynamic_cast<ResolvingFileSystem*>(commit_impl->snapshot_manager_->fs_.get());
    ASSERT_OK_AND_ASSIGN(auto real_fs, resolving_fs->GetRealFileSystem(table_path));
    auto* mock_fs = dynamic_cast<GmockFileSystem*>(real_fs.get());
    EXPECT_CALL(*mock_fs, ReadFile(testing::StrEq(latest_hint), testing::_))
        .WillRepeatedly(testing::Invoke([](const std::string& path, std::string* content) {
            *content = "-1";
            return Status::OK();
        }));
    EXPECT_CALL(*mock_fs,
                ListDir(testing::StrEq(PathUtil::JoinPath(table_path, "snapshot")), testing::_))
        .WillRepeatedly(
            testing::Invoke([](const std::string& directory,
                               std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) {
                return Status::OK();
            }));

    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);
    ASSERT_GT(msgs.size(), 0);
    ASSERT_NOK(commit->Commit(msgs));
}

TEST_F(FileStoreCommitImplTest, DISABLED_TestCommitWithConflictSnapshotAndRetryOnce) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    CommitContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "gmock_fs")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    std::string latest_hint = PathUtil::JoinPath(table_path, "snapshot/LATEST");
    auto* resolving_fs =
        dynamic_cast<ResolvingFileSystem*>(commit_impl->snapshot_manager_->fs_.get());
    ASSERT_OK_AND_ASSIGN(auto real_fs, resolving_fs->GetRealFileSystem(table_path));
    auto* mock_fs = dynamic_cast<GmockFileSystem*>(real_fs.get());
    EXPECT_CALL(*mock_fs, ReadFile(testing::StrEq(latest_hint), testing::_))
        .WillRepeatedly(testing::Invoke([](const std::string& path, std::string* content) {
            *content = "-1";
            return Status::OK();
        }));

    EXPECT_CALL(
        *mock_fs,
        ReadFile(testing::StrEq(PathUtil::JoinPath(table_path, "snapshot/snapshot-5")), testing::_))
        .WillOnce(testing::Invoke([&](const std::string& path, std::string* content) {
            return mock_fs->FileSystem::ReadFile(path, content);
        }));

    EXPECT_CALL(*mock_fs,
                ListDir(testing::StrEq(PathUtil::JoinPath(table_path, "snapshot")), testing::_))
        .WillOnce(
            testing::Invoke([](const std::string& directory,
                               std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) {
                return Status::OK();
            }))
        .WillRepeatedly(
            testing::Invoke([&](const std::string& directory,
                                std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) {
                return mock_fs->LocalFileSystem::ListDir(directory, file_status_list);
            }));

    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);
    ASSERT_GT(msgs.size(), 0);
    ASSERT_OK(commit->Commit(msgs));
    std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
    ASSERT_TRUE(metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(2u, counter);
    ASSERT_OK_AND_ASSIGN(
        bool exist, file_system_->Exists(PathUtil::JoinPath(table_path, "snapshot/snapshot-6")));
    ASSERT_TRUE(exist);
}

TEST_F(FileStoreCommitImplTest,
       DISABLED_TestCommitWithAtomicWriteSnapshotTimeoutAndActuallySucceed) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    CommitContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "gmock_fs")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    std::string new_snapshot_6 = PathUtil::JoinPath(table_path, "snapshot/snapshot-6");
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    auto* resolving_fs = dynamic_cast<ResolvingFileSystem*>(commit_impl->fs_.get());
    ASSERT_OK_AND_ASSIGN(auto real_fs, resolving_fs->GetRealFileSystem(table_path));
    auto* mock_fs = dynamic_cast<GmockFileSystem*>(real_fs.get());
    EXPECT_CALL(*mock_fs, AtomicStore(testing::StrEq(new_snapshot_6), testing::_))
        .WillOnce(testing::Invoke([&](const std::string& path, const std::string& content) {
            // to mock atomic store timeout actually succeed
            auto s = mock_fs->FileSystem::AtomicStore(path, content);
            assert(s.ok());
            return Status::IOError("atomic write snapshot failed");
        }));

    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);
    ASSERT_GT(msgs.size(), 0);
    ASSERT_NOK(commit->Commit(msgs, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(
        bool exist, file_system_->Exists(PathUtil::JoinPath(table_path, "snapshot/snapshot-6")));
    ASSERT_TRUE(exist);

    CommitContextBuilder context_builder_2(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context_2,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "gmock_fs")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit_2, FileStoreCommit::Create(std::move(commit_context_2)));
    ASSERT_OK_AND_ASSIGN(int32_t num_committed, commit_2->FilterAndCommit({{1, msgs}}));
    ASSERT_EQ(0, num_committed);
    auto commit_impl_2 = dynamic_cast<FileStoreCommitImpl*>(commit_2.get());
    auto* resolving_fs_2 = dynamic_cast<ResolvingFileSystem*>(commit_impl_2->fs_.get());
    ASSERT_OK_AND_ASSIGN(auto real_fs_2, resolving_fs_2->GetRealFileSystem(table_path));
    auto* mock_fs_2 = dynamic_cast<GmockFileSystem*>(real_fs_2.get());
    std::string new_snapshot_7 = PathUtil::JoinPath(table_path, "snapshot/snapshot-7");
    EXPECT_CALL(*mock_fs_2, AtomicStore(testing::StrEq(new_snapshot_7), testing::_))
        .WillOnce(testing::Invoke([&](const std::string& path, const std::string& content) {
            return mock_fs_2->FileSystem::AtomicStore(path, content);
        }));
    std::vector<std::shared_ptr<CommitMessage>> msgs_2 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-02",
                          /*version=*/3);
    ASSERT_OK(commit_2->Commit(msgs_2, /*commit_identifier=*/2));
    ASSERT_OK_AND_ASSIGN(exist, file_system_->Exists(new_snapshot_7));
    ASSERT_TRUE(exist);
}

TEST_F(FileStoreCommitImplTest, TestCommitWithSameMsgs) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "5kb")
                             .AddOption(Options::MANIFEST_MERGE_MIN_COUNT, "2")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));

    {
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/"
                                  "commit_messages-01",
                              /*version=*/3);
        ASSERT_GT(msgs.size(), 0);
        ASSERT_OK(commit->Commit(msgs));
        std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
        ASSERT_TRUE(metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t counter,
                             metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
        ASSERT_EQ(1u, counter);
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(PathUtil::JoinPath(
                                             table_path_, "snapshot/snapshot-1")));
        ASSERT_TRUE(exist);
    }
    {
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/"
                                  "commit_messages-01",
                              /*version=*/3);
        ASSERT_GT(msgs.size(), 0);
        ASSERT_OK(commit->Commit(msgs));
        std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
        ASSERT_TRUE(metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t counter,
                             metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
        ASSERT_EQ(1u, counter);
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(PathUtil::JoinPath(
                                             table_path_, "snapshot/snapshot-2")));
        ASSERT_TRUE(exist);
    }
    {
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/"
                                  "commit_messages-01",
                              /*version=*/3);
        ASSERT_GT(msgs.size(), 0);
        ASSERT_NOK(commit->Commit(msgs));
        std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
        ASSERT_TRUE(metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t counter,
                             metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
        ASSERT_EQ(1u, counter);
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(PathUtil::JoinPath(
                                             table_path_, "snapshot/snapshot-3")));
        ASSERT_FALSE(exist);
    }
}

TEST_F(FileStoreCommitImplTest, TestCommitMultipleTimes) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    {
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/"
                                  "commit_messages-01",
                              /*version=*/3);
        ASSERT_GT(msgs.size(), 0);
        ASSERT_OK(commit->Commit(msgs, /*commit_identifier=*/0, /*watermark=*/10));
        std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
        ASSERT_TRUE(metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t counter,
                             metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
        ASSERT_EQ(1u, counter);
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(PathUtil::JoinPath(
                                             table_path_, "snapshot/snapshot-1")));
        ASSERT_TRUE(exist);
        ASSERT_OK_AND_ASSIGN(auto snapshot1, commit_impl->snapshot_manager_->LatestSnapshot());
        ASSERT_TRUE(snapshot1.value().Watermark());
        ASSERT_EQ(10, snapshot1.value().Watermark().value());
    }
    {
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/"
                                  "commit_messages-02",
                              /*version=*/3);
        ASSERT_GT(msgs.size(), 0);
        ASSERT_OK(commit->Commit(msgs, /*commit_identifier=*/1));
        std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
        ASSERT_TRUE(metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t counter,
                             metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
        ASSERT_EQ(1u, counter);
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(PathUtil::JoinPath(
                                             table_path_, "snapshot/snapshot-2")));
        ASSERT_TRUE(exist);
        ASSERT_OK_AND_ASSIGN(auto snapshot2, commit_impl->snapshot_manager_->LatestSnapshot());
        ASSERT_TRUE(snapshot2.value().Watermark());
        ASSERT_EQ(10, snapshot2.value().Watermark().value());
    }
    {
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/"
                                  "commit_messages-03",
                              /*version=*/3);
        ASSERT_GT(msgs.size(), 0);
        ASSERT_OK(commit->Commit(msgs, /*commit_identifier=*/2, /*watermark=*/9));
        std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
        ASSERT_TRUE(metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t counter,
                             metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
        ASSERT_EQ(1u, counter);
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(PathUtil::JoinPath(
                                             table_path_, "snapshot/snapshot-3")));
        ASSERT_TRUE(exist);
        ASSERT_OK_AND_ASSIGN(auto snapshot3, commit_impl->snapshot_manager_->LatestSnapshot());
        ASSERT_TRUE(snapshot3.value().Watermark());
        ASSERT_EQ(10, snapshot3.value().Watermark().value());
    }
}

TEST_F(FileStoreCommitImplTest, TestCommitAndOverwriteWithNoPartitionKey) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    {
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/"
                                  "commit_messages-01",
                              /*version=*/3);
        ASSERT_GT(msgs.size(), 0);
        ASSERT_OK(commit->Commit(msgs, 1));
        std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
        ASSERT_TRUE(metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t counter,
                             metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
        ASSERT_EQ(1u, counter);
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(PathUtil::JoinPath(
                                             table_path_, "snapshot/snapshot-1")));
        ASSERT_TRUE(exist);
    }
    {
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/"
                                  "commit_messages-02",
                              /*version=*/3);
        ASSERT_GT(msgs.size(), 0);
        auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
            std::shared_ptr<FileStoreCommit>(std::move(commit)));
        ASSERT_OK(commit_impl->Overwrite({}, msgs, 2));
        std::shared_ptr<Metrics> metrics = commit_impl->GetCommitMetrics();
        ASSERT_TRUE(metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t counter,
                             metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
        ASSERT_EQ(1u, counter);
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(PathUtil::JoinPath(
                                             table_path_, "snapshot/snapshot-2")));
        ASSERT_TRUE(exist);
    }
}

TEST_F(FileStoreCommitImplTest, TestCommitSuccessAfterIOException) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));

    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-01",
                          /*version=*/3);
    ASSERT_GT(msgs.size(), 0);
    ASSERT_OK(commit->Commit(msgs));
    std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
    ASSERT_TRUE(metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(1u, counter);
    ASSERT_OK_AND_ASSIGN(
        bool exist, file_system_->Exists(PathUtil::JoinPath(table_path_, "snapshot/snapshot-1")));
    ASSERT_TRUE(exist);

    bool scanned_all_io_hook = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 300; i++) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        auto status = commit->Commit(msgs);
        io_hook->Clear();
        // termination conditions:
        // 1. status does not hit IOHook, already touch all IO operation
        // 2. hit IOHook in commit hint, at this point, the snapshot is already committed
        if (!HitIOHook(status) || HitIOHookInCommitHint(status)) {
            scanned_all_io_hook = true;
            ASSERT_OK_AND_ASSIGN(bool exist2, file_system_->Exists(PathUtil::JoinPath(
                                                  table_path_, "snapshot/snapshot-2")));
            ASSERT_TRUE(exist2);
            break;
        }
        ASSERT_OK_AND_ASSIGN(bool exist2, file_system_->Exists(PathUtil::JoinPath(
                                              table_path_, "snapshot/snapshot-2")));
        ASSERT_FALSE(exist2);
        std::vector<int64_t> actual_snapshots;
        ASSERT_OK(
            FileUtils::ListVersionedFiles(file_system_, PathUtil::JoinPath(table_path_, "snapshot"),
                                          SnapshotManager::SNAPSHOT_PREFIX, &actual_snapshots));
        std::vector<int64_t> expected_snapshots = {1};
        ASSERT_EQ(actual_snapshots, expected_snapshots);
    }
    ASSERT_TRUE(scanned_all_io_hook);
}

TEST_F(FileStoreCommitImplTest, TestCleanUpTmpManifests) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));

    {
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/"
                                  "commit_messages-01",
                              /*version=*/3);
        ASSERT_GT(msgs.size(), 0);
        ASSERT_OK(commit->Commit(msgs));
        std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
        ASSERT_TRUE(metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t counter,
                             metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
        ASSERT_EQ(1u, counter);
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(PathUtil::JoinPath(
                                             table_path_, "snapshot/snapshot-1")));
        ASSERT_TRUE(exist);
    }
    {
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/"
                                  "commit_messages-02",
                              /*version=*/3);
        ASSERT_GT(msgs.size(), 0);
        ASSERT_OK(commit->Commit(msgs));
        std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
        ASSERT_TRUE(metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t counter,
                             metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
        ASSERT_EQ(1u, counter);
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(PathUtil::JoinPath(
                                             table_path_, "snapshot/snapshot-2")));
        ASSERT_TRUE(exist);
    }
    {
        // commit index
        std::vector<std::shared_ptr<IndexFileMeta>> new_index_files;
        new_index_files.push_back(std::make_shared<IndexFileMeta>(
            "bitmap", "bitmap-global-index-567ff117-68a0-436d-a270-dc8f6e403d06.index", 100, 5,
            /*dv_ranges=*/std::nullopt,
            /*external_path=*/std::nullopt, std::nullopt));
        DataIncrement data_increment({}, {}, {}, std::move(new_index_files), {});
        std::shared_ptr<CommitMessage> msgs = std::make_shared<CommitMessageImpl>(
            BinaryRowGenerator::GenerateRow({10}, GetDefaultPool().get()), /*bucket=*/0,
            /*total_bucket=*/2, data_increment, CompactIncrement({}, {}, {}));
        ASSERT_OK(commit->Commit({msgs}));
        std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
        ASSERT_TRUE(metrics);
        ASSERT_OK_AND_ASSIGN(uint64_t counter,
                             metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
        ASSERT_EQ(1u, counter);
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(PathUtil::JoinPath(
                                             table_path_, "snapshot/snapshot-3")));
        ASSERT_TRUE(exist);
    }
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         commit_impl->snapshot_manager_->LatestSnapshot());

    std::vector<ManifestFileMeta> previous_manifests;
    ASSERT_OK(
        commit_impl->manifest_list_->ReadDataManifests(snapshot.value(), &previous_manifests));
    auto index_manifest = snapshot.value().IndexManifest();

    bool exist = false;
    ASSERT_OK_AND_ASSIGN(exist,
                         file_system_->Exists(PathUtil::JoinPath(
                             table_path_, "manifest/" + snapshot.value().BaseManifestList())));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(exist,
                         file_system_->Exists(PathUtil::JoinPath(
                             table_path_, "manifest/" + snapshot.value().DeltaManifestList())));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(exist, file_system_->Exists(PathUtil::JoinPath(
                                    table_path_, "manifest/" + index_manifest.value())));
    ASSERT_TRUE(exist);

    commit_impl->CleanUpTmpManifests(snapshot.value().BaseManifestList(),
                                     snapshot.value().DeltaManifestList(), previous_manifests,
                                     previous_manifests, /*old_index_manifest=*/std::nullopt,
                                     /*new_index_manifest=*/index_manifest);
    for (const auto& manifest : previous_manifests) {
        ASSERT_OK_AND_ASSIGN(exist, file_system_->Exists(PathUtil::JoinPath(
                                        table_path_, "manifest/" + manifest.FileName())));
        ASSERT_TRUE(exist);
    }
    ASSERT_OK_AND_ASSIGN(exist,
                         file_system_->Exists(PathUtil::JoinPath(
                             table_path_, "manifest/" + snapshot.value().BaseManifestList())));
    ASSERT_FALSE(exist);
    ASSERT_OK_AND_ASSIGN(exist,
                         file_system_->Exists(PathUtil::JoinPath(
                             table_path_, "manifest/" + snapshot.value().DeltaManifestList())));
    ASSERT_FALSE(exist);
    ASSERT_OK_AND_ASSIGN(exist, file_system_->Exists(PathUtil::JoinPath(
                                    table_path_, "manifest/" + index_manifest.value())));
    ASSERT_FALSE(exist);

    commit_impl->CleanUpTmpManifests(
        snapshot.value().BaseManifestList(), snapshot.value().DeltaManifestList(), /*old_metas=*/{},
        /*new_metas=*/previous_manifests, /*old_index_manifest=*/std::nullopt,
        /*new_index_manifest=*/std::nullopt);
    for (const auto& manifest : previous_manifests) {
        ASSERT_OK_AND_ASSIGN(exist, file_system_->Exists(PathUtil::JoinPath(
                                        table_path_, "manifest/" + manifest.FileName())));
        ASSERT_FALSE(exist);
    }
}

TEST_F(FileStoreCommitImplTest, TestCommitWithIgnoreEmptyCommit) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .IgnoreEmptyCommit(true)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    std::vector<std::shared_ptr<CommitMessage>> msgs;
    ASSERT_OK(commit->Commit(msgs));
    std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
    ASSERT_TRUE(metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(0u, counter);
}

TEST_F(FileStoreCommitImplTest, TestCheckConflict) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .IgnoreEmptyCommit(true)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("file1", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file2", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file3", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file4", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file5", FileKind::Add()));

        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("file1", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file2", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file3", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file4", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file5", FileKind::Delete()));
        ASSERT_OK(commit_impl->NoConflictsOrFail("commit_user_1", base_entries, changes));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("file1", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file2", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file3", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file4", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file5", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file6", FileKind::Add()));

        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("file1", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file2", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file3", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file4", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file5", FileKind::Delete()));
        ASSERT_OK(commit_impl->NoConflictsOrFail("commit_user_1", base_entries, changes));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("file1", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file1", FileKind::Add()));

        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("file2", FileKind::Add()));
        changes.push_back(CreateManifestEntry("file3", FileKind::Add()));
        changes.push_back(CreateManifestEntry("file4", FileKind::Add()));
        changes.push_back(CreateManifestEntry("file5", FileKind::Add()));
        ASSERT_NOK(commit_impl->NoConflictsOrFail("commit_user_1", base_entries, changes));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("file1", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file2", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file3", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file4", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file5", FileKind::Add()));

        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("file1", FileKind::Add()));
        changes.push_back(CreateManifestEntry("file2", FileKind::Add()));
        changes.push_back(CreateManifestEntry("file3", FileKind::Add()));
        changes.push_back(CreateManifestEntry("file4", FileKind::Add()));
        changes.push_back(CreateManifestEntry("file5", FileKind::Add()));
        ASSERT_NOK(commit_impl->NoConflictsOrFail("commit_user_1", base_entries, changes));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("file1", FileKind::Delete()));
        base_entries.push_back(CreateManifestEntry("file2", FileKind::Delete()));
        base_entries.push_back(CreateManifestEntry("file3", FileKind::Delete()));
        base_entries.push_back(CreateManifestEntry("file4", FileKind::Delete()));
        base_entries.push_back(CreateManifestEntry("file5", FileKind::Delete()));

        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("file1", FileKind::Add()));
        changes.push_back(CreateManifestEntry("file2", FileKind::Add()));
        changes.push_back(CreateManifestEntry("file3", FileKind::Add()));
        changes.push_back(CreateManifestEntry("file4", FileKind::Add()));
        changes.push_back(CreateManifestEntry("file5", FileKind::Add()));
        ASSERT_NOK(commit_impl->NoConflictsOrFail("commit_user_1", base_entries, changes));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("file1", FileKind::Delete()));
        base_entries.push_back(CreateManifestEntry("file1", FileKind::Delete()));

        std::vector<ManifestEntry> changes;
        ASSERT_OK(commit_impl->NoConflictsOrFail("commit_user_1", base_entries, changes));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("file1", FileKind::Delete()));
        base_entries.push_back(CreateManifestEntry("file2", FileKind::Delete()));
        base_entries.push_back(CreateManifestEntry("file3", FileKind::Delete()));
        base_entries.push_back(CreateManifestEntry("file4", FileKind::Delete()));
        base_entries.push_back(CreateManifestEntry("file5", FileKind::Delete()));

        std::vector<ManifestEntry> changes;
        ASSERT_NOK(commit_impl->NoConflictsOrFail("commit_user_1", base_entries, changes));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("file1", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file2", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file3", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file4", FileKind::Add()));
        base_entries.push_back(CreateManifestEntry("file5", FileKind::Add()));

        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("file1", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file2", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file3", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file4", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file5", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("file6", FileKind::Delete()));
        ASSERT_NOK(commit_impl->NoConflictsOrFail("commit_user_1", base_entries, changes));
    }
}

TEST_F(FileStoreCommitImplTest, TestTryOverwrite) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .IgnoreEmptyCommit(true)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-01",
                          /*version=*/3);
    ASSERT_OK(commit->Commit(msgs, /*commit_identifier=*/0));
    std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
    ASSERT_TRUE(metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(1u, counter);
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);
    std::vector<ManifestEntry> changes;
    changes.push_back(CreateManifestEntry("new_file_1", FileKind::Add()));
    std::vector<std::map<std::string, std::string>> partitions = {{{"f1", "10"}}, {{"f1", "20"}}};
    ASSERT_OK(commit_impl->TryOverwrite(partitions, changes,
                                        /*commit_identifier=*/1, std::nullopt));
}

TEST_F(FileStoreCommitImplTest, TestTryOverwriteFromNothing) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .IgnoreEmptyCommit(true)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);
    std::vector<ManifestEntry> changes;
    changes.push_back(CreateManifestEntry("new_file_1", FileKind::Add()));
    std::vector<std::map<std::string, std::string>> partitions = {{{"f1", "10"}}, {{"f1", "20"}}};
    ASSERT_OK(commit_impl->TryOverwrite(partitions, changes,
                                        /*commit_identifier=*/0, std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto snapshot1, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries1, commit_impl->GetAllFiles(snapshot1.value(), {}));
    ASSERT_EQ(1u, entries1.size());
    ASSERT_EQ("new_file_1", entries1[0].FileName());
    ASSERT_EQ(FileKind::Add(), entries1[0].Kind());
    std::vector<ManifestEntry> changes2;
    changes2.push_back(CreateManifestEntry("new_file_2", FileKind::Add()));
    ASSERT_OK(commit_impl->TryOverwrite(partitions, changes2,
                                        /*commit_identifier=*/1, std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto snapshot2, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries2, commit_impl->GetAllFiles(snapshot2.value(), {}));
    ASSERT_EQ(1u, entries2.size());
    ASSERT_EQ("new_file_2", entries2[0].FileName());
    ASSERT_EQ(FileKind::Add(), entries2[0].Kind());
}

TEST_F(FileStoreCommitImplTest, TestTryOverwriteThenCommit) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .IgnoreEmptyCommit(true)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);
    std::vector<ManifestEntry> changes;
    changes.push_back(CreateManifestEntry("new_file_1", FileKind::Add()));
    std::vector<std::map<std::string, std::string>> partitions = {{{"f1", "10"}}, {{"f1", "20"}}};
    ASSERT_OK(commit_impl->TryOverwrite(partitions, changes,
                                        /*commit_identifier=*/0, std::nullopt));
    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-01",
                          /*version=*/3);
    ASSERT_OK(commit->Commit(msgs, /*commit_identifier=*/1));
    std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
    ASSERT_TRUE(metrics);

    ASSERT_OK_AND_ASSIGN(auto snapshot1, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries1, commit_impl->GetAllFiles(snapshot1.value(), {}));
    ASSERT_EQ(4u, entries1.size());
    std::set<std::string> file_names = CollectFileNames(entries1);
    ASSERT_TRUE(IsStringInSet(file_names, "new_file_1"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-51a45441-6037-4af3-b67b-5cefd75dc6f2-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-6828284c-e707-49b5-af6b-69be79af120c-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-8dc7f04c-3c98-48b2-9d56-834d746c4a40-0.orc"));

    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(1u, counter);

    std::vector<ManifestEntry> changes2;
    changes2.push_back(CreateManifestEntry("new_file_2", FileKind::Add()));
    ASSERT_OK(commit_impl->TryOverwrite(partitions, changes2,
                                        /*commit_identifier=*/2, std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto snapshot2, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries2, commit_impl->GetAllFiles(snapshot2.value(), {}));
    ASSERT_EQ(1u, entries2.size());
    ASSERT_EQ("new_file_2", entries2[0].FileName());
    ASSERT_EQ(FileKind::Add(), entries2[0].Kind());
}

TEST_F(FileStoreCommitImplTest, TestDropPartitionAndExpireSnapshot) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .AddOption(Options::SNAPSHOT_NUM_RETAINED_MIN, "1")
                             .AddOption(Options::SNAPSHOT_NUM_RETAINED_MAX, "1")
                             .AddOption(Options::SNAPSHOT_EXPIRE_LIMIT, "30")
                             .AddOption(Options::SNAPSHOT_TIME_RETAINED, "1ms")
                             .IgnoreEmptyCommit(true)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-01",
                          /*version=*/3);
    ASSERT_OK(commit->Commit(msgs, /*commit_identifier=*/0));
    std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
    ASSERT_TRUE(metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(1u, counter);
    ASSERT_OK(commit->DropPartition({{{"f1", "10"}}}, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(int32_t expire_snapshot_cnt, commit->Expire());
    ASSERT_EQ(expire_snapshot_cnt, 1);
    ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(table_path_ + "/snapshot/snapshot-2"));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(exist, file_system_->Exists(table_path_ + "/snapshot/snapshot-1"));
    ASSERT_FALSE(exist);
    ASSERT_OK_AND_ASSIGN(exist, file_system_->Exists(table_path_ + "/snapshot/EARLIEST"));
    ASSERT_TRUE(exist);
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> earliest_snapshot_id,
                         commit_impl->snapshot_manager_->EarliestSnapshotId());
    ASSERT_TRUE(earliest_snapshot_id);
    ASSERT_EQ(earliest_snapshot_id.value(), 2);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, commit_impl->snapshot_manager_->LoadSnapshot(2));
    std::vector<ManifestFileMeta> manifests;
    ASSERT_OK(commit_impl->manifest_list_->ReadDeltaManifests(snapshot, &manifests));
    ASSERT_EQ(1, manifests.size());
    ASSERT_EQ(0, manifests[0].NumAddedFiles());
    ASSERT_EQ(2, manifests[0].NumDeletedFiles());
}

TEST_F(FileStoreCommitImplTest, TestDropMultiPartitionAndExpireSnapshot) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .AddOption(Options::SNAPSHOT_NUM_RETAINED_MIN, "1")
                             .AddOption(Options::SNAPSHOT_NUM_RETAINED_MAX, "1")
                             .AddOption(Options::SNAPSHOT_EXPIRE_LIMIT, "30")
                             .AddOption(Options::SNAPSHOT_TIME_RETAINED, "1ms")
                             .IgnoreEmptyCommit(true)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-01",
                          /*version=*/3);
    ASSERT_OK(commit->Commit(msgs, /*commit_identifier=*/0));
    std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
    ASSERT_TRUE(metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(1u, counter);
    ASSERT_OK(commit->DropPartition({{{"f1", "10"}}, {{"f1", "20"}}}, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(int32_t expire_snapshot_cnt, commit->Expire());
    ASSERT_EQ(expire_snapshot_cnt, 1);
    ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(table_path_ + "/snapshot/snapshot-2"));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(exist, file_system_->Exists(table_path_ + "/snapshot/snapshot-1"));
    ASSERT_FALSE(exist);
    ASSERT_OK_AND_ASSIGN(exist, file_system_->Exists(table_path_ + "/snapshot/EARLIEST"));
    ASSERT_TRUE(exist);
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> earliest_snapshot_id,
                         commit_impl->snapshot_manager_->EarliestSnapshotId());
    ASSERT_TRUE(earliest_snapshot_id);
    ASSERT_EQ(earliest_snapshot_id.value(), 2);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, commit_impl->snapshot_manager_->LoadSnapshot(2));
    std::vector<ManifestFileMeta> manifests;
    ASSERT_OK(commit_impl->manifest_list_->ReadDeltaManifests(snapshot, &manifests));
    ASSERT_EQ(1, manifests.size());
    ASSERT_EQ(0, manifests[0].NumAddedFiles());
    ASSERT_EQ(3, manifests[0].NumDeletedFiles());
}

TEST_F(FileStoreCommitImplTest, TestCreateManifestCommittable) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .IgnoreEmptyCommit(true)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));
    std::vector<std::shared_ptr<CommitMessage>> msgs;
    auto committable = commit_impl->CreateManifestCommittable(1, msgs, 30);
    ASSERT_TRUE(committable);
    EXPECT_EQ(1, committable->Identifier());
    EXPECT_EQ(30, committable->Watermark().value());
    ASSERT_TRUE(IsEqualMsgs(msgs, committable->FileCommittables()));
}

TEST_F(FileStoreCommitImplTest, TestCollectChanges) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .AddOption(Options::BUCKET, "10")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-01",
                          /*version=*/3);
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));
    std::vector<ManifestEntry> append_table_files;
    std::vector<ManifestEntry> append_changelog_files;
    std::vector<ManifestEntry> compact_table_files;
    std::vector<ManifestEntry> compact_changelog_files;
    std::vector<IndexManifestEntry> append_table_index_files;
    std::vector<IndexManifestEntry> compact_table_index_files;
    ASSERT_OK(commit_impl->CollectChanges(msgs, &append_table_files, &append_changelog_files,
                                          &compact_table_files, &compact_changelog_files,
                                          &append_table_index_files, &compact_table_index_files));
    ASSERT_EQ(append_table_files.size(), 3u);
    ASSERT_EQ(append_changelog_files.size(), 0u);
    ASSERT_EQ(compact_table_files.size(), 0u);
    ASSERT_EQ(compact_changelog_files.size(), 0u);
    ASSERT_EQ(append_table_index_files.size(), 0u);
    ASSERT_EQ(compact_table_index_files.size(), 0u);
    ASSERT_EQ(append_table_files[0].Kind(), FileKind::Add());
    ASSERT_EQ(append_table_files[0].Bucket(), 0);
    ASSERT_EQ(append_table_files[0].TotalBuckets(), 10);
    ASSERT_EQ(append_table_files[0].Level(), 0);
    ASSERT_EQ(append_table_files[0].FileName(), "data-51a45441-6037-4af3-b67b-5cefd75dc6f2-0.orc");
    ASSERT_EQ(append_table_files[1].Kind(), FileKind::Add());
    ASSERT_EQ(append_table_files[1].Bucket(), 1);
    ASSERT_EQ(append_table_files[1].TotalBuckets(), 10);
    ASSERT_EQ(append_table_files[1].Level(), 0);
    ASSERT_EQ(append_table_files[1].FileName(), "data-6828284c-e707-49b5-af6b-69be79af120c-0.orc");
    ASSERT_EQ(append_table_files[2].Kind(), FileKind::Add());
    ASSERT_EQ(append_table_files[2].Bucket(), 0);
    ASSERT_EQ(append_table_files[2].TotalBuckets(), 10);
    ASSERT_EQ(append_table_files[2].Level(), 0);
    ASSERT_EQ(append_table_files[2].FileName(), "data-8dc7f04c-3c98-48b2-9d56-834d746c4a40-0.orc");
}

TEST_F(FileStoreCommitImplTest, TestFilterCommitted) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-01",
                          /*version=*/3);
    auto committable = commit_impl->CreateManifestCommittable(1, msgs, std::nullopt);
    std::vector<std::shared_ptr<ManifestCommittable>> committables = {committable};

    // Test with no previous snapshots
    ASSERT_OK_AND_ASSIGN(auto filtered_committables, commit_impl->FilterCommitted(committables));
    ASSERT_EQ(filtered_committables.size(), committables.size());

    // Test with a previous snapshot
    ASSERT_OK(commit_impl->Commit(committable, /*check_append_files=*/false));
    ASSERT_OK_AND_ASSIGN(filtered_committables, commit_impl->FilterCommitted(committables));
    ASSERT_EQ(filtered_committables.size(), 0);
}

TEST_F(FileStoreCommitImplTest, TestFilterCommittedWithMultipleCommittables) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    std::vector<std::shared_ptr<CommitMessage>> msgs1 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-01",
                          /*version=*/3);
    auto committable1 = commit_impl->CreateManifestCommittable(1, msgs1, std::nullopt);

    std::vector<std::shared_ptr<CommitMessage>> msgs2 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-02",
                          /*version=*/3);
    auto committable2 = commit_impl->CreateManifestCommittable(2, msgs2, std::nullopt);

    std::vector<std::shared_ptr<ManifestCommittable>> committables = {committable1, committable2};

    // Test with no previous snapshots
    ASSERT_OK_AND_ASSIGN(auto filtered_committables, commit_impl->FilterCommitted(committables));
    ASSERT_EQ(filtered_committables.size(), committables.size());

    // Test with a previous snapshot
    ASSERT_OK(commit_impl->Commit(committable1, /*check_append_files=*/false));
    ASSERT_OK_AND_ASSIGN(filtered_committables, commit_impl->FilterCommitted(committables));
    ASSERT_EQ(filtered_committables.size(), 1);
    ASSERT_EQ(filtered_committables[0]->Identifier(), committable2->Identifier());
}

TEST_F(FileStoreCommitImplTest, FilterAndCommit) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));
    std::vector<std::string> data_files = {
        "/f1=10/bucket-0/data-51a45441-6037-4af3-b67b-5cefd75dc6f2-0.orc",
        "/f1=10/bucket-1/data-6828284c-e707-49b5-af6b-69be79af120c-0.orc",
        "/f1=20/bucket-0/data-8dc7f04c-3c98-48b2-9d56-834d746c4a40-0.orc",
        "/f1=10/bucket-1/data-fd1d2255-43f2-4534-b4cc-08b29e662940-0.orc",
        "/f1=20/bucket-0/data-7b3f4cc7-116b-4d2f-9c62-5dadc1f11bcb-0.orc"};
    ASSERT_OK(PrepareFakeFiles(data_files));
    std::vector<std::shared_ptr<CommitMessage>> msgs1 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-01",
                          /*version=*/3);

    std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>> inputs1;
    inputs1[1] = msgs1;
    ASSERT_OK_AND_ASSIGN(int32_t actual_committed, commit_impl->FilterAndCommit(inputs1, 5));
    ASSERT_EQ(1u, actual_committed);

    ASSERT_OK_AND_ASSIGN(actual_committed, commit_impl->FilterAndCommit(inputs1, 10));
    ASSERT_EQ(0u, actual_committed);
    ASSERT_OK_AND_ASSIGN(auto snapshot1, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_EQ(5, snapshot1.value().Watermark().value());

    std::vector<std::shared_ptr<CommitMessage>> msgs2 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-02",
                          /*version=*/3);

    std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>> inputs2;
    inputs2[2] = msgs2;
    ASSERT_OK_AND_ASSIGN(actual_committed, commit_impl->FilterAndCommit(inputs2, 20));
    ASSERT_EQ(1u, actual_committed);
    ASSERT_OK_AND_ASSIGN(auto snapshot2, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_EQ(20, snapshot2.value().Watermark().value());
}

TEST_F(FileStoreCommitImplTest, FilterAndCommitWithNotExistFile) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    std::vector<std::shared_ptr<CommitMessage>> msgs1 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-01",
                          /*version=*/3);

    std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>> inputs1;
    inputs1[1] = msgs1;
    ASSERT_NOK(commit_impl->FilterAndCommit(inputs1));
}

TEST_F(FileStoreCommitImplTest, TestOverwriteNonSpecifyPartition) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    std::vector<std::shared_ptr<CommitMessage>> msgs1 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-01",
                          /*version=*/3);

    ASSERT_OK(commit_impl->Commit(msgs1, 1));
    ASSERT_OK_AND_ASSIGN(auto snapshot1, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries1, commit_impl->GetAllFiles(snapshot1.value(), {}));
    ASSERT_EQ(3u, entries1.size());
    std::set<std::string> file_names = CollectFileNames(entries1);
    ASSERT_TRUE(IsStringInSet(file_names, "data-51a45441-6037-4af3-b67b-5cefd75dc6f2-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-6828284c-e707-49b5-af6b-69be79af120c-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-8dc7f04c-3c98-48b2-9d56-834d746c4a40-0.orc"));

    std::vector<std::shared_ptr<CommitMessage>> msgs2 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-02",
                          /*version=*/3);
    ASSERT_OK(commit_impl->Overwrite({}, msgs2, 2));
    ASSERT_OK_AND_ASSIGN(auto snapshot2, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries2, commit_impl->GetAllFiles(snapshot2.value(), {}));
    file_names = CollectFileNames(entries2);
    ASSERT_TRUE(IsStringInSet(file_names, "data-fd1d2255-43f2-4534-b4cc-08b29e662940-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-7b3f4cc7-116b-4d2f-9c62-5dadc1f11bcb-0.orc"));
}

TEST_F(FileStoreCommitImplTest, TestFilterAndOverwrite) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));
    std::vector<std::string> data_files = {
        "/f1=10/bucket-0/data-51a45441-6037-4af3-b67b-5cefd75dc6f2-0.orc",
        "/f1=10/bucket-1/data-6828284c-e707-49b5-af6b-69be79af120c-0.orc",
        "/f1=20/bucket-0/data-8dc7f04c-3c98-48b2-9d56-834d746c4a40-0.orc",
        "/f1=10/bucket-1/data-fd1d2255-43f2-4534-b4cc-08b29e662940-0.orc",
        "/f1=20/bucket-0/data-7b3f4cc7-116b-4d2f-9c62-5dadc1f11bcb-0.orc"};
    ASSERT_OK(PrepareFakeFiles(data_files));

    std::vector<std::shared_ptr<CommitMessage>> msgs1 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);

    ASSERT_OK_AND_ASSIGN(int32_t actual_commit, commit_impl->FilterAndOverwrite({}, msgs1, 1, 10));
    ASSERT_EQ(1, actual_commit);
    ASSERT_OK_AND_ASSIGN(actual_commit, commit_impl->FilterAndOverwrite({}, msgs1, 1, 5));
    ASSERT_EQ(0, actual_commit);

    ASSERT_OK_AND_ASSIGN(auto snapshot1, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_TRUE(snapshot1.value().Watermark());
    ASSERT_EQ(10, snapshot1.value().Watermark().value());
    ASSERT_OK_AND_ASSIGN(auto entries1, commit_impl->GetAllFiles(snapshot1.value(), {}));
    ASSERT_EQ(3u, entries1.size());
    std::set<std::string> file_names = CollectFileNames(entries1);
    ASSERT_TRUE(IsStringInSet(file_names, "data-51a45441-6037-4af3-b67b-5cefd75dc6f2-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-6828284c-e707-49b5-af6b-69be79af120c-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-8dc7f04c-3c98-48b2-9d56-834d746c4a40-0.orc"));

    std::vector<std::shared_ptr<CommitMessage>> msgs2 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-02",
                          /*version=*/3);
    ASSERT_OK_AND_ASSIGN(actual_commit, commit_impl->FilterAndOverwrite({}, msgs2, 2, 20));
    ASSERT_EQ(1, actual_commit);
    ASSERT_OK_AND_ASSIGN(auto snapshot2, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_TRUE(snapshot2);
    ASSERT_TRUE(snapshot2.value().Watermark());
    ASSERT_EQ(20, snapshot2.value().Watermark().value());
    ASSERT_OK_AND_ASSIGN(auto entries2, commit_impl->GetAllFiles(snapshot2.value(), {}));
    file_names = CollectFileNames(entries2);
    ASSERT_TRUE(IsStringInSet(file_names, "data-fd1d2255-43f2-4534-b4cc-08b29e662940-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-7b3f4cc7-116b-4d2f-9c62-5dadc1f11bcb-0.orc"));
}

TEST_F(FileStoreCommitImplTest, TestOverwriteWithSpecifyPartition) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    std::vector<std::shared_ptr<CommitMessage>> msgs1 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);

    ASSERT_OK(commit_impl->Commit(msgs1, 1));

    std::vector<std::shared_ptr<CommitMessage>> msgs2 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-02",
                          /*version=*/3);

    std::map<std::string, std::string> partitions;
    partitions["f1"] = "10";
    ASSERT_OK(commit_impl->Overwrite({partitions}, msgs2, 2));
    ASSERT_OK_AND_ASSIGN(auto snapshot1, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries1, commit_impl->GetAllFiles(snapshot1.value(), {}));
    ASSERT_EQ(3u, entries1.size());
    std::set<std::string> file_names = CollectFileNames(entries1);
    ASSERT_TRUE(IsStringInSet(file_names, "data-fd1d2255-43f2-4534-b4cc-08b29e662940-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-7b3f4cc7-116b-4d2f-9c62-5dadc1f11bcb-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-8dc7f04c-3c98-48b2-9d56-834d746c4a40-0.orc"));
}

TEST_F(FileStoreCommitImplTest, TestOverwriteWithSameFile) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    std::vector<std::shared_ptr<CommitMessage>> msgs1 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);

    ASSERT_OK(commit_impl->Commit(msgs1, 1));
    ASSERT_OK_AND_ASSIGN(auto snapshot1, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries1, commit_impl->GetAllFiles(snapshot1.value(), {}));
    ASSERT_EQ(3u, entries1.size());
    std::set<std::string> file_names = CollectFileNames(entries1);
    ASSERT_TRUE(IsStringInSet(file_names, "data-51a45441-6037-4af3-b67b-5cefd75dc6f2-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-6828284c-e707-49b5-af6b-69be79af120c-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-8dc7f04c-3c98-48b2-9d56-834d746c4a40-0.orc"));

    // same file delete, then add, file will also be removed in result
    ASSERT_OK(commit_impl->Overwrite({}, msgs1, 2));
    ASSERT_OK_AND_ASSIGN(auto snapshot2, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries2, commit_impl->GetAllFiles(snapshot2.value(), {}));
    ASSERT_EQ(0u, entries2.size());
}

TEST_F(FileStoreCommitImplTest, TestCommitWithIOException) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));

    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-01",
                          /*version=*/3);
    ASSERT_GT(msgs.size(), 0);
    // commit first snapshot
    ASSERT_OK(commit->Commit(msgs));
    std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
    ASSERT_TRUE(metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(1u, counter);
    ASSERT_OK_AND_ASSIGN(
        bool exist, file_system_->Exists(PathUtil::JoinPath(table_path_, "snapshot/snapshot-1")));
    ASSERT_TRUE(exist);

    bool commit_run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 500; i++) {
        auto tmp_dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(TestUtil::CopyDirectory(table_path_, tmp_dir->Str()));
        std::string tmp_table_path = tmp_dir->Str();
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        CommitContextBuilder context_builder2(tmp_table_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context2,
                             context_builder2.AddOption(Options::MANIFEST_FORMAT, "orc")
                                 .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                                 .AddOption(Options::FILE_SYSTEM, "local")
                                 .Finish());
        auto commit2 = FileStoreCommit::Create(std::move(commit_context2));
        CHECK_HOOK_STATUS(commit2.status(), i);
        CHECK_HOOK_STATUS(commit2.value()->Commit(msgs), i);
        commit_run_complete = true;
        io_hook->Clear();
        ASSERT_OK_AND_ASSIGN(bool exist2, file_system_->Exists(PathUtil::JoinPath(
                                              tmp_table_path, "snapshot/snapshot-2")));
        ASSERT_TRUE(exist2);
        break;
    }
    ASSERT_TRUE(commit_run_complete);
}

}  // namespace paimon::test
