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
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/catalog/commit_table_request.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_committable.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/metrics/commit_metrics.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/file_utils.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/fs/local/local_file_system_factory.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
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
    MOCK_METHOD(Result<bool>, Exists, (const std::string& path), (const, override));
};

class GmockFileSystemFactory : public LocalFileSystemFactory {
 public:
    const char* Identifier() const override {
        return "gmock_fs";
    }

    Result<std::unique_ptr<FileSystem>> Create(
        const std::string& path, const std::map<std::string, std::string>& options) const override {
        auto fs = std::make_unique<testing::NiceMock<GmockFileSystem>>();
        auto fs_ptr = fs.get();
        using ::testing::A;
        using ::testing::Invoke;

        ON_CALL(*fs, ListDir(A<const std::string&>(),
                             A<std::vector<std::unique_ptr<BasicFileStatus>>*>()))
            .WillByDefault(
                Invoke([fs_ptr](const std::string& directory,
                                std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) {
                    return fs_ptr->LocalFileSystem::ListDir(directory, file_status_list);
                }));

        ON_CALL(*fs, ReadFile(A<const std::string&>(), A<std::string*>()))
            .WillByDefault(Invoke([fs_ptr](const std::string& path, std::string* content) {
                return fs_ptr->FileSystem::ReadFile(path, content);
            }));

        ON_CALL(*fs, AtomicStore(A<const std::string&>(), A<const std::string&>()))
            .WillByDefault(Invoke([fs_ptr](const std::string& path, const std::string& content) {
                return fs_ptr->FileSystem::AtomicStore(path, content);
            }));

        ON_CALL(*fs, Exists(A<const std::string&>()))
            .WillByDefault(Invoke([fs_ptr](const std::string& path) {
                return fs_ptr->LocalFileSystem::Exists(path);
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
        return CreateManifestEntry(file_name, partition, kind, DataFileMeta::EmptyMinKey(),
                                   DataFileMeta::EmptyMaxKey(), /*level=*/2, /*bucket=*/0);
    }

    ManifestEntry CreateManifestEntry(const std::string& file_name, const BinaryRow& partition,
                                      const FileKind& kind, const BinaryRow& min_key,
                                      const BinaryRow& max_key, int32_t level, int32_t bucket = 0,
                                      int32_t total_buckets = 2) const {
        auto data_file_meta = std::make_shared<DataFileMeta>(
            file_name, 1024, 8, min_key, max_key, SimpleStats::EmptyStats(),
            SimpleStats::EmptyStats(), /*min_seq_no=*/16, /*max_seq_no=*/32,
            /*schema_id=*/1, level,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/3,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        return ManifestEntry(kind, partition, bucket, total_buckets, data_file_meta);
    }

    BinaryRow CreateIntRow(int32_t value) const {
        BinaryRow row(1);
        BinaryRowWriter writer(&row, 20, GetDefaultPool().get());
        writer.WriteInt(0, value);
        writer.Complete();
        return row;
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

    size_t CountFiles(const std::string& dir) const {
        size_t count = 0;
        std::error_code ec;
        for (std::filesystem::directory_iterator it(dir, ec), end; it != end; it.increment(ec)) {
            if (it->is_regular_file(ec)) {
                ++count;
            }
        }
        return count;
    }

    std::shared_ptr<IndexFileMeta> CreateIndexFileMeta(const std::string& file_name,
                                                       const std::string& index_type = "bitmap") {
        return std::make_shared<IndexFileMeta>(index_type, file_name, /*file_size=*/100,
                                               /*row_count=*/5, /*dv_ranges=*/std::nullopt,
                                               /*external_path=*/std::nullopt,
                                               /*global_index_meta=*/std::nullopt);
    }

    std::shared_ptr<IndexFileMeta> CreateGlobalIndexFileMeta(const std::string& file_name,
                                                             int64_t row_range_start,
                                                             int64_t row_range_end) {
        GlobalIndexMeta global_index(row_range_start, row_range_end, /*index_field_id=*/1,
                                     /*extra_field_ids=*/std::nullopt,
                                     std::make_shared<Bytes>("meta", GetDefaultPool().get()));
        return std::make_shared<IndexFileMeta>(
            "HASH", file_name, /*file_size=*/100, /*row_count=*/5,
            /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt, global_index);
    }

    std::shared_ptr<DataFileMeta> CreateLeveledDataFileMeta(const std::string& file_name,
                                                            const BinaryRow& min_key,
                                                            const BinaryRow& max_key, int32_t level,
                                                            int32_t schema_id = 0) {
        return std::make_shared<DataFileMeta>(
            file_name, 1024, 8, min_key, max_key, SimpleStats::EmptyStats(),
            SimpleStats::EmptyStats(), /*min_seq_no=*/16, /*max_seq_no=*/32, schema_id, level,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/std::nullopt,
            /*embedded_index=*/nullptr, FileSource::Append(),
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    }

    std::shared_ptr<DataFileMeta> CreateAppendDataFileMeta(const std::string& file_name,
                                                           int64_t row_count) {
        return std::make_shared<DataFileMeta>(
            file_name, 1024, row_count, DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), /*min_seq_no=*/16,
            /*max_seq_no=*/32,
            /*schema_id=*/1, /*level=*/2,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/std::nullopt,
            /*embedded_index=*/nullptr, FileSource::Append(),
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
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
        EXPECT_OK_AND_ASSIGN(
            [[maybe_unused]] int64_t length,
            in_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
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
        /*total_record_count=*/5, /*delta_record_count=*/5, /*changelog_record_count=*/std::nullopt,
        /*watermark=*/std::nullopt, /*statistics=*/std::nullopt, /*properties=*/std::nullopt,
        /*next_row_id=*/0);
    std::vector<PartitionStatistics> expected_partition_statistics = {
        PartitionStatistics(/*spec=*/{{"f1", "20"}}, /*record_count=*/1, /*file_size_in_bytes=*/541,
                            /*file_count=*/1,
                            /*last_file_creation_time=*/1724090888743l - 28800000l,
                            /*total_buckets=*/-1),
        PartitionStatistics(/*spec=*/{{"f1", "10"}}, /*record_count=*/4,
                            /*file_size_in_bytes=*/1118, /*file_count=*/2,
                            /*last_file_creation_time=*/1724090888727l - 28800000l,
                            /*total_buckets=*/-1)};
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

TEST_F(FileStoreCommitImplTest, TestSnapshotSequenceMaxPropertyMergedOnCommit) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .AddOption(Options::WRITE_SEQUENCE_NUMBER_INIT_MODE, "snapshot")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    std::vector<std::shared_ptr<CommitMessage>> msgs1 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);
    ASSERT_GT(msgs1.size(), 0);
    ASSERT_OK(commit_impl->Commit(msgs1, 1));

    ASSERT_OK_AND_ASSIGN(Snapshot snapshot1, commit_impl->snapshot_manager_->LoadSnapshot(1));
    ASSERT_TRUE(snapshot1.Properties());
    auto iter1 = snapshot1.Properties().value().find("sequence.generation.max-sequence-number");
    ASSERT_TRUE(iter1 != snapshot1.Properties().value().end());
    int64_t max_seq_1 = std::stoll(iter1->second);

    std::vector<std::shared_ptr<CommitMessage>> msgs2 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-02",
                          /*version=*/3);
    ASSERT_GT(msgs2.size(), 0);
    ASSERT_OK(commit_impl->Commit(msgs2, 2));

    ASSERT_OK_AND_ASSIGN(Snapshot snapshot2, commit_impl->snapshot_manager_->LoadSnapshot(2));
    ASSERT_TRUE(snapshot2.Properties());
    auto iter2 = snapshot2.Properties().value().find("sequence.generation.max-sequence-number");
    ASSERT_TRUE(iter2 != snapshot2.Properties().value().end());
    int64_t max_seq_2 = std::stoll(iter2->second);

    ASSERT_GE(max_seq_2, max_seq_1);
}

TEST_F(FileStoreCommitImplTest, TestCommitWithConflictSnapshotAndRetryTenTimes) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("gmock_fs", table_path, {}));
    CommitContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::COMMIT_MAX_RETRIES, "10")
                             .AddOption(Options::COMMIT_MIN_RETRY_WAIT, "1ms")
                             .AddOption(Options::COMMIT_MAX_RETRY_WAIT, "1ms")
                             .WithFileSystem(fs)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    std::string latest_hint = PathUtil::JoinPath(table_path, "snapshot/LATEST");

    auto* mock_fs = dynamic_cast<GmockFileSystem*>(fs.get());
    EXPECT_CALL(*mock_fs, ReadFile(testing::StrEq(latest_hint), testing::_))
        .WillRepeatedly(testing::Invoke([](const std::string& path, std::string* content) {
            *content = "-1";
            return Status::OK();
        }));
    EXPECT_CALL(*mock_fs, ListDir(testing::_, testing::_)).Times(testing::AnyNumber());
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

TEST_F(FileStoreCommitImplTest, TestCommitWithConflictSnapshotAndRetryOnce) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("gmock_fs", table_path, {}));
    CommitContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::COMMIT_MIN_RETRY_WAIT, "1ms")
                             .AddOption(Options::COMMIT_MAX_RETRY_WAIT, "1ms")
                             .WithFileSystem(fs)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    std::string latest_hint = PathUtil::JoinPath(table_path, "snapshot/LATEST");
    auto* mock_fs = dynamic_cast<GmockFileSystem*>(fs.get());
    EXPECT_CALL(*mock_fs, ReadFile(testing::_, testing::_))
        .Times(testing::AnyNumber())
        .WillRepeatedly(testing::Invoke([&](const std::string& path, std::string* content) {
            return mock_fs->FileSystem::ReadFile(path, content);
        }));
    EXPECT_CALL(*mock_fs, ReadFile(testing::StrEq(latest_hint), testing::_))
        .WillRepeatedly(testing::Invoke([](const std::string& path, std::string* content) {
            *content = "-1";
            return Status::OK();
        }));

    EXPECT_CALL(*mock_fs, ListDir(testing::_, testing::_)).Times(testing::AnyNumber());
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

TEST_F(FileStoreCommitImplTest, TestCommitWithAtomicWriteSnapshotTimeoutAndActuallySucceed) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("gmock_fs", table_path, {}));
    CommitContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .WithFileSystem(fs)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    std::string new_snapshot_6 = PathUtil::JoinPath(table_path, "snapshot/snapshot-6");
    auto* mock_fs = dynamic_cast<GmockFileSystem*>(fs.get());
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
    ASSERT_OK(commit->Commit(msgs, /*commit_identifier=*/1));
    std::shared_ptr<Metrics> metrics = commit->GetCommitMetrics();
    ASSERT_TRUE(metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(2u, counter);
    ASSERT_OK_AND_ASSIGN(
        bool exist, file_system_->Exists(PathUtil::JoinPath(table_path, "snapshot/snapshot-6")));
    ASSERT_TRUE(exist);

    CommitContextBuilder context_builder_2(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context_2,
                         context_builder_2.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .WithFileSystem(fs)
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit_2, FileStoreCommit::Create(std::move(commit_context_2)));
    std::string new_snapshot_7 = PathUtil::JoinPath(table_path, "snapshot/snapshot-7");
    EXPECT_CALL(*mock_fs, AtomicStore(testing::StrEq(new_snapshot_7), testing::_))
        .WillOnce(testing::Invoke([&](const std::string& path, const std::string& content) {
            return mock_fs->FileSystem::AtomicStore(path, content);
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
        ASSERT_EQ(0u, counter);
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

TEST_F(FileStoreCommitImplTest, TestRollbackToAsLatest) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);

    // Append three times so the latest snapshot (3) is a superset of snapshot 1.
    const std::string base_path = paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/commit_messages-0";
    for (int32_t i = 1; i <= 3; ++i) {
        std::vector<std::shared_ptr<CommitMessage>> msgs =
            GetCommitMessages(base_path + std::to_string(i), /*version=*/3);
        ASSERT_GT(msgs.size(), 0);
        ASSERT_OK(commit->Commit(msgs, /*commit_identifier=*/i));
    }

    ASSERT_OK_AND_ASSIGN(Snapshot snapshot1, commit_impl->snapshot_manager_->LoadSnapshot(1));
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot3, commit_impl->snapshot_manager_->LoadSnapshot(3));
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestEntry> snapshot1_entries,
                         commit_impl->ReadAddManifestEntries(snapshot1));
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestEntry> snapshot3_entries,
                         commit_impl->ReadAddManifestEntries(snapshot3));

    // Roll back to snapshot 1: the delta only removes files (DELETE branch).
    ASSERT_OK_AND_ASSIGN(bool rolled_back, commit->RollbackToAsLatest(/*target_snapshot_id=*/1));
    ASSERT_TRUE(rolled_back);
    ASSERT_OK_AND_ASSIGN(bool snapshot4_exist, file_system_->Exists(PathUtil::JoinPath(
                                                   table_path_, "snapshot/snapshot-4")));
    ASSERT_TRUE(snapshot4_exist);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot4, commit_impl->snapshot_manager_->LoadSnapshot(4));
    ASSERT_EQ(snapshot4.Id(), 4);
    ASSERT_TRUE(snapshot4.GetCommitKind() == Snapshot::CommitKind::Overwrite());
    ASSERT_EQ(snapshot4.SchemaId(), snapshot1.SchemaId());
    ASSERT_EQ(snapshot4.TotalRecordCount(), snapshot1.TotalRecordCount());
    ASSERT_EQ(snapshot4.NextRowId(),
              FileStoreCommitImpl::MaxNextRowId(snapshot3.NextRowId(), snapshot1.NextRowId()));
    ASSERT_EQ(snapshot4.IndexManifest(), snapshot1.IndexManifest());
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestEntry> snapshot4_entries,
                         commit_impl->ReadAddManifestEntries(snapshot4));
    ASSERT_EQ(CollectFileNames(snapshot4_entries), CollectFileNames(snapshot1_entries));

    // Roll back forward to snapshot 3: the delta only adds files (ADD branch).
    ASSERT_OK_AND_ASSIGN(bool rolled_forward, commit->RollbackToAsLatest(/*target_snapshot_id=*/3));
    ASSERT_TRUE(rolled_forward);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot5, commit_impl->snapshot_manager_->LoadSnapshot(5));
    ASSERT_EQ(snapshot5.Id(), 5);
    ASSERT_TRUE(snapshot5.GetCommitKind() == Snapshot::CommitKind::Overwrite());
    ASSERT_EQ(snapshot5.TotalRecordCount(), snapshot3.TotalRecordCount());
    ASSERT_EQ(snapshot5.IndexManifest(), snapshot3.IndexManifest());
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestEntry> snapshot5_entries,
                         commit_impl->ReadAddManifestEntries(snapshot5));
    ASSERT_EQ(CollectFileNames(snapshot5_entries), CollectFileNames(snapshot3_entries));
}

TEST_F(FileStoreCommitImplTest, TestRollbackToAsLatestNoLatestSnapshotReturnsError) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_NOK_WITH_MSG(commit->RollbackToAsLatest(/*target_snapshot_id=*/1),
                        "Latest snapshot is null, can not roll back.");
}

TEST_F(FileStoreCommitImplTest, TestRollbackToAsLatestTargetNotExistReturnsError) {
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
    ASSERT_OK(commit->Commit(msgs, /*commit_identifier=*/1));
    ASSERT_FALSE(commit->RollbackToAsLatest(/*target_snapshot_id=*/999).ok());
}

TEST_F(FileStoreCommitImplTest, TestRollbackToAsLatestDeletionVectorOnlyChange) {
    // Mirror Java testRollbackToAsLatestDeletionVectorChangeIsInvisibleToStreaming: when the target
    // and the latest snapshot share identical data files and differ only in the index (deletion
    // vector) manifest, the rollback produces an empty data delta and the new snapshot inherits the
    // target's index manifest.
    std::map<std::string, std::string> table_options = {{Options::FILE_FORMAT, "orc"},
                                                        {Options::FILE_SYSTEM, "local"},
                                                        {Options::BUCKET, "2"},
                                                        {Options::BUCKET_KEY, "f2"}};
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(test_root_, table_options));
    arrow::Schema typed_schema(fields_);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "dv_bar"), &schema,
                                   /*partition_keys=*/{"f1"},
                                   /*primary_keys=*/{}, table_options,
                                   /*ignore_if_exists=*/false));
    std::string dv_table_path = PathUtil::JoinPath(test_root_, "foo.db/dv_bar");

    CommitContextBuilder context_builder(dv_table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);

    BinaryRow partition = BinaryRowGenerator::GenerateRow({10}, GetDefaultPool().get());

    // snapshot 1 (target): a single data file, no deletion vectors.
    std::vector<std::shared_ptr<DataFileMeta>> new_files;
    new_files.push_back(CreateAppendDataFileMeta("data-file-1", /*row_count=*/10));
    DataIncrement data_increment(std::move(new_files), {}, {});
    std::shared_ptr<CommitMessage> data_msg = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/0, /*total_buckets=*/2, data_increment, CompactIncrement({}, {}, {}));
    ASSERT_OK(commit->Commit({data_msg}, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(Snapshot target_snapshot, commit_impl->snapshot_manager_->LoadSnapshot(1));

    // snapshot 2 (latest): a deletion-vector-only change on the same data file. The data file is
    // unchanged, only the index manifest gains a deletion vector.
    LinkedHashMap<std::string, DeletionVectorMeta> dv_ranges;
    dv_ranges.insert_or_assign("data-file-1",
                               DeletionVectorMeta(/*data_file_name=*/"data-file-1", /*offset=*/0,
                                                  /*length=*/10, /*cardinality=*/1));
    std::vector<std::shared_ptr<IndexFileMeta>> new_index_files;
    new_index_files.push_back(std::make_shared<IndexFileMeta>(
        DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "dv-index-1", /*file_size=*/100,
        /*row_count=*/1, /*dv_ranges=*/dv_ranges, /*external_path=*/std::nullopt));
    DataIncrement dv_increment({}, {}, {}, std::move(new_index_files), {});
    std::shared_ptr<CommitMessage> dv_msg = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/0, /*total_buckets=*/2, dv_increment, CompactIncrement({}, {}, {}));
    ASSERT_OK(commit->Commit({dv_msg}, /*commit_identifier=*/2));
    ASSERT_OK_AND_ASSIGN(Snapshot latest_snapshot, commit_impl->snapshot_manager_->LoadSnapshot(2));
    ASSERT_TRUE(latest_snapshot.IndexManifest().has_value());

    // Roll back to snapshot 1: the data files are identical, so the delta carries no data change
    // and the new snapshot inherits the target's (empty) index manifest, dropping the deletion
    // vector.
    ASSERT_OK_AND_ASSIGN(bool rolled_back, commit->RollbackToAsLatest(/*target_snapshot_id=*/1));
    ASSERT_TRUE(rolled_back);
    ASSERT_OK_AND_ASSIGN(Snapshot rolled_back_snapshot,
                         commit_impl->snapshot_manager_->LoadSnapshot(3));
    ASSERT_EQ(rolled_back_snapshot.Id(), 3);
    ASSERT_TRUE(rolled_back_snapshot.GetCommitKind() == Snapshot::CommitKind::Overwrite());
    // Empty data delta: no records are added or removed by the rollback.
    ASSERT_EQ(rolled_back_snapshot.DeltaRecordCount(), 0);
    // The new snapshot points back to the target's index manifest.
    ASSERT_EQ(rolled_back_snapshot.IndexManifest(), target_snapshot.IndexManifest());
    // The surviving data files are unchanged relative to the target.
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestEntry> target_entries,
                         commit_impl->ReadAddManifestEntries(target_snapshot));
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestEntry> rolled_back_entries,
                         commit_impl->ReadAddManifestEntries(rolled_back_snapshot));
    ASSERT_EQ(CollectFileNames(rolled_back_entries), CollectFileNames(target_entries));
}

TEST_F(FileStoreCommitImplTest, TestRollbackToAsLatestConcurrentConflictReturnsFalse) {
    // When a concurrent writer wins the race for the next snapshot id, the atomic snapshot commit
    // reports failure (returns false); RollbackToAsLatest surfaces that without error and without
    // advancing the committed snapshot id. The temporary base/delta manifest lists written before
    // the failed commit are intentionally left behind, matching Java (no cleanup on this path).
    //
    // The conflict is a TOCTOU race: snapshot-2 does not exist when LatestSnapshot() resolves the
    // current latest (so it stays snapshot-1), but exists by the time RenamingSnapshotCommit checks
    // before writing. A stateful Exists() mock reproduces exactly that ordering.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("gmock_fs", table_path_, {}));
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .WithFileSystem(fs)
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);

    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);
    ASSERT_GT(msgs.size(), 0);
    ASSERT_OK(commit->Commit(msgs, /*commit_identifier=*/1));
    ASSERT_EQ(commit_impl->last_committed_snapshot_id_, 1);

    // snapshot-2 is invisible the first time it is probed (LatestSnapshot keeps snapshot-1 as the
    // latest) and visible afterwards (RenamingSnapshotCommit sees the conflicting file).
    const std::string next_snapshot = PathUtil::JoinPath(table_path_, "snapshot/snapshot-2");
    auto* mock_fs = dynamic_cast<GmockFileSystem*>(fs.get());
    ASSERT_TRUE(mock_fs);
    EXPECT_CALL(*mock_fs, Exists(testing::_))
        .Times(testing::AnyNumber())
        .WillRepeatedly(testing::Invoke(
            [mock_fs](const std::string& path) { return mock_fs->LocalFileSystem::Exists(path); }));
    EXPECT_CALL(*mock_fs, Exists(testing::StrEq(next_snapshot)))
        .WillOnce(testing::Return(Result<bool>(false)))
        .WillRepeatedly(testing::Return(Result<bool>(true)));

    const std::string manifest_dir = PathUtil::JoinPath(table_path_, "manifest");
    const size_t manifest_count_before = CountFiles(manifest_dir);

    ASSERT_OK_AND_ASSIGN(bool rolled_back, commit->RollbackToAsLatest(/*target_snapshot_id=*/1));
    ASSERT_FALSE(rolled_back);
    // The committed snapshot id must not advance on the failed rollback.
    ASSERT_EQ(commit_impl->last_committed_snapshot_id_, 1);
    // The temporary base/delta manifest lists are leaked (not cleaned up) on the failure path.
    ASSERT_GT(CountFiles(manifest_dir), manifest_count_before);
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
        ASSERT_OK_AND_ASSIGN(bool exist2, file_system_->Exists(PathUtil::JoinPath(
                                              table_path_, "snapshot/snapshot-2")));
        // termination conditions:
        // 1. hit IOHook in commit hint, at this point, the snapshot is already committed
        // 2. snapshot file exists, which means atomic store succeeded even if a later IO failed
        if (HitIOHookInCommitHint(status) || exist2) {
            scanned_all_io_hook = true;
            ASSERT_TRUE(exist2);
            break;
        }
        // For some IO-hook positions, retries may be exhausted and return a generic non-IOHook
        // status while the snapshot is still not committed. Keep scanning next positions.
        if (!HitIOHook(status)) {
            continue;
        }
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
    ASSERT_OK(commit_impl->TryOverwrite(partitions, changes, /*index_entries=*/{},
                                        /*commit_identifier=*/1, std::nullopt,
                                        /*properties=*/{}));
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
    ASSERT_OK(commit_impl->TryOverwrite(partitions, changes, /*index_entries=*/{},
                                        /*commit_identifier=*/0, std::nullopt,
                                        /*properties=*/{}));
    ASSERT_OK_AND_ASSIGN(auto snapshot1, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries1, commit_impl->GetAllFiles(snapshot1.value(), {}));
    ASSERT_EQ(1u, entries1.size());
    ASSERT_EQ("new_file_1", entries1[0].FileName());
    ASSERT_EQ(FileKind::Add(), entries1[0].Kind());
    std::vector<ManifestEntry> changes2;
    changes2.push_back(CreateManifestEntry("new_file_2", FileKind::Add()));
    ASSERT_OK(commit_impl->TryOverwrite(partitions, changes2, /*index_entries=*/{},
                                        /*commit_identifier=*/1, std::nullopt,
                                        /*properties=*/{}));
    ASSERT_OK_AND_ASSIGN(auto snapshot2, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries2, commit_impl->GetAllFiles(snapshot2.value(), {}));
    ASSERT_EQ(1u, entries2.size());
    ASSERT_EQ("new_file_2", entries2[0].FileName());
    ASSERT_EQ(FileKind::Add(), entries2[0].Kind());
}

TEST_F(FileStoreCommitImplTest, TestTryOverwriteWithProperties) {
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
    changes.push_back(CreateManifestEntry("new_file_with_properties", FileKind::Add()));
    std::vector<std::map<std::string, std::string>> partitions = {{{"f1", "10"}}};
    std::map<std::string, std::string> properties = {{"overwrite-prop", "v1"}};
    ASSERT_OK(commit_impl->TryOverwrite(partitions, changes, /*index_entries=*/{},
                                        /*commit_identifier=*/0, std::nullopt, properties));

    ASSERT_OK_AND_ASSIGN(auto snapshot, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_TRUE(snapshot->Properties().has_value());
    auto iter = snapshot->Properties()->find("overwrite-prop");
    ASSERT_TRUE(iter != snapshot->Properties()->end());
    ASSERT_EQ("v1", iter->second);
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
    ASSERT_OK(commit_impl->TryOverwrite(partitions, changes, /*index_entries=*/{},
                                        /*commit_identifier=*/0, std::nullopt,
                                        /*properties=*/{}));
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
    ASSERT_OK(commit_impl->TryOverwrite(partitions, changes2, /*index_entries=*/{},
                                        /*commit_identifier=*/2, std::nullopt,
                                        /*properties=*/{}));
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

TEST_F(FileStoreCommitImplTest, TestTruncateTable) {
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

    // Truncate overwrites all partitions with no new files, deleting every existing file.
    ASSERT_OK(commit->TruncateTable(/*commit_identifier=*/1));

    ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(table_path_ + "/snapshot/snapshot-2"));
    ASSERT_TRUE(exist);
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, commit_impl->snapshot_manager_->LoadSnapshot(2));
    ASSERT_EQ(Snapshot::CommitKind::Overwrite(), snapshot.GetCommitKind());
    std::vector<ManifestFileMeta> manifests;
    ASSERT_OK(commit_impl->manifest_list_->ReadDeltaManifests(snapshot, &manifests));
    ASSERT_EQ(1, manifests.size());
    ASSERT_EQ(0, manifests[0].NumAddedFiles());
    // The append_09 fixture contains 3 data files spread across partitions f1=10 and f1=20.
    ASSERT_EQ(3, manifests[0].NumDeletedFiles());
}

TEST_F(FileStoreCommitImplTest, TestAbortDeletesDataAndIndexFiles) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    const BinaryRow partition = CreateIntRow(10);
    const int32_t bucket = 0;
    auto new_data_file = CreateAppendDataFileMeta("abort-new-data", 1);
    auto compact_data_file = CreateAppendDataFileMeta("abort-compact-data", 1);
    auto new_index_file = CreateIndexFileMeta("abort-new-index");
    auto compact_index_file = CreateIndexFileMeta("abort-compact-index");

    DataIncrement data_increment(/*new_files=*/{new_data_file}, /*deleted_files=*/{},
                                 /*changelog_files=*/{}, /*new_index_files=*/{new_index_file},
                                 /*deleted_index_files=*/{});
    CompactIncrement compact_increment(/*compact_before=*/{}, /*compact_after=*/{compact_data_file},
                                       /*changelog_files=*/{},
                                       /*new_index_files=*/{compact_index_file},
                                       /*deleted_index_files=*/{});
    std::shared_ptr<CommitMessage> message = std::make_shared<CommitMessageImpl>(
        partition, bucket, /*total_buckets=*/2, data_increment, compact_increment);

    // Materialize the referenced files so we can observe them being cleaned up.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFilePathFactory> data_pf,
                         commit_impl->path_factory_->CreateDataFilePathFactory(partition, bucket));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexPathFactory> index_pf,
                         commit_impl->path_factory_->CreateIndexFileFactory(partition, bucket));
    std::vector<std::string> paths = {
        data_pf->ToPath(new_data_file), data_pf->ToPath(compact_data_file),
        index_pf->ToPath(new_index_file), index_pf->ToPath(compact_index_file)};
    for (const auto& path : paths) {
        ASSERT_OK(file_system_->WriteFile(path, /*content=*/"", /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(path));
        ASSERT_TRUE(exist);
    }

    ASSERT_OK(commit_impl->Abort({message}));

    for (const auto& path : paths) {
        ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(path));
        ASSERT_FALSE(exist);
    }
}

TEST_F(FileStoreCommitImplTest, AbortIgnoresMissingFilesAndFailsForNonImplMessage) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    // Deleting files that were never written is a best-effort no-op, not an error.
    DataIncrement data_increment(/*new_files=*/{CreateAppendDataFileMeta("abort-missing", 1)},
                                 /*deleted_files=*/{}, /*changelog_files=*/{});
    std::shared_ptr<CommitMessage> message =
        std::make_shared<CommitMessageImpl>(CreateIntRow(10), /*bucket=*/0, /*total_buckets=*/2,
                                            data_increment, CompactIncrement({}, {}, {}));
    ASSERT_OK(commit_impl->Abort({message}));

    // A commit message that is not a CommitMessageImpl is rejected.
    ASSERT_NOK_WITH_MSG(commit_impl->Abort({std::make_shared<CommitMessage>()}),
                        "fail to cast commit message to impl");
}

TEST_F(FileStoreCommitImplTest, AbortIgnoresDeleteFailures) {
    // A delete that fails with an IO error (e.g. permission denied) must be swallowed by
    // best-effort cleanup, mirroring Java deleteQuietly: Abort still returns OK and does not
    // propagate the failure.
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);

    const BinaryRow partition = CreateIntRow(10);
    const int32_t bucket = 0;
    auto new_data_file = CreateAppendDataFileMeta("abort-io-fail-data", 1);
    DataIncrement data_increment(/*new_files=*/{new_data_file}, /*deleted_files=*/{},
                                 /*changelog_files=*/{});
    std::shared_ptr<CommitMessage> message = std::make_shared<CommitMessageImpl>(
        partition, bucket, /*total_buckets=*/2, data_increment, CompactIncrement({}, {}, {}));

    // Materialize the file so we can observe that a failed delete leaves it untouched.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFilePathFactory> data_pf,
                         commit_impl->path_factory_->CreateDataFilePathFactory(partition, bucket));
    const std::string data_path = data_pf->ToPath(new_data_file);
    ASSERT_OK(file_system_->WriteFile(data_path, /*content=*/"", /*overwrite=*/false));

    // Fault-inject an IO error on the delete. Abort performs no local file IO before its delete
    // loop, so position 0 lands on the delete itself. The error must be swallowed. Clearing on
    // scope exit keeps the process-global hook from leaking into other tests.
    auto io_hook = IOHook::GetInstance();
    ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
    io_hook->Reset(/*pos=*/0, IOHook::Mode::RETURN_ERROR);
    ASSERT_OK(commit_impl->Abort({message}));
    io_hook->Clear();

    // Because the delete failed, the file remains on disk.
    ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(data_path));
    ASSERT_TRUE(exist);
}

TEST_F(FileStoreCommitImplTest, TestTruncateEmptyTable) {
    // Truncating a table that has never been committed succeeds and produces an OVERWRITE snapshot
    // with no added and no deleted files: there is nothing to overwrite, but the overwrite is still
    // materialized.
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .IgnoreEmptyCommit(true)
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));

    ASSERT_OK(commit->TruncateTable(/*commit_identifier=*/1));

    ASSERT_OK_AND_ASSIGN(bool exist, file_system_->Exists(table_path_ + "/snapshot/snapshot-1"));
    ASSERT_TRUE(exist);
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, commit_impl->snapshot_manager_->LoadSnapshot(1));
    ASSERT_EQ(Snapshot::CommitKind::Overwrite(), snapshot.GetCommitKind());
    std::vector<ManifestFileMeta> manifests;
    ASSERT_OK(commit_impl->manifest_list_->ReadDeltaManifests(snapshot, &manifests));
    for (const auto& manifest : manifests) {
        ASSERT_EQ(0, manifest.NumAddedFiles());
        ASSERT_EQ(0, manifest.NumDeletedFiles());
    }
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
    ASSERT_OK_AND_ASSIGN(ManifestEntryChanges changes, commit_impl->CollectChanges(msgs));
    ASSERT_EQ(changes.append_table_files.size(), 3u);
    ASSERT_EQ(changes.append_changelog.size(), 0u);
    ASSERT_EQ(changes.compact_table_files.size(), 0u);
    ASSERT_EQ(changes.compact_changelog.size(), 0u);
    ASSERT_EQ(changes.append_index_files.size(), 0u);
    ASSERT_EQ(changes.compact_index_files.size(), 0u);
    ASSERT_EQ(changes.append_table_files[0].Kind(), FileKind::Add());
    ASSERT_EQ(changes.append_table_files[0].Bucket(), 0);
    ASSERT_EQ(changes.append_table_files[0].TotalBuckets(), 10);
    ASSERT_EQ(changes.append_table_files[0].Level(), 0);
    ASSERT_EQ(changes.append_table_files[0].FileName(),
              "data-51a45441-6037-4af3-b67b-5cefd75dc6f2-0.orc");
    ASSERT_EQ(changes.append_table_files[1].Kind(), FileKind::Add());
    ASSERT_EQ(changes.append_table_files[1].Bucket(), 1);
    ASSERT_EQ(changes.append_table_files[1].TotalBuckets(), 10);
    ASSERT_EQ(changes.append_table_files[1].Level(), 0);
    ASSERT_EQ(changes.append_table_files[1].FileName(),
              "data-6828284c-e707-49b5-af6b-69be79af120c-0.orc");
    ASSERT_EQ(changes.append_table_files[2].Kind(), FileKind::Add());
    ASSERT_EQ(changes.append_table_files[2].Bucket(), 0);
    ASSERT_EQ(changes.append_table_files[2].TotalBuckets(), 10);
    ASSERT_EQ(changes.append_table_files[2].Level(), 0);
    ASSERT_EQ(changes.append_table_files[2].FileName(),
              "data-8dc7f04c-3c98-48b2-9d56-834d746c4a40-0.orc");
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

TEST_F(FileStoreCommitImplTest, TestFilterCommittedRejectsDuplicateIdentifiers) {
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
    std::vector<std::shared_ptr<CommitMessage>> msgs2 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/"
                              "commit_messages-02",
                          /*version=*/3);

    auto committable1 = commit_impl->CreateManifestCommittable(1, msgs1, std::nullopt);
    auto committable2 = commit_impl->CreateManifestCommittable(1, msgs2, std::nullopt);

    std::vector<std::shared_ptr<ManifestCommittable>> committables = {committable1, committable2};
    ASSERT_NOK_WITH_MSG(commit_impl->FilterCommitted(committables),
                        "Committables must be sorted according to identifiers before filtering");
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

TEST_F(FileStoreCommitImplTest, FilterAndCommitWithCompactedChangelogFakePath) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    const BinaryRow partition = CreateIntRow(10);
    const std::string base_name = "compacted-changelog-8e049c65-5ce4-4ce7-b1b0-78ce694ab351";
    const std::string fake_name = base_name + "$0-39253-39253-35699.cc-parquet";
    const std::string real_name = base_name + "$0-39253.cc-parquet";

    ASSERT_OK(PrepareFakeFiles({"/f1=10/bucket-0/" + real_name}));

    auto fake_changelog_file = CreateAppendDataFileMeta(fake_name, /*row_count=*/1);
    DataIncrement data_increment(
        /*new_files=*/{}, /*deleted_files=*/{},
        /*changelog_files=*/{fake_changelog_file}, /*new_index_files=*/{},
        /*deleted_index_files=*/{});
    CompactIncrement compact_increment(/*compact_before=*/{}, /*compact_after=*/{},
                                       /*changelog_files=*/{});
    std::shared_ptr<CommitMessage> message = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/1, /*total_buckets=*/2, data_increment, compact_increment);

    std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>> inputs;
    inputs[1] = {message};
    ASSERT_OK_AND_ASSIGN(int32_t actual_committed, commit_impl->FilterAndCommit(inputs));
    ASSERT_EQ(1u, actual_committed);
}

TEST_F(FileStoreCommitImplTest, FilterAndCommitSkipCompactBeforeFileCheck) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    const BinaryRow partition = CreateIntRow(10);
    const std::string compact_before_name = "missing-compact-before.orc";
    const std::string compact_after_name = "existing-compact-after.orc";
    ASSERT_OK(PrepareFakeFiles({"/f1=10/bucket-0/" + compact_after_name}));

    auto compact_before_file = CreateAppendDataFileMeta(compact_before_name, /*row_count=*/1);
    auto compact_after_file = CreateAppendDataFileMeta(compact_after_name, /*row_count=*/1);
    DataIncrement data_increment(
        /*new_files=*/{}, /*deleted_files=*/{}, /*changelog_files=*/{}, /*new_index_files=*/{},
        /*deleted_index_files=*/{});
    CompactIncrement compact_increment(/*compact_before=*/{compact_before_file},
                                       /*compact_after=*/{compact_after_file},
                                       /*changelog_files=*/{});
    std::shared_ptr<CommitMessage> message = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/0, /*total_buckets=*/2, data_increment, compact_increment);

    std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>> inputs;
    inputs[1] = {message};
    ASSERT_OK_AND_ASSIGN(int32_t actual_committed, commit_impl->FilterAndCommit(inputs));
    ASSERT_EQ(1u, actual_committed);
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

TEST_F(FileStoreCommitImplTest, TestCommitWithIndexFiles) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));
    const BinaryRow partition = CreateIntRow(10);

    std::vector<std::shared_ptr<IndexFileMeta>> new_index_files;
    new_index_files.push_back(CreateIndexFileMeta("bitmap-index-commit-1"));
    DataIncrement data_increment({}, {}, {}, std::move(new_index_files), {});
    std::shared_ptr<CommitMessage> msg = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/0, /*total_buckets=*/2, data_increment, CompactIncrement({}, {}, {}));

    ASSERT_OK(commit_impl->Commit({msg}, 1));
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, commit_impl->snapshot_manager_->LoadSnapshot(1));
    ASSERT_EQ(Snapshot::CommitKind::Append(), snapshot.GetCommitKind());
    ASSERT_TRUE(snapshot.IndexManifest());

    std::vector<IndexManifestEntry> index_entries;
    ASSERT_OK(commit_impl->index_manifest_file_->Read(snapshot.IndexManifest().value(),
                                                      /*filter=*/nullptr, &index_entries));
    ASSERT_EQ(1u, index_entries.size());
    ASSERT_EQ("bitmap-index-commit-1", index_entries[0].index_file->FileName());
}

TEST_F(FileStoreCommitImplTest, TestCommitWithGlobalIndexFilesChecksConflicts) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .AddOption(Options::ROW_TRACKING_ENABLED, "true")
                             .AddOption(Options::DATA_EVOLUTION_ENABLED, "true")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));
    const BinaryRow partition = CreateIntRow(10);

    DataIncrement data_increment_1({CreateAppendDataFileMeta("data-with-row-id", 10)}, {}, {});
    std::shared_ptr<CommitMessage> msg_1 =
        std::make_shared<CommitMessageImpl>(partition, /*bucket=*/0, /*total_buckets=*/2,
                                            data_increment_1, CompactIncrement({}, {}, {}));
    ASSERT_OK(commit_impl->Commit({msg_1}, 1));

    std::vector<std::shared_ptr<IndexFileMeta>> global_index_files;
    global_index_files.push_back(CreateGlobalIndexFileMeta("global-index-out-of-range",
                                                           /*row_range_start=*/0,
                                                           /*row_range_end=*/10));
    DataIncrement data_increment_2({}, {}, {}, std::move(global_index_files), {});
    std::shared_ptr<CommitMessage> msg_2 =
        std::make_shared<CommitMessageImpl>(partition, /*bucket=*/0, /*total_buckets=*/2,
                                            data_increment_2, CompactIncrement({}, {}, {}));

    ASSERT_NOK_WITH_MSG(commit_impl->Commit({msg_2}, 2), "Global index row ID existence conflict");
}

TEST_F(FileStoreCommitImplTest, TestCommitWithCompactIndexFiles) {
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
    const BinaryRow partition = CreateIntRow(10);

    std::vector<std::shared_ptr<IndexFileMeta>> compact_index_files;
    compact_index_files.push_back(CreateIndexFileMeta("bitmap-index-commit-compact-1"));
    DataIncrement data_increment({}, {}, {});
    CompactIncrement compact_increment({}, {}, {}, std::move(compact_index_files), {});
    std::shared_ptr<CommitMessage> msg = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/0, /*total_buckets=*/2, data_increment, compact_increment);

    ASSERT_OK(commit_impl->Commit({msg}, 1));
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, commit_impl->snapshot_manager_->LoadSnapshot(1));
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot.GetCommitKind());
    ASSERT_TRUE(snapshot.IndexManifest());

    std::vector<IndexManifestEntry> index_entries;
    ASSERT_OK(commit_impl->index_manifest_file_->Read(snapshot.IndexManifest().value(),
                                                      /*filter=*/nullptr, &index_entries));
    ASSERT_EQ(1u, index_entries.size());
    ASSERT_EQ("bitmap-index-commit-compact-1", index_entries[0].index_file->FileName());
}

TEST_F(FileStoreCommitImplTest, TestCommitWithDeletedIndexFiles) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));
    const BinaryRow partition = CreateIntRow(10);

    std::vector<std::shared_ptr<IndexFileMeta>> new_index_files;
    new_index_files.push_back(CreateIndexFileMeta("bitmap-index-delete-1"));
    DataIncrement data_increment_1({}, {}, {}, std::move(new_index_files), {});
    std::shared_ptr<CommitMessage> msg_1 =
        std::make_shared<CommitMessageImpl>(partition, /*bucket=*/0, /*total_buckets=*/2,
                                            data_increment_1, CompactIncrement({}, {}, {}));
    ASSERT_OK(commit_impl->Commit({msg_1}, 1));

    std::vector<std::shared_ptr<IndexFileMeta>> deleted_index_files;
    deleted_index_files.push_back(CreateIndexFileMeta("bitmap-index-delete-1"));
    DataIncrement data_increment_2({}, {}, {}, {}, std::move(deleted_index_files));
    std::shared_ptr<CommitMessage> msg_2 =
        std::make_shared<CommitMessageImpl>(partition, /*bucket=*/0, /*total_buckets=*/2,
                                            data_increment_2, CompactIncrement({}, {}, {}));
    ASSERT_OK(commit_impl->Commit({msg_2}, 2));

    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, commit_impl->snapshot_manager_->LoadSnapshot(2));
    ASSERT_TRUE(snapshot.IndexManifest());

    std::vector<IndexManifestEntry> index_entries;
    ASSERT_OK(commit_impl->index_manifest_file_->Read(snapshot.IndexManifest().value(),
                                                      /*filter=*/nullptr, &index_entries));
    ASSERT_TRUE(index_entries.empty());
}

TEST_F(FileStoreCommitImplTest, TestCommitWithCompactDeletedIndexFiles) {
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
    const BinaryRow partition = CreateIntRow(10);

    std::vector<std::shared_ptr<IndexFileMeta>> new_index_files;
    new_index_files.push_back(CreateIndexFileMeta("bitmap-index-compact-delete-1"));
    DataIncrement data_increment_1({}, {}, {}, std::move(new_index_files), {});
    std::shared_ptr<CommitMessage> msg_1 =
        std::make_shared<CommitMessageImpl>(partition, /*bucket=*/0, /*total_buckets=*/2,
                                            data_increment_1, CompactIncrement({}, {}, {}));
    ASSERT_OK(commit_impl->Commit({msg_1}, 1));

    std::vector<std::shared_ptr<IndexFileMeta>> deleted_index_files;
    deleted_index_files.push_back(CreateIndexFileMeta("bitmap-index-compact-delete-1"));
    DataIncrement data_increment_2({}, {}, {});
    CompactIncrement compact_increment({}, {}, {}, {}, std::move(deleted_index_files));
    std::shared_ptr<CommitMessage> msg_2 = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/0, /*total_buckets=*/2, data_increment_2, compact_increment);
    ASSERT_OK(commit_impl->Commit({msg_2}, 2));

    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, commit_impl->snapshot_manager_->LoadSnapshot(2));
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot.GetCommitKind());
    ASSERT_TRUE(snapshot.IndexManifest());

    std::vector<IndexManifestEntry> index_entries;
    ASSERT_OK(commit_impl->index_manifest_file_->Read(snapshot.IndexManifest().value(),
                                                      /*filter=*/nullptr, &index_entries));
    ASSERT_TRUE(index_entries.empty());
}

TEST_F(FileStoreCommitImplTest, TestOverwriteWithCompactIndexFiles) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));
    const BinaryRow partition = CreateIntRow(10);

    std::vector<std::shared_ptr<IndexFileMeta>> compact_index_files;
    compact_index_files.push_back(CreateIndexFileMeta("bitmap-index-compact-1"));
    DataIncrement data_increment({}, {}, {});
    CompactIncrement compact_increment({}, {}, {}, std::move(compact_index_files), {});
    std::shared_ptr<CommitMessage> msg = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/0, /*total_buckets=*/2, data_increment, compact_increment);

    ASSERT_OK(commit_impl->Overwrite({}, {msg}, 1));

    ASSERT_OK_AND_ASSIGN(Snapshot compact_snapshot,
                         commit_impl->snapshot_manager_->LoadSnapshot(1));
    ASSERT_EQ(Snapshot::CommitKind::Compact(), compact_snapshot.GetCommitKind());
    ASSERT_TRUE(compact_snapshot.IndexManifest());

    std::vector<IndexManifestEntry> index_entries;
    ASSERT_OK(commit_impl->index_manifest_file_->Read(compact_snapshot.IndexManifest().value(),
                                                      /*filter=*/nullptr, &index_entries));
    ASSERT_EQ(1u, index_entries.size());
    ASSERT_EQ("bitmap-index-compact-1", index_entries[0].index_file->FileName());
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

TEST_F(FileStoreCommitImplTest, TestFilterAndOverwriteWithCompactIndexFiles) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));
    const BinaryRow partition = CreateIntRow(10);

    std::vector<std::shared_ptr<IndexFileMeta>> compact_index_files;
    compact_index_files.push_back(CreateIndexFileMeta("bitmap-index-filter-compact-1"));
    DataIncrement data_increment({}, {}, {});
    CompactIncrement compact_increment({}, {}, {}, std::move(compact_index_files), {});
    std::shared_ptr<CommitMessage> msg = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/0, /*total_buckets=*/2, data_increment, compact_increment);

    ASSERT_OK_AND_ASSIGN(int32_t actual_commit, commit_impl->FilterAndOverwrite({}, {msg}, 1, 10));
    ASSERT_EQ(1, actual_commit);
    ASSERT_OK_AND_ASSIGN(actual_commit, commit_impl->FilterAndOverwrite({}, {msg}, 1, 5));
    ASSERT_EQ(0, actual_commit);

    ASSERT_OK_AND_ASSIGN(Snapshot compact_snapshot,
                         commit_impl->snapshot_manager_->LoadSnapshot(1));
    ASSERT_EQ(Snapshot::CommitKind::Compact(), compact_snapshot.GetCommitKind());
    ASSERT_TRUE(compact_snapshot.Watermark());
    ASSERT_EQ(10, compact_snapshot.Watermark().value());
    ASSERT_TRUE(compact_snapshot.IndexManifest());

    std::vector<IndexManifestEntry> index_entries;
    ASSERT_OK(commit_impl->index_manifest_file_->Read(compact_snapshot.IndexManifest().value(),
                                                      /*filter=*/nullptr, &index_entries));
    ASSERT_EQ(1u, index_entries.size());
    ASSERT_EQ("bitmap-index-filter-compact-1", index_entries[0].index_file->FileName());
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
    ASSERT_OK(commit_impl->Overwrite(partitions, msgs2, 2));
    ASSERT_OK_AND_ASSIGN(auto snapshot1, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries1, commit_impl->GetAllFiles(snapshot1.value(), {}));
    ASSERT_EQ(2u, entries1.size());
    std::set<std::string> file_names = CollectFileNames(entries1);
    ASSERT_TRUE(IsStringInSet(file_names, "data-fd1d2255-43f2-4534-b4cc-08b29e662940-0.orc"));
    ASSERT_TRUE(IsStringInSet(file_names, "data-7b3f4cc7-116b-4d2f-9c62-5dadc1f11bcb-0.orc"));
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

    // Java parity: overwrite provider adds DELETE(old) + ADD(newChanges) without dedup,
    // so same-file overwrite fails on duplicate add.
    ASSERT_NOK_WITH_MSG(commit_impl->Overwrite({}, msgs1, 2), "Trying to add file");
}

TEST_F(FileStoreCommitImplTest, TestAppendDiscardDuplicateFiles) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .AddOption(Options::COMMIT_DISCARD_DUPLICATE_FILES, "true")
                             .Finish());

    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);

    ASSERT_OK(commit_impl->Commit(msgs, 1));
    ASSERT_OK_AND_ASSIGN(auto snapshot1, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries1, commit_impl->GetAllFiles(snapshot1.value(), {}));
    std::set<std::string> file_names_before = CollectFileNames(entries1);
    ASSERT_FALSE(file_names_before.empty());

    // Committing exactly the same append files should be accepted and filtered out when
    // commit.discard-duplicate-files=true.
    ASSERT_OK(commit_impl->Commit(msgs, 2));
    ASSERT_OK_AND_ASSIGN(auto snapshot2, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto entries2, commit_impl->GetAllFiles(snapshot2.value(), {}));
    std::set<std::string> file_names_after = CollectFileNames(entries2);
    ASSERT_EQ(file_names_before, file_names_after);
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

TEST_F(FileStoreCommitImplTest, TestObjectStoreAllowedWithRESTCatalogCommit) {
    // Verify: the object store check in FileStoreCommit::Create is skipped when
    // UseRESTCatalogCommit is true. We can't use an actual oss:// path in unit
    // tests (LocalFileSystem rejects the scheme), but we verify the condition
    // by confirming that the "enable-object-store-commit-in-inte-test" flag is
    // not needed when UseRESTCatalogCommit is enabled. End-to-end oss:// testing
    // is covered by duckdb-paimon integration tests.
    ASSERT_OK_AND_ASSIGN(bool is_oss, FileSystem::IsObjectStore("oss://bucket/path"));
    ASSERT_TRUE(is_oss);

    // REST commit with local path should work without the object store flag
    CommitContextBuilder builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(
        auto ctx,
        builder.AddOption(Options::MANIFEST_FORMAT, "orc").UseRESTCatalogCommit(true).Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(ctx)));

    auto msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          3);
    ASSERT_GT(msgs.size(), 0);
    ASSERT_OK(commit->Commit(msgs));
    ASSERT_OK_AND_ASSIGN(auto json, commit->GetLastCommitTableRequest());
    ASSERT_FALSE(json.empty());
}

TEST_F(FileStoreCommitImplTest, TestFixedBucketPKTableCommitAllowed) {
    auto pk_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(pk_dir);
    std::string pk_root = pk_dir->Str();
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(pk_root, {}));
    ASSERT_OK(catalog->CreateDatabase("db", {}, false));

    arrow::Schema pk_schema(
        {arrow::field("pk", arrow::int32()), arrow::field("val", arrow::utf8())});
    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(pk_schema, &arrow_schema).ok());
    std::map<std::string, std::string> table_options = {{Options::BUCKET, "4"}};
    ASSERT_OK(catalog->CreateTable(Identifier("db", "pk_tbl"), &arrow_schema,
                                   /*partition_keys=*/{}, /*primary_keys=*/{"pk"}, table_options,
                                   /*ignore_if_exists=*/false));

    std::string pk_table_path = PathUtil::JoinPath(pk_root, "db.db/pk_tbl");

    CommitContextBuilder builder(pk_table_path, "test_user");
    builder.AddOption(Options::FILE_SYSTEM, "local").UseRESTCatalogCommit(true);
    ASSERT_OK_AND_ASSIGN(auto commit_context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto committer, FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_TRUE(committer != nullptr);
}

TEST_F(FileStoreCommitImplTest, ValidateCommitOptionsRejectsUnsupportedOptions) {
    const std::vector<std::string> unsupported_keys = {
        "commit.strict-mode.last-safe-snapshot", "manifest.delete-file-drop-stats",
        "sequence.snapshot-ordering", "pk-clustering-override"};
    for (const auto& key : unsupported_keys) {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({{key, "true"}}));
        ASSERT_NOK_WITH_MSG(FileStoreCommitImpl::ValidateCommitOptions(options),
                            "not supported by C++ commit path");
    }

    // Supported options should validate successfully.
    ASSERT_OK_AND_ASSIGN(CoreOptions ok_options,
                         CoreOptions::FromMap({{Options::FILE_SYSTEM, "local"}}));
    ASSERT_OK(FileStoreCommitImpl::ValidateCommitOptions(ok_options));
}

TEST_F(FileStoreCommitImplTest, DropPartitionWithEmptyPartitionsFails) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_NOK_WITH_MSG(commit->DropPartition({}, /*commit_identifier=*/1),
                        "partitions list cannot be empty");
}

TEST_F(FileStoreCommitImplTest, FilterAndCommitMultipleIdentifiersAndEmptyInput) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    // Empty input map takes the FilterCommitted fast-exit path (nothing to commit).
    ASSERT_OK_AND_ASSIGN(int32_t committed_empty, commit_impl->FilterAndCommit({}, 1));
    ASSERT_EQ(0, committed_empty);

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
    std::vector<std::shared_ptr<CommitMessage>> msgs2 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-02",
                          /*version=*/3);

    // Two identifiers in a single call exercises the sort-by-identifier comparator.
    std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>> inputs;
    inputs[2] = msgs2;
    inputs[1] = msgs1;
    ASSERT_OK_AND_ASSIGN(int32_t committed, commit_impl->FilterAndCommit(inputs, 5));
    ASSERT_EQ(2, committed);
}

TEST_F(FileStoreCommitImplTest, CheckFilesExistenceFailsForNonImplCommitMessage) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    auto committable = std::make_shared<ManifestCommittable>(/*identifier=*/1);
    committable->AddFileCommittable(std::make_shared<CommitMessage>());
    ASSERT_NOK_WITH_MSG(commit_impl->CheckFilesExistence({committable}),
                        "fail to cast commit message to impl");
}

TEST_F(FileStoreCommitImplTest, CheckFilesExistenceCollectsIndexFilePaths) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));
    const BinaryRow partition = CreateIntRow(10);

    // New index files from both the data increment and the compact increment must be
    // collected for the existence check.
    DataIncrement data_increment(/*new_files=*/{}, /*deleted_files=*/{}, /*changelog_files=*/{},
                                 /*new_index_files=*/{CreateIndexFileMeta("new-index-missing")},
                                 /*deleted_index_files=*/{});
    CompactIncrement compact_increment(
        /*compact_before=*/{}, /*compact_after=*/{}, /*changelog_files=*/{},
        /*new_index_files=*/{CreateIndexFileMeta("compact-index-missing")},
        /*deleted_index_files=*/{});
    std::shared_ptr<CommitMessage> message = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/0, /*total_buckets=*/2, data_increment, compact_increment);

    auto committable = std::make_shared<ManifestCommittable>(/*identifier=*/1);
    committable->AddFileCommittable(message);
    // The referenced index files were never written, so the existence check reports them missing.
    ASSERT_NOK_WITH_MSG(commit_impl->CheckFilesExistence({committable}), "have been deleted");
}

TEST_F(FileStoreCommitImplTest, OverwriteStaticPartitionValidatesFileOwnership) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .AddOption(Options::DYNAMIC_PARTITION_OVERWRITE, "false")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    // Files whose partition matches the overwrite target are accepted.
    DataIncrement matching_increment({CreateAppendDataFileMeta("static-f1-10", 1)}, {}, {});
    std::shared_ptr<CommitMessage> matching_msg =
        std::make_shared<CommitMessageImpl>(CreateIntRow(10), /*bucket=*/0, /*total_buckets=*/2,
                                            matching_increment, CompactIncrement({}, {}, {}));
    ASSERT_OK(commit_impl->Overwrite({{"f1", "10"}}, {matching_msg}, /*commit_identifier=*/1));

    // Files belonging to a different partition than the overwrite target are rejected.
    DataIncrement mismatching_increment({CreateAppendDataFileMeta("static-f1-20", 1)}, {}, {});
    std::shared_ptr<CommitMessage> mismatching_msg =
        std::make_shared<CommitMessageImpl>(CreateIntRow(20), /*bucket=*/0, /*total_buckets=*/2,
                                            mismatching_increment, CompactIncrement({}, {}, {}));
    ASSERT_NOK_WITH_MSG(
        commit_impl->Overwrite({{"f1", "10"}}, {mismatching_msg}, /*commit_identifier=*/2),
        "does not belong to this partition");
}

TEST_F(FileStoreCommitImplTest, OverwriteWithChangelogFilesLogsWarning) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));
    const BinaryRow partition = CreateIntRow(10);

    // Overwrite ignores changelog files but emits a warning listing them.
    DataIncrement data_increment(
        /*new_files=*/{CreateAppendDataFileMeta("overwrite-with-changelog", 1)},
        /*deleted_files=*/{},
        /*changelog_files=*/{CreateAppendDataFileMeta("ignored-append-changelog", 1)},
        /*new_index_files=*/{}, /*deleted_index_files=*/{});
    CompactIncrement compact_increment(
        /*compact_before=*/{}, /*compact_after=*/{},
        /*changelog_files=*/{CreateAppendDataFileMeta("ignored-compact-changelog", 1)});
    std::shared_ptr<CommitMessage> message = std::make_shared<CommitMessageImpl>(
        partition, /*bucket=*/0, /*total_buckets=*/2, data_increment, compact_increment);

    ASSERT_OK(commit_impl->Overwrite({}, {message}, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_TRUE(snapshot.has_value());
}

TEST_F(FileStoreCommitImplTest, OverwriteUpgradesNonOverlappingPrimaryKeyFiles) {
    auto pk_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(pk_dir);
    std::string pk_root = pk_dir->Str();
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(pk_root, {}));
    ASSERT_OK(catalog->CreateDatabase("db", {}, false));

    arrow::Schema pk_schema(
        {arrow::field("pk", arrow::int32()), arrow::field("val", arrow::utf8())});
    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(pk_schema, &arrow_schema).ok());
    std::map<std::string, std::string> table_options = {{Options::BUCKET, "4"}};
    ASSERT_OK(catalog->CreateTable(Identifier("db", "pk_tbl"), &arrow_schema,
                                   /*partition_keys=*/{}, /*primary_keys=*/{"pk"}, table_options,
                                   /*ignore_if_exists=*/false));
    std::string pk_table_path = PathUtil::JoinPath(pk_root, "db.db/pk_tbl");

    CommitContextBuilder builder(pk_table_path, "test_user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .AddOption(Options::OVERWRITE_UPGRADE, "true")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    const BinaryRow partition = BinaryRow::EmptyRow();
    // Bucket 0: non-overlapping key ranges -> files are upgraded to a higher level.
    DataIncrement bucket0_increment(
        {CreateLeveledDataFileMeta("pk-b0-lo", CreateIntRow(0), CreateIntRow(10), /*level=*/0),
         CreateLeveledDataFileMeta("pk-b0-hi", CreateIntRow(20), CreateIntRow(30), /*level=*/0)},
        {}, {});
    std::shared_ptr<CommitMessage> bucket0_msg =
        std::make_shared<CommitMessageImpl>(partition, /*bucket=*/0, /*total_buckets=*/4,
                                            bucket0_increment, CompactIncrement({}, {}, {}));
    // Bucket 1: overlapping key ranges -> files are kept at their original level.
    DataIncrement bucket1_increment(
        {CreateLeveledDataFileMeta("pk-b1-a", CreateIntRow(0), CreateIntRow(20), /*level=*/0),
         CreateLeveledDataFileMeta("pk-b1-b", CreateIntRow(10), CreateIntRow(30), /*level=*/0)},
        {}, {});
    std::shared_ptr<CommitMessage> bucket1_msg =
        std::make_shared<CommitMessageImpl>(partition, /*bucket=*/1, /*total_buckets=*/4,
                                            bucket1_increment, CompactIncrement({}, {}, {}));

    ASSERT_OK(commit_impl->Overwrite({}, {bucket0_msg, bucket1_msg}, /*commit_identifier=*/1));

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestEntry> entries,
                         commit_impl->GetAllFiles(snapshot.value(), {}));
    ASSERT_EQ(4u, entries.size());

    int32_t max_level = 0;
    int32_t level0_count = 0;
    for (const auto& entry : entries) {
        max_level = std::max(max_level, entry.Level());
        if (entry.Level() == 0) {
            level0_count++;
        }
    }
    // Bucket 0 files were upgraded above level 0, bucket 1 files stayed at level 0.
    ASSERT_GT(max_level, 0);
    ASSERT_EQ(2, level0_count);
}

TEST_F(FileStoreCommitImplTest, CommitWithAppendCommitCheckConflict) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .AppendCommitCheckConflict(true)
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));

    std::vector<std::shared_ptr<CommitMessage>> msgs =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);
    ASSERT_GT(msgs.size(), 0);
    ASSERT_OK(commit->Commit(msgs, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(
        bool exist, file_system_->Exists(PathUtil::JoinPath(table_path_, "snapshot/snapshot-1")));
    ASSERT_TRUE(exist);
}

TEST_F(FileStoreCommitImplTest, SnapshotSequenceMaxFallsBackToManifestScan) {
    // First snapshot is committed in the default (scan) mode, so it does NOT carry the
    // max-sequence-number property.
    {
        CommitContextBuilder context_builder(table_path_, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                             context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                                 .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                                 .AddOption(Options::FILE_SYSTEM, "local")
                                 .Finish());
        ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
        std::vector<std::shared_ptr<CommitMessage>> msgs1 =
            GetCommitMessages(paimon::test::GetDataDir() +
                                  "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                              /*version=*/3);
        ASSERT_OK(commit->Commit(msgs1, /*commit_identifier=*/1));
    }

    // Second commit uses snapshot-init mode; since the previous snapshot lacks the property,
    // the max sequence number is recomputed by scanning the base manifests.
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .AddOption(Options::WRITE_SEQUENCE_NUMBER_INIT_MODE, "snapshot")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    ASSERT_OK_AND_ASSIGN(Snapshot snapshot1, commit_impl->snapshot_manager_->LoadSnapshot(1));
    if (snapshot1.Properties()) {
        ASSERT_EQ(snapshot1.Properties().value().end(),
                  snapshot1.Properties().value().find("sequence.generation.max-sequence-number"));
    }

    std::vector<std::shared_ptr<CommitMessage>> msgs2 =
        GetCommitMessages(paimon::test::GetDataDir() +
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-02",
                          /*version=*/3);
    ASSERT_OK(commit_impl->Commit(msgs2, /*commit_identifier=*/2));

    ASSERT_OK_AND_ASSIGN(Snapshot snapshot2, commit_impl->snapshot_manager_->LoadSnapshot(2));
    ASSERT_TRUE(snapshot2.Properties());
    auto iter = snapshot2.Properties().value().find("sequence.generation.max-sequence-number");
    ASSERT_TRUE(iter != snapshot2.Properties().value().end());
}

TEST_F(FileStoreCommitImplTest, FilterAndOverwriteWithSpecifiedPartition) {
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
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);
    // A non-empty partition spec is forwarded to the overwrite so the partition list is populated.
    std::map<std::string, std::string> partition_spec = {{"f1", "10"}};
    ASSERT_OK_AND_ASSIGN(int32_t actual_commit, commit_impl->FilterAndOverwrite(
                                                    partition_spec, msgs, /*commit_identifier=*/1,
                                                    /*watermark=*/10));
    ASSERT_EQ(1, actual_commit);

    ASSERT_OK_AND_ASSIGN(auto snapshot, commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_TRUE(snapshot);
    ASSERT_EQ(Snapshot::CommitKind::Overwrite(), snapshot.value().GetCommitKind());
}

TEST_F(FileStoreCommitImplTest, TryUpgradeReturnsInputWhenOverwriteUpgradeDisabled) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .AddOption(Options::OVERWRITE_UPGRADE, "false")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    std::vector<ManifestEntry> entries = {
        CreateManifestEntry("upgrade-disabled-1", FileKind::Add())};
    // overwrite-upgrade disabled: TryUpgrade returns the input unchanged.
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestEntry> result, commit_impl->TryUpgrade(entries));
    ASSERT_EQ(entries.size(), result.size());
}

TEST_F(FileStoreCommitImplTest, TryUpgradeReturnsInputWhenEntryLevelAboveZero) {
    auto pk_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(pk_dir);
    std::string pk_root = pk_dir->Str();
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(pk_root, {}));
    ASSERT_OK(catalog->CreateDatabase("db", {}, false));

    arrow::Schema pk_schema(
        {arrow::field("pk", arrow::int32()), arrow::field("val", arrow::utf8())});
    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(pk_schema, &arrow_schema).ok());
    std::map<std::string, std::string> table_options = {{Options::BUCKET, "4"}};
    ASSERT_OK(catalog->CreateTable(Identifier("db", "pk_tbl"), &arrow_schema,
                                   /*partition_keys=*/{}, /*primary_keys=*/{"pk"}, table_options,
                                   /*ignore_if_exists=*/false));
    std::string pk_table_path = PathUtil::JoinPath(pk_root, "db.db/pk_tbl");

    CommitContextBuilder builder(pk_table_path, "test_user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .AddOption(Options::OVERWRITE_UPGRADE, "true")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    // A PK-table entry already at a level above 0 bypasses the upgrade and returns the input.
    std::vector<ManifestEntry> entries = {
        CreateManifestEntry("already-upgraded", BinaryRow::EmptyRow(), FileKind::Add(),
                            DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(), /*level=*/2,
                            /*bucket=*/0)};
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestEntry> result, commit_impl->TryUpgrade(entries));
    ASSERT_EQ(entries.size(), result.size());
    ASSERT_EQ(2, result[0].Level());
}

TEST_F(FileStoreCommitImplTest, CheckSameBucketFromSnapshotReturnsOkForEmptyDelta) {
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
                              "/orc/append_09.db/append_09/commit_messages/commit_messages-01",
                          /*version=*/3);
    ASSERT_OK(commit_impl->Commit(msgs, /*commit_identifier=*/1));

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         commit_impl->snapshot_manager_->LatestSnapshot());
    ASSERT_TRUE(snapshot);
    // No delta entries -> no buckets to verify -> returns OK without scanning the snapshot.
    ASSERT_OK(commit_impl->CheckSameBucketFromSnapshot(/*delta_entries=*/{}, snapshot));
}

TEST_F(FileStoreCommitImplTest, MaxSequenceNumberReturnsNulloptForEmptyManifests) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    // No manifests to scan -> no sequence number found -> returns nullopt.
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> max_seq, commit_impl->MaxSequenceNumber({}));
    ASSERT_FALSE(max_seq.has_value());
}

TEST_F(FileStoreCommitImplTest, RowIdCheckConflictSetsCheckSnapshotAndReturnsSelf) {
    CommitContextBuilder context_builder(table_path_, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = std::dynamic_pointer_cast<FileStoreCommitImpl>(
        std::shared_ptr<FileStoreCommit>(std::move(commit)));

    ASSERT_FALSE(commit_impl->conflict_detection_.HasRowIdCheckFromSnapshot());
    // RowIdCheckConflict records the snapshot to verify against and returns *this for chaining.
    FileStoreCommit& returned = commit_impl->RowIdCheckConflict(/*row_id_check_from_snapshot=*/5);
    ASSERT_EQ(commit_impl.get(), &returned);
    ASSERT_TRUE(commit_impl->conflict_detection_.HasRowIdCheckFromSnapshot());
}

}  // namespace paimon::test
