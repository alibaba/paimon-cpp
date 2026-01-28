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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/file_store_commit_impl.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/orphan_files_cleaner.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace arrow {
class Array;
}  // namespace arrow
namespace paimon {
class CommitMessage;
}  // namespace paimon

namespace paimon::test {
class CleanInteTest : public testing::Test {
 public:
    void SetUp() override {
        file_system_ = std::make_shared<LocalFileSystem>();
        pool_ = GetDefaultPool();
        int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
        std::srand(seed);
    }

    Result<std::string> CreateTestTable(const std::string& base_path, const std::string& db_name,
                                        const std::string& table_name, ::ArrowSchema* schema,
                                        const std::vector<std::string>& partition_keys,
                                        const std::vector<std::string>& primary_keys,
                                        const std::map<std::string, std::string>& options) const {
        PAIMON_ASSIGN_OR_RAISE(auto catalog, Catalog::Create(base_path, options));
        PAIMON_RETURN_NOT_OK(catalog->CreateDatabase(db_name, options, /*ignore_if_exists=*/false));
        Identifier table_id(db_name, table_name);
        PAIMON_RETURN_NOT_OK(catalog->CreateTable(table_id, schema, partition_keys, primary_keys,
                                                  options, /*ignore_if_exists=*/false));
        return PathUtil::JoinPath(base_path, db_name + ".db/" + table_name);
    }

    using ValueType = std::tuple<std::string, int32_t, int32_t, double>;
    Result<std::unique_ptr<RecordBatch>> MakeRecordBatch(
        const std::vector<ValueType>& raw_data,
        const std::map<std::string, std::string>& partition_map, int32_t bucket) const {
        ::ArrowArray arrow_array;
        std::shared_ptr<arrow::Array> array = GenerateArrowArray(raw_data);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &arrow_array));
        RecordBatchBuilder batch_builder(&arrow_array);
        return batch_builder.SetPartition(partition_map).SetBucket(bucket).Finish();
    }

    Result<std::unique_ptr<RecordBatch>> MakeRecordBatch(
        const std::shared_ptr<arrow::DataType>& data_type, const std::string& data_str,
        const std::map<std::string, std::string>& partition_map, int32_t bucket,
        const std::vector<RecordBatch::RowKind>& row_kinds) const {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto array, arrow::ipc::internal::json::ArrayFromJSON(data_type, data_str));
        ::ArrowArray arrow_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &arrow_array));
        RecordBatchBuilder batch_builder(&arrow_array);
        return batch_builder.SetPartition(partition_map)
            .SetBucket(bucket)
            .SetRowKinds(row_kinds)
            .Finish();
    }

    std::shared_ptr<arrow::Array> GenerateArrowArray(const std::vector<ValueType>& raw_data,
                                                     bool exist_null_value = false) const {
        auto string_field = arrow::field("f0", arrow::utf8());
        auto int_field = arrow::field("f1", arrow::int32());
        auto int_field1 = arrow::field("f2", arrow::int32());
        auto double_field = arrow::field("f3", arrow::float64());
        auto struct_type = arrow::struct_({string_field, int_field, int_field1, double_field});
        auto schema =
            arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));

        arrow::StructBuilder struct_builder(
            struct_type, arrow::default_memory_pool(),
            {std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int32Builder>(),
             std::make_shared<arrow::Int32Builder>(), std::make_shared<arrow::DoubleBuilder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        auto int_builder = static_cast<arrow::Int32Builder*>(struct_builder.field_builder(1));
        auto int_builder1 = static_cast<arrow::Int32Builder*>(struct_builder.field_builder(2));
        auto double_builder = static_cast<arrow::DoubleBuilder*>(struct_builder.field_builder(3));

        for (const auto& d : raw_data) {
            EXPECT_TRUE(struct_builder.Append().ok());
            EXPECT_TRUE(string_builder->Append(std::get<0>(d)).ok());
            EXPECT_TRUE(int_builder->Append(std::get<1>(d)).ok());
            EXPECT_TRUE(int_builder1->Append(std::get<2>(d)).ok());
            if (exist_null_value) {
                EXPECT_TRUE(double_builder->AppendNull().ok());
            } else {
                EXPECT_TRUE(double_builder->Append(std::get<3>(d)).ok());
            }
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    }

    bool HitIOHook(const Status& status, size_t io_count) const {
        if (status.ToString().find(fmt::format("io hook triggered io error at position {}",
                                               io_count)) != std::string::npos) {
            return true;
        }
        return false;
    }

    bool HitIOHookInCommitHint(const Status& status) const {
        if (status.ToString().find("io hook triggered io error at position") != std::string::npos &&
            (status.ToString().find("snapshot/LATEST") != std::string::npos ||
             status.ToString().find("snapshot/EARLIEST") != std::string::npos)) {
            return true;
        }
        return false;
    }

    bool CheckEqual(const std::set<std::string>& cleaned_paths,
                    const std::set<std::string>& expected_names) {
        std::set<std::string> file_names;
        for (const auto& file_path : cleaned_paths) {
            file_names.insert(PathUtil::GetName(file_path));
        }
        return file_names == expected_names;
    }

 private:
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(CleanInteTest, TestExpireSnapshotFailover) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    std::map<std::string, std::string> clean_options = {
        {Options::MANIFEST_TARGET_FILE_SIZE, "8mb"},
        {Options::FILE_SYSTEM, "local"},
        {Options::SNAPSHOT_NUM_RETAINED_MAX, "2"},
        {Options::SNAPSHOT_NUM_RETAINED_MIN, "1"},
        {Options::SNAPSHOT_TIME_RETAINED, "1ms"},
        {Options::MANIFEST_MERGE_MIN_COUNT, "2"},
        {Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES, "true"}};
    // no failover
    {
        auto dir = UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
        CommitContextBuilder commit_context_builder(table_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<CommitContext> commit_context,
            commit_context_builder.SetOptions(clean_options).IgnoreEmptyCommit(false).Finish());
        ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
        ASSERT_OK_AND_ASSIGN(int32_t expired_items, commit->Expire());
        ASSERT_EQ(expired_items, 4);
    }
    // failover with non-exist manifest
    {
        auto dir = UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
        ASSERT_OK(file_system_->Delete(PathUtil::JoinPath(
            table_path, "manifest/manifest-list-616d1847-a02c-495f-9cca-2c8b7def0fec-1")));
        CommitContextBuilder commit_context_builder(table_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<CommitContext> commit_context,
            commit_context_builder.SetOptions(clean_options).IgnoreEmptyCommit(false).Finish());
        ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
        ASSERT_OK_AND_ASSIGN(int32_t expired_items, commit->Expire());
        ASSERT_EQ(expired_items, 4);
    }
    // failover with non-exist data file
    {
        auto dir = UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
        ASSERT_OK(file_system_->Delete(PathUtil::JoinPath(
            table_path, "f1=10/bucket-1/data-10b9eea8-241d-4e4b-8ab8-2a82d72d79a2-0.orc")));
        CommitContextBuilder commit_context_builder(table_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<CommitContext> commit_context,
            commit_context_builder.SetOptions(clean_options).IgnoreEmptyCommit(false).Finish());
        ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
        ASSERT_OK_AND_ASSIGN(int32_t expired_items, commit->Expire());
        ASSERT_EQ(expired_items, 4);
    }
    // failover with discontinuous snapshot
    {
        auto dir = UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
        ASSERT_OK(file_system_->Delete(PathUtil::JoinPath(table_path, "snapshot/snapshot-3")));
        CommitContextBuilder commit_context_builder(table_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<CommitContext> commit_context,
            commit_context_builder.SetOptions(clean_options).IgnoreEmptyCommit(false).Finish());
        ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
        ASSERT_OK_AND_ASSIGN(int32_t expired_items, commit->Expire());
        ASSERT_EQ(expired_items, 1);
    }
}

TEST_F(CleanInteTest, TestDropPartitionAndExpireSnapshot) {
    auto string_field = arrow::field("f0", arrow::utf8());
    auto int_field = arrow::field("f1", arrow::int32());
    auto int_field1 = arrow::field("f2", arrow::int32());
    auto double_field = arrow::field("f3", arrow::float64());
    auto schema =
        arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));

    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &arrow_schema).ok());
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, "orc"},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, "local"},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
        {Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES, "true"},
    };

    ASSERT_OK_AND_ASSIGN(
        std::string table_path,
        CreateTestTable(dir->Str(), /*db_name=*/"foo", /*table_name=*/"bar", &arrow_schema,
                        /*partition_keys=*/{"f1"},
                        /*primary_keys=*/{}, options));
    WriteContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.SetOptions(options).WithStreamingMode(true).Finish());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_0,
                         MakeRecordBatch({{"Alice", 10, 1, 11.1}}, {{"f1", "10"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_0)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_1,
        MakeRecordBatch({{"Bob", 10, 0, 12.1}, {"Emily", 10, 0, 13.1}, {"Tony", 10, 0, 14.1}},
                        {{"f1", "10"}}, 1));
    ASSERT_OK(file_store_write->Write(std::move(batch_1)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         MakeRecordBatch({{"Lucy", 20, 1, 14.1}}, {{"f1", "20"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_2)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 0));
    ASSERT_EQ(3u, results.size());
    CommitContextBuilder commit_context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         commit_context_builder.AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .WithFileSystem(file_system_)
                             .AddOption(Options::SNAPSHOT_NUM_RETAINED_MAX, "2")
                             .AddOption(Options::SNAPSHOT_NUM_RETAINED_MIN, "1")
                             .AddOption(Options::SNAPSHOT_TIME_RETAINED, "1ms")
                             .AddOption(Options::MANIFEST_MERGE_MIN_COUNT, "2")
                             .AddOption(Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES, "true")
                             .IgnoreEmptyCommit(false)
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(results, 0, 10));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_3,
        MakeRecordBatch({{"Emily", 10, 0, 15.1}, {"Bob", 10, 0, 12.1}, {"Alex", 10, 0, 16.1}},
                        {{"f1", "10"}}, 1));
    ASSERT_OK(file_store_write->Write(std::move(batch_3)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_4,
                         MakeRecordBatch({{"Paul", 20, 1, 0}}, {{"f1", "20"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_4)));

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results2,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 1));
    ASSERT_EQ(3u, results2.size());
    ASSERT_OK(commit->Commit(results2, 1, 30));
    ASSERT_OK(commit->DropPartition({{{"f1", "10"}}}, 2));
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);
    ASSERT_OK_AND_ASSIGN(bool snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(1));
    ASSERT_TRUE(snapshot_exist);
    ASSERT_OK_AND_ASSIGN(snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(2));
    ASSERT_TRUE(snapshot_exist);
    ASSERT_OK_AND_ASSIGN(snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(3));
    ASSERT_TRUE(snapshot_exist);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot_3, commit_impl->snapshot_manager_->LoadSnapshot(3));
    ASSERT_EQ(30, snapshot_3.Watermark().value());
    ASSERT_EQ(-7, snapshot_3.DeltaRecordCount().value());
    ASSERT_EQ(2, snapshot_3.TotalRecordCount().value());
    ASSERT_EQ(Snapshot::CommitKind::Overwrite(), snapshot_3.GetCommitKind());
    ASSERT_EQ(2, snapshot_3.CommitIdentifier());
    ASSERT_OK_AND_ASSIGN(bool f1_10_bucket_0_exist,
                         file_system_->Exists(PathUtil::JoinPath(table_path, "f1=10/bucket-0")));
    ASSERT_TRUE(f1_10_bucket_0_exist);
    ASSERT_OK_AND_ASSIGN(bool f1_10_bucket_1_exist,
                         file_system_->Exists(PathUtil::JoinPath(table_path, "f1=10/bucket-1")));
    ASSERT_TRUE(f1_10_bucket_1_exist);
    ASSERT_OK_AND_ASSIGN(bool f1_20_bucket_0_exist,
                         file_system_->Exists(PathUtil::JoinPath(table_path, "f1=20/bucket-0")));
    ASSERT_TRUE(f1_20_bucket_0_exist);
    ASSERT_OK(commit->Expire());
    ASSERT_OK_AND_ASSIGN(f1_10_bucket_0_exist,
                         file_system_->Exists(PathUtil::JoinPath(table_path, "f1=10/bucket-0")));
    ASSERT_FALSE(f1_10_bucket_0_exist);
    ASSERT_OK_AND_ASSIGN(f1_10_bucket_1_exist,
                         file_system_->Exists(PathUtil::JoinPath(table_path, "f1=10/bucket-1")));
    ASSERT_FALSE(f1_10_bucket_1_exist);
    ASSERT_OK_AND_ASSIGN(f1_20_bucket_0_exist,
                         file_system_->Exists(PathUtil::JoinPath(table_path, "f1=20/bucket-0")));
    ASSERT_TRUE(f1_20_bucket_0_exist);
    ASSERT_OK_AND_ASSIGN(snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(1));
    ASSERT_FALSE(snapshot_exist);
    ASSERT_OK_AND_ASSIGN(snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(2));
    ASSERT_FALSE(snapshot_exist);
    ASSERT_OK_AND_ASSIGN(snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(3));
    ASSERT_TRUE(snapshot_exist);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_5,
                         MakeRecordBatch({{"Lucas", 20, 2, 22.2}}, {{"f1", "20"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_5)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results3,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 3));
    ASSERT_EQ(3u, results3.size());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_6,
                         MakeRecordBatch({{"Ken", 20, 3, 33.3}}, {{"f1", "20"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_6)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results4,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 4));
    ASSERT_EQ(1u, results4.size());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_7,
                         MakeRecordBatch({{"Dennis", 10, 4, 44.4}}, {{"f1", "10"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_7)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results5,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 5));
    ASSERT_EQ(2u, results5.size());

    results3.insert(results3.end(), results4.begin(), results4.end());
    results3.insert(results3.end(), results5.begin(), results5.end());
    ASSERT_OK(commit->Commit(results3, 6, 40));
    ASSERT_OK(commit->Expire());
    ASSERT_OK_AND_ASSIGN(snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(4));
    ASSERT_TRUE(snapshot_exist);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot_4, commit_impl->snapshot_manager_->LoadSnapshot(4));
    ASSERT_EQ(40, snapshot_4.Watermark().value());
    std::vector<ManifestFileMeta> manifests;
    ASSERT_OK(commit_impl->manifest_list_->ReadDataManifests(snapshot_4, &manifests));
    ASSERT_EQ(2u, manifests.size());
    ASSERT_EQ(2u, manifests[0].NumAddedFiles());
    ASSERT_EQ(3u, manifests[1].NumAddedFiles());
}

TEST_F(CleanInteTest, TestDropPartitionAndExpireSnapshotWithIOException) {
    auto string_field = arrow::field("f0", arrow::utf8());
    auto int_field = arrow::field("f1", arrow::int32());
    auto int_field1 = arrow::field("f2", arrow::int32());
    auto double_field = arrow::field("f3", arrow::float64());
    auto schema =
        arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, "orc"},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, "local"},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
        {Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES, "true"},
    };

    bool drop_partition_scanned_all_io_hook = false;
    bool expire_snapshot_scanned_all_io_hook = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 1000; i += paimon::test::RandomNumber(5, 15)) {
        ::ArrowSchema arrow_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &arrow_schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(std::string table_path,
                             CreateTestTable(dir->Str(), /*db_name=*/"foo",
                                             /*table_name=*/"bar", &arrow_schema,
                                             /*partition_keys=*/{"f1"},
                                             /*primary_keys=*/{}, options));
        WriteContextBuilder context_builder(table_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                             context_builder.SetOptions(options).WithStreamingMode(true).Finish());

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_0,
                             MakeRecordBatch({{"Alice", 10, 1, 11.1}}, {{"f1", "10"}}, 0));
        ASSERT_OK(file_store_write->Write(std::move(batch_0)));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch_1,
            MakeRecordBatch({{"Bob", 10, 0, 12.1}, {"Emily", 10, 0, 13.1}, {"Tony", 10, 0, 14.1}},
                            {{"f1", "10"}}, 1));
        ASSERT_OK(file_store_write->Write(std::move(batch_1)));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                             MakeRecordBatch({{"Lucy", 20, 1, 14.1}}, {{"f1", "20"}}, 0));
        ASSERT_OK(file_store_write->Write(std::move(batch_2)));
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results,
                             file_store_write->PrepareCommit(/*wait_compaction=*/false, 0));
        ASSERT_EQ(3u, results.size());
        CommitContextBuilder commit_context_builder(table_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<CommitContext> commit_context,
            commit_context_builder.AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                .WithFileSystem(file_system_)
                .AddOption(Options::SNAPSHOT_NUM_RETAINED_MAX, "2")
                .AddOption(Options::SNAPSHOT_NUM_RETAINED_MIN, "1")
                .AddOption(Options::SNAPSHOT_TIME_RETAINED, "1ms")
                .AddOption(Options::MANIFEST_MERGE_MIN_COUNT, "2")
                .AddOption(Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES, "true")
                .IgnoreEmptyCommit(false)
                .Finish());
        ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
        ASSERT_OK(commit->Commit(results, 0));

        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch_3,
            MakeRecordBatch({{"Emily", 10, 0, 15.1}, {"Bob", 10, 0, 12.1}, {"Alex", 10, 0, 16.1}},
                            {{"f1", "10"}}, 1));
        ASSERT_OK(file_store_write->Write(std::move(batch_3)));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_4,
                             MakeRecordBatch({{"Paul", 20, 1, 0}}, {{"f1", "20"}}, 0));
        ASSERT_OK(file_store_write->Write(std::move(batch_4)));

        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results2,
                             file_store_write->PrepareCommit(/*wait_compaction=*/false, 1));
        ASSERT_EQ(3u, results2.size());
        ASSERT_OK(commit->Commit(results2, 1));
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        auto status = commit->DropPartition({{{"f1", "10"}}}, 2);
        io_hook->Clear();
        if (HitIOHook(status, i)) {
            if (HitIOHookInCommitHint(status)) {
                continue;
            }
            ASSERT_OK(commit->DropPartition({{{"f1", "10"}}}, 2));
        } else {
            ASSERT_OK(status);
            drop_partition_scanned_all_io_hook = true;
        }
        auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
        ASSERT_TRUE(commit_impl);
        ASSERT_OK_AND_ASSIGN(bool snapshot_exist,
                             commit_impl->snapshot_manager_->SnapshotExists(1));
        ASSERT_TRUE(snapshot_exist);
        ASSERT_OK_AND_ASSIGN(snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(2));
        ASSERT_TRUE(snapshot_exist);
        ASSERT_OK_AND_ASSIGN(snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(3));
        ASSERT_TRUE(snapshot_exist);
        ASSERT_OK_AND_ASSIGN(Snapshot snapshot_3, commit_impl->snapshot_manager_->LoadSnapshot(3));
        ASSERT_EQ(-7, snapshot_3.DeltaRecordCount().value());
        ASSERT_EQ(2, snapshot_3.TotalRecordCount().value());
        ASSERT_EQ(Snapshot::CommitKind::Overwrite(), snapshot_3.GetCommitKind());
        ASSERT_EQ(2, snapshot_3.CommitIdentifier());
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        Result<int32_t> expire_result = commit->Expire();
        io_hook->Clear();
        if (HitIOHook(expire_result.status(), i)) {
            if (HitIOHookInCommitHint(expire_result.status())) {
                continue;
            }
            ASSERT_OK(commit->Expire());
        } else {
            ASSERT_OK(expire_result.status());
            expire_snapshot_scanned_all_io_hook = true;
        }
        ASSERT_OK_AND_ASSIGN(snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(1));
        ASSERT_FALSE(snapshot_exist);
        ASSERT_OK_AND_ASSIGN(snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(2));
        ASSERT_FALSE(snapshot_exist);
        ASSERT_OK_AND_ASSIGN(snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(3));
        ASSERT_TRUE(snapshot_exist);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_5,
                             MakeRecordBatch({{"Lucas", 20, 2, 22.2}}, {{"f1", "20"}}, 0));
        ASSERT_OK(file_store_write->Write(std::move(batch_5)));
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results3,
                             file_store_write->PrepareCommit(/*wait_compaction=*/false, 3));
        ASSERT_EQ(3u, results3.size());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_6,
                             MakeRecordBatch({{"Ken", 20, 3, 33.3}}, {{"f1", "20"}}, 0));
        ASSERT_OK(file_store_write->Write(std::move(batch_6)));
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results4,
                             file_store_write->PrepareCommit(/*wait_compaction=*/false, 4));
        ASSERT_EQ(1u, results4.size());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_7,
                             MakeRecordBatch({{"Dennis", 10, 4, 44.4}}, {{"f1", "10"}}, 0));
        ASSERT_OK(file_store_write->Write(std::move(batch_7)));
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results5,
                             file_store_write->PrepareCommit(/*wait_compaction=*/false, 5));
        ASSERT_EQ(2u, results5.size());

        results3.insert(results3.end(), results4.begin(), results4.end());
        results3.insert(results3.end(), results5.begin(), results5.end());
        ASSERT_OK(commit->Commit(results3, 6));
        ASSERT_OK(commit->Expire());
        ASSERT_OK_AND_ASSIGN(snapshot_exist, commit_impl->snapshot_manager_->SnapshotExists(4));
        ASSERT_TRUE(snapshot_exist);
        ASSERT_OK_AND_ASSIGN(Snapshot snapshot_4, commit_impl->snapshot_manager_->LoadSnapshot(4));
        std::vector<ManifestFileMeta> manifests;
        ASSERT_OK(commit_impl->manifest_list_->ReadDataManifests(snapshot_4, &manifests));
        ASSERT_EQ(2u, manifests.size());
        ASSERT_EQ(2u, manifests[0].NumAddedFiles());
        ASSERT_EQ(3u, manifests[1].NumAddedFiles());
        if (drop_partition_scanned_all_io_hook && expire_snapshot_scanned_all_io_hook) {
            break;
        }
    }
    ASSERT_TRUE(drop_partition_scanned_all_io_hook);
    ASSERT_TRUE(expire_snapshot_scanned_all_io_hook);
}

TEST_F(CleanInteTest, TestOrphanFilesClean) {
    auto string_field = arrow::field("f0", arrow::utf8());
    auto int_field = arrow::field("f1", arrow::int32());
    auto int_field1 = arrow::field("f2", arrow::int32());
    auto double_field = arrow::field("f3", arrow::float64());
    auto schema =
        arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));

    std::string commit_user = "commit_user_1";
    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &arrow_schema).ok());

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, "orc"},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, "local"},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f3"},
        {Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES, "true"},
    };

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::string table_path,
                         CreateTestTable(dir->Str(), /*db_name=*/"foo",
                                         /*table_name=*/"bar", &arrow_schema,
                                         /*partition_keys=*/{"f1", "f2"},
                                         /*primary_keys=*/{}, options));

    WriteContextBuilder context_builder(table_path, commit_user);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.SetOptions(options).WithStreamingMode(true).Finish());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_0,
                         MakeRecordBatch({{"Alice", 10, 1, 11.1}}, {{"f1", "10"}, {"f2", "1"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_0)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_1,
        MakeRecordBatch({{"Bob", 10, 0, 12.1}, {"Emily", 10, 0, 13.1}, {"Tony", 10, 0, 14.1}},
                        {{"f1", "10"}, {"f2", "0"}}, 1));
    ASSERT_OK(file_store_write->Write(std::move(batch_1)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         MakeRecordBatch({{"Lucy", 20, 1, 14.1}}, {{"f1", "20"}, {"f2", "1"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_2)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 0));
    ASSERT_EQ(3u, results.size());
    CommitContextBuilder commit_context_builder(table_path, commit_user);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         commit_context_builder.AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .WithFileSystem(file_system_)
                             .IgnoreEmptyCommit(false)
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(results, 0));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_3,
        MakeRecordBatch({{"Emily", 10, 0, 15.1}, {"Bob", 10, 0, 12.1}, {"Alex", 10, 0, 16.1}},
                        {{"f1", "10"}, {"f2", "0"}}, 1));
    ASSERT_OK(file_store_write->Write(std::move(batch_3)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_4,
                         MakeRecordBatch({{"Paul", 20, 1, 0}}, {{"f1", "20"}, {"f2", "1"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_4)));

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results2,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 1));
    ASSERT_EQ(3u, results2.size());
    ASSERT_OK(commit->Commit(results2, 1));

    ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(table_path, "f1=10/f2=0/data-orphan1.orc"),
                                      "orphan", true));
    ASSERT_OK(file_system_->Mkdirs(PathUtil::JoinPath(table_path, "f1=20/f2=0/bucket-999")));
    ASSERT_OK(file_system_->WriteFile(
        PathUtil::JoinPath(table_path, "f1=10/f2=1/bucket-0/data-orphan2.orc"), "orphan", true));
    ASSERT_OK(file_system_->WriteFile(
        PathUtil::JoinPath(table_path, "f1=10/f2=1/bucket-0/data-orphan2.orc.index"), "orphan",
        true));
    ASSERT_OK(file_system_->WriteFile(
        PathUtil::JoinPath(table_path, "f1=10/f2=0/bucket-0/orphan3.test"), "orphan", true));
    ASSERT_OK(file_system_->WriteFile(
        PathUtil::JoinPath(table_path, "f1=10/f2=1/bucket-0/data-orphan4"), "orphan", true));
    ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(table_path, "manifest/orphan5.test"),
                                      "orphan", true));
    ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(table_path, "manifest/manifest-orphan"),
                                      "orphan", true));
    ASSERT_OK(file_system_->WriteFile(
        PathUtil::JoinPath(table_path, "manifest/.manifest-orphan.uuid.tmp"), "orphan", true));
    ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(table_path, "snapshot/orphan6.test"),
                                      "orphan", true));
    ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(table_path, "data-orphan7.parquet"),
                                      "orphan", true));

    {
        CleanContextBuilder clean_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                             clean_context_builder.WithFileSystem(file_system_).Finish());
        ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
        ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
        ASSERT_TRUE(cleaned_paths.empty());
    }
    {
        CleanContextBuilder clean_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                             clean_context_builder.WithFileSystem(file_system_)
                                 .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                                 .Finish());
        ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
        ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
        ASSERT_TRUE(CheckEqual(
            cleaned_paths, {"data-orphan2.orc", "manifest-orphan", ".manifest-orphan.uuid.tmp"}));
    }
}

TEST_F(CleanInteTest, TestOrphanFilesCleanWithFileRetainCondition) {
    auto string_field = arrow::field("f0", arrow::utf8());
    auto int_field = arrow::field("f1", arrow::int32());
    auto int_field1 = arrow::field("f2", arrow::int32());
    auto double_field = arrow::field("f3", arrow::float64());
    auto schema =
        arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));

    std::string commit_user = "commit_user_1";
    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &arrow_schema).ok());

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, "orc"},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, "local"},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f3"},
        {Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES, "true"},
    };

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::string table_path,
                         CreateTestTable(dir->Str(), /*db_name=*/"foo",
                                         /*table_name=*/"bar", &arrow_schema,
                                         /*partition_keys=*/{"f1", "f2"},
                                         /*primary_keys=*/{}, options));

    WriteContextBuilder context_builder(table_path, commit_user);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.SetOptions(options).WithStreamingMode(true).Finish());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_0,
                         MakeRecordBatch({{"Alice", 10, 1, 11.1}}, {{"f1", "10"}, {"f2", "1"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_0)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_1,
        MakeRecordBatch({{"Bob", 10, 0, 12.1}, {"Emily", 10, 0, 13.1}, {"Tony", 10, 0, 14.1}},
                        {{"f1", "10"}, {"f2", "0"}}, 1));
    ASSERT_OK(file_store_write->Write(std::move(batch_1)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         MakeRecordBatch({{"Lucy", 20, 1, 14.1}}, {{"f1", "20"}, {"f2", "1"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_2)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 0));
    ASSERT_EQ(3u, results.size());
    CommitContextBuilder commit_context_builder(table_path, commit_user);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         commit_context_builder.AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .WithFileSystem(file_system_)
                             .IgnoreEmptyCommit(false)
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(results, 0));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_3,
        MakeRecordBatch({{"Emily", 10, 0, 15.1}, {"Bob", 10, 0, 12.1}, {"Alex", 10, 0, 16.1}},
                        {{"f1", "10"}, {"f2", "0"}}, 1));
    ASSERT_OK(file_store_write->Write(std::move(batch_3)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_4,
                         MakeRecordBatch({{"Paul", 20, 1, 0}}, {{"f1", "20"}, {"f2", "1"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_4)));

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results2,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 1));
    ASSERT_EQ(3u, results2.size());
    ASSERT_OK(commit->Commit(results2, 1));

    ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(table_path, "f1=10/f2=0/data-orphan1.orc"),
                                      "orphan", true));
    ASSERT_OK(file_system_->Mkdirs(PathUtil::JoinPath(table_path, "f1=20/f2=0/bucket-999")));
    ASSERT_OK(file_system_->WriteFile(
        PathUtil::JoinPath(table_path, "f1=10/f2=1/bucket-0/data-orphan2.orc"), "orphan", true));
    ASSERT_OK(file_system_->WriteFile(
        PathUtil::JoinPath(table_path, "f1=10/f2=1/bucket-0/data-orphan2.orc.index"), "orphan",
        true));
    ASSERT_OK(file_system_->WriteFile(
        PathUtil::JoinPath(table_path, "f1=10/f2=0/bucket-0/orphan3.test"), "orphan", true));
    ASSERT_OK(file_system_->WriteFile(
        PathUtil::JoinPath(table_path, "f1=10/f2=1/bucket-0/data-orphan4"), "orphan", true));
    ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(table_path, "manifest/orphan5.test"),
                                      "orphan", true));
    ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(table_path, "manifest/manifest-orphan"),
                                      "orphan", true));
    ASSERT_OK(file_system_->WriteFile(
        PathUtil::JoinPath(table_path, "manifest/.manifest-orphan.uuid.tmp"), "orphan", true));
    ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(table_path, "snapshot/orphan6.test"),
                                      "orphan", true));
    ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(table_path, "data-orphan7.parquet"),
                                      "orphan", true));

    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.WithFileSystem(file_system_)
                             .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                             .WithFileRetainCondition([](const std::string& file_name) -> bool {
                                 if (file_name == "data-orphan2.orc") {
                                     return true;
                                 }
                                 return false;
                             })
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
    ASSERT_TRUE(CheckEqual(cleaned_paths, {"manifest-orphan", ".manifest-orphan.uuid.tmp"}));
}

TEST_F(CleanInteTest, TestOrphanFilesCleanWithIOException) {
    auto string_field = arrow::field("f0", arrow::utf8());
    auto int_field = arrow::field("f1", arrow::int32());
    auto int_field1 = arrow::field("f2", arrow::int32());
    auto double_field = arrow::field("f3", arrow::float64());
    auto schema =
        arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));

    std::string commit_user = "commit_user_1";
    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &arrow_schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, "orc"},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, "local"},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
        {Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES, "true"}};

    ASSERT_OK_AND_ASSIGN(std::string table_path,
                         CreateTestTable(dir->Str(), /*db_name=*/"foo",
                                         /*table_name=*/"bar", &arrow_schema,
                                         /*partition_keys=*/{"f1"},
                                         /*primary_keys=*/{}, options));

    WriteContextBuilder context_builder(table_path, commit_user);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.SetOptions(options).WithStreamingMode(true).Finish());
    std::string root_path = write_context->GetRootPath();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_0,
                         MakeRecordBatch({{"Alice", 10, 1, 11.1}}, {{"f1", "10"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_0)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_1,
        MakeRecordBatch({{"Bob", 10, 0, 12.1}, {"Emily", 10, 0, 13.1}, {"Tony", 10, 0, 14.1}},
                        {{"f1", "10"}}, 1));
    ASSERT_OK(file_store_write->Write(std::move(batch_1)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         MakeRecordBatch({{"Lucy", 20, 1, 14.1}}, {{"f1", "20"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_2)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 0));
    ASSERT_EQ(3u, results.size());
    CommitContextBuilder commit_context_builder(root_path, commit_user);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         commit_context_builder.AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .WithFileSystem(file_system_)
                             .AddOption(Options::BUCKET, "2")
                             .IgnoreEmptyCommit(false)
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(results, 0));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_3,
        MakeRecordBatch({{"Emily", 10, 0, 15.1}, {"Bob", 10, 0, 12.1}, {"Alex", 10, 0, 16.1}},
                        {{"f1", "10"}}, 1));
    ASSERT_OK(file_store_write->Write(std::move(batch_3)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_4,
                         MakeRecordBatch({{"Paul", 20, 1, 0}}, {{"f1", "20"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_4)));

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results2,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 1));
    ASSERT_EQ(3u, results2.size());
    ASSERT_OK(commit->Commit(results2, 1));

    auto write_orphan_files = [this](const std::string& root_path) {
        ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(root_path, "f1=10/data-orphan1.orc"),
                                          "orphan", true));
        ASSERT_OK(file_system_->Mkdirs(PathUtil::JoinPath(root_path, "f1=20/bucket-999")));
        ASSERT_OK(file_system_->WriteFile(
            PathUtil::JoinPath(root_path, "f1=10/bucket-0/data-orphan2.orc"), "orphan", true));
        ASSERT_OK(file_system_->WriteFile(
            PathUtil::JoinPath(root_path, "f1=10/bucket-0/data-orphan2.orc.index"), "orphan",
            true));
        ASSERT_OK(file_system_->WriteFile(
            PathUtil::JoinPath(root_path, "f1=10/bucket-0/orphan3.test"), "orphan", true));
        ASSERT_OK(file_system_->WriteFile(
            PathUtil::JoinPath(root_path, "f1=10/bucket-0/data-orphan4"), "orphan", true));
        ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(root_path, "manifest/orphan5.test"),
                                          "orphan", true));
        ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(root_path, "manifest/manifest-orphan"),
                                          "orphan", true));
        ASSERT_OK(file_system_->WriteFile(
            PathUtil::JoinPath(root_path, "manifest/.manifest-orphan.uuid.tmp"), "orphan", true));
        ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(root_path, "snapshot/orphan6.test"),
                                          "orphan", true));
        ASSERT_OK(file_system_->WriteFile(PathUtil::JoinPath(root_path, "data-orphan7.parquet"),
                                          "orphan", true));
    };

    bool scanned_all_io_hook = true;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 500; i += paimon::test::RandomNumber(5, 15)) {
        scanned_all_io_hook = true;
        write_orphan_files(root_path);
        {
            CleanContextBuilder clean_context_builder(root_path);
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                                 clean_context_builder.WithFileSystem(file_system_).Finish());
            ASSERT_OK_AND_ASSIGN(auto cleaner,
                                 OrphanFilesCleaner::Create(std::move(clean_context)));
            io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
            auto clean_result = cleaner->Clean();
            io_hook->Clear();
            if (HitIOHook(clean_result.status(), i)) {
                scanned_all_io_hook = false;
                clean_result = cleaner->Clean();
            }
            ASSERT_OK(clean_result);
            ASSERT_TRUE(clean_result.value().empty());
        }
        {
            CleanContextBuilder clean_context_builder(root_path);
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                                 clean_context_builder.WithFileSystem(file_system_)
                                     .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                                     .Finish());

            ASSERT_OK_AND_ASSIGN(auto cleaner,
                                 OrphanFilesCleaner::Create(std::move(clean_context)));

            io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
            auto clean_result = cleaner->Clean();
            io_hook->Clear();
            if (HitIOHook(clean_result.status(), i)) {
                scanned_all_io_hook = false;
                clean_result = cleaner->Clean();
            }
            ASSERT_OK(clean_result);
            ASSERT_TRUE(CheckEqual(clean_result.value(), {"data-orphan2.orc", "manifest-orphan",
                                                          ".manifest-orphan.uuid.tmp"}));
            // because clean is quietly delete, if touch IO exception in Delete(), Clean() will
            // return OK, but files may not be deleted, so need to call Clean() again.
            // If the next call Clean() return empty, it means all files are deleted.
            clean_result = cleaner->Clean();
            ASSERT_OK(clean_result);
            if (!clean_result.value().empty()) {
                scanned_all_io_hook = false;
            }
        }
        if (scanned_all_io_hook) {
            break;
        }
    }
    ASSERT_TRUE(scanned_all_io_hook);
}

}  // namespace paimon::test
