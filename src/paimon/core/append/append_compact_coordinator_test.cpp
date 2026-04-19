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

#include "paimon/append/append_compact_coordinator.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/commit_context.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

class AppendCompactCoordinatorTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create("local");
    }

    void TearDown() override {
        dir_.reset();
    }

    void CreateTable(const arrow::FieldVector& fields,
                     const std::vector<std::string>& partition_keys,
                     const std::map<std::string, std::string>& options) {
        fields_ = fields;
        auto schema = arrow::schema(fields_);
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir_->Str(), options));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema, partition_keys,
                                       /*primary_keys=*/{}, options,
                                       /*ignore_if_exists=*/false));
    }

    std::string TablePath() const {
        return PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    }

    Status WriteAndCommit(const std::string& table_path,
                          const std::map<std::string, std::string>& partition, int32_t bucket,
                          const std::shared_ptr<arrow::Array>& write_array,
                          int64_t commit_identifier) {
        WriteContextBuilder write_builder(table_path, "commit_user_1");
        write_builder.WithStreamingMode(false);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto file_store_write,
                               FileStoreWrite::Create(std::move(write_context)));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*write_array, &c_array));
        auto record_batch = std::make_unique<RecordBatch>(
            partition, bucket, std::vector<RecordBatch::RowKind>{}, &c_array);
        PAIMON_RETURN_NOT_OK(file_store_write->Write(std::move(record_batch)));
        PAIMON_ASSIGN_OR_RAISE(auto commit_msgs, file_store_write->PrepareCommit(
                                                     /*wait_compaction=*/false, commit_identifier));
        PAIMON_RETURN_NOT_OK(file_store_write->Close());
        return Commit(table_path, commit_msgs);
    }

    Status Commit(const std::string& table_path,
                  const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        CommitContextBuilder commit_builder(table_path, "commit_user_1");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context,
                               commit_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> file_store_commit,
                               FileStoreCommit::Create(std::move(commit_context)));
        return file_store_commit->Commit(commit_msgs);
    }

    void ScanAndVerify(const std::string& table_path, const arrow::FieldVector& fields,
                       const std::map<std::pair<std::string, int32_t>, std::string>&
                           expected_data_per_partition_bucket) {
        std::map<std::string, std::string> options = {{Options::FILE_SYSTEM, "local"}};
        ASSERT_OK_AND_ASSIGN(auto helper,
                             TestHelper::Create(table_path, options, /*is_streaming_mode=*/false));
        ASSERT_OK_AND_ASSIGN(
            std::vector<std::shared_ptr<Split>> data_splits,
            helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));

        arrow::FieldVector fields_with_row_kind = fields;
        fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                    arrow::field("_VALUE_KIND", arrow::int8()));
        auto data_type = arrow::struct_(fields_with_row_kind);

        std::map<std::pair<std::string, int32_t>, std::vector<std::shared_ptr<Split>>>
            splits_by_partition_bucket;
        for (const auto& split : data_splits) {
            auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
            ASSERT_OK_AND_ASSIGN(std::string partition_str,
                                 helper->PartitionStr(split_impl->Partition()));
            splits_by_partition_bucket[std::make_pair(partition_str, split_impl->Bucket())]
                .push_back(split);
        }

        ASSERT_EQ(splits_by_partition_bucket.size(), expected_data_per_partition_bucket.size());
        for (const auto& [key, splits] : splits_by_partition_bucket) {
            auto iter = expected_data_per_partition_bucket.find(key);
            ASSERT_TRUE(iter != expected_data_per_partition_bucket.end())
                << "Unexpected partition=" << key.first << " bucket=" << key.second;
            ASSERT_OK_AND_ASSIGN(bool success,
                                 helper->ReadAndCheckResult(data_type, splits, iter->second));
            ASSERT_TRUE(success);
        }
    }

    void CheckCommitMessage(const std::shared_ptr<CommitMessage>& msg, size_t expected_before_files,
                            int64_t expected_total_rows) {
        auto impl = dynamic_cast<CommitMessageImpl*>(msg.get());
        ASSERT_TRUE(impl);
        ASSERT_EQ(impl->Bucket(), 0);

        const auto& compact_before = impl->GetCompactIncrement().CompactBefore();
        const auto& compact_after = impl->GetCompactIncrement().CompactAfter();
        ASSERT_EQ(compact_before.size(), expected_before_files);
        ASSERT_EQ(compact_after.size(), 1);

        int64_t total_before_rows = 0;
        for (const auto& file : compact_before) {
            total_before_rows += file->row_count;
        }
        ASSERT_EQ(total_before_rows, expected_total_rows);
        ASSERT_EQ(compact_after[0]->row_count, expected_total_rows);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> dir_;
    arrow::FieldVector fields_;
};

/// Test that AppendCompactCoordinator::Run compacts all partitions' small files.
///
/// Steps:
///   1. Create an unaware-bucket append-only table (bucket=-1) with partition key "f1".
///   2. Write 4 batches across 2 partitions (f1=10, f1=20), producing multiple small files.
///   3. Call AppendCompactCoordinator::Run with empty partitions (compact all).
///   4. Commit the compact results.
///   5. Verify that the data is correct after compaction.
TEST_F(AppendCompactCoordinatorTest, TestRunCompactsAllPartitions) {
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},
        {Options::COMPACTION_MIN_FILE_NUM, "2"},
    };

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    CreateTable(fields, /*partition_keys=*/{"f1"}, options);

    auto table_path = TablePath();
    auto data_type = arrow::struct_(fields);

    // Write batch 1: partition f1=10
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 1, 11.1],
            ["Bob", 10, 0, 12.1],
            ["Emily", 10, 0, 13.1],
            ["Tony", 10, 0, 14.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, /*bucket=*/0, array,
                                 /*commit_identifier=*/0));
    }

    // Write batch 2: partition f1=10
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Emily", 10, 0, 15.1],
            ["Bob", 10, 0, 12.1],
            ["Alex", 10, 0, 16.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, /*bucket=*/0, array,
                                 /*commit_identifier=*/1));
    }

    // Write batch 3: partition f1=20
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Lucy", 20, 1, 14.1],
            ["Paul", 20, 1, null]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "20"}}, /*bucket=*/0, array,
                                 /*commit_identifier=*/2));
    }

    // Write batch 4: partition f1=20 (another small file)
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["David", 20, 0, 17.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "20"}}, /*bucket=*/0, array,
                                 /*commit_identifier=*/3));
    }

    // Run AppendCompactCoordinator to compact all partitions
    ASSERT_OK_AND_ASSIGN(
        auto compact_messages,
        AppendCompactCoordinator::Run(table_path, options,
                                      /*partitions=*/{}, /*file_system=*/nullptr, pool_));

    // Verify commit messages: 2 messages (one per partition)
    ASSERT_EQ(compact_messages.size(), 2);
    // f1=10: 2 files compacted into 1, total 7 rows
    CheckCommitMessage(compact_messages[0],
                       /*expected_before_files=*/2,
                       /*expected_total_rows=*/7);
    // f1=20: 2 files compacted into 1, total 3 rows
    CheckCommitMessage(compact_messages[1],
                       /*expected_before_files=*/2,
                       /*expected_total_rows=*/3);

    // Commit compact results
    ASSERT_OK(Commit(table_path, compact_messages));

    // Verify data after compaction
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    // Files are sorted by file_size ascending in PackFiles, so the smaller file
    // (batch 2: 3 rows) comes before the larger file (batch 1: 4 rows).
    expected_datas[std::make_pair("f1=10/", 0)] = R"([
[0, "Emily", 10, 0, 15.1],
[0, "Bob", 10, 0, 12.1],
[0, "Alex", 10, 0, 16.1],
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1]
])";

    expected_datas[std::make_pair("f1=20/", 0)] = R"([
[0, "David", 20, 0, 17.1],
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, null]
])";

    ScanAndVerify(table_path, fields, expected_datas);
}

/// Test that AppendCompactCoordinator::Run only compacts the specified partition.
///
/// Steps:
///   1. Create an unaware-bucket append-only table with partition key "f1".
///   2. Write data to both f1=10 and f1=20 partitions.
///   3. Call Run with partitions=[{f1=10}], so only f1=10 is compacted.
///   4. Verify only 1 commit message for f1=10, and data is correct for both partitions.
TEST_F(AppendCompactCoordinatorTest, TestRunCompactsSinglePartition) {
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},
        {Options::COMPACTION_MIN_FILE_NUM, "2"},
    };

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    CreateTable(fields, /*partition_keys=*/{"f1"}, options);

    auto table_path = TablePath();
    auto data_type = arrow::struct_(fields);

    // Write 2 batches to f1=10
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 1, 11.1],
            ["Bob", 10, 0, 12.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, 0));
    }
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Emily", 10, 0, 13.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, 1));
    }

    // Write 2 batches to f1=20
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Lucy", 20, 1, 14.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "20"}}, 0, array, 2));
    }
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Paul", 20, 0, 15.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "20"}}, 0, array, 3));
    }

    // Only compact partition f1=10
    ASSERT_OK_AND_ASSIGN(auto compact_messages,
                         AppendCompactCoordinator::Run(table_path, options,
                                                       /*partitions=*/{{{"f1", "10"}}},
                                                       /*file_system=*/nullptr, pool_));

    // Should only have 1 commit message for f1=10
    ASSERT_EQ(compact_messages.size(), 1);
    CheckCommitMessage(compact_messages[0], /*expected_before_files=*/2,
                       /*expected_total_rows=*/3);

    ASSERT_OK(Commit(table_path, compact_messages));

    // Verify: f1=10 is compacted (1 file), f1=20 remains uncompacted (2 splits)
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("f1=10/", 0)] = R"([
[0, "Emily", 10, 0, 13.1],
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1]
])";

    expected_datas[std::make_pair("f1=20/", 0)] = R"([
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 0, 15.1]
])";

    ScanAndVerify(table_path, fields, expected_datas);
}

/// Test that no compaction happens when files don't meet compact conditions.
/// With COMPACTION_MIN_FILE_NUM=3, having only 2 file won't trigger compaction.
TEST_F(AppendCompactCoordinatorTest, TestNoCompactWhenConditionsNotMet) {
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},
        {Options::COMPACTION_MIN_FILE_NUM, "3"},
    };

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    CreateTable(fields, /*partition_keys=*/{}, options);

    auto table_path = TablePath();
    auto data_type = arrow::struct_(fields);

    // Write only 2 files (less than min_file_num=3)
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 1, 11.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, 0));
    }
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Bob", 10, 0, 12.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, 1));
    }

    // Run compact: should return empty since 2 files < min_file_num=3
    ASSERT_OK_AND_ASSIGN(
        auto compact_messages,
        AppendCompactCoordinator::Run(table_path, options,
                                      /*partitions=*/{}, /*file_system=*/nullptr, pool_));
    ASSERT_TRUE(compact_messages.empty());

    // Data should remain unchanged
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("", 0)] = R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1]
])";
    ScanAndVerify(table_path, fields, expected_datas);
}

/// Test compacting a partition that has no data (e.g. f1=30).
/// Should return empty compact messages without error.
TEST_F(AppendCompactCoordinatorTest, TestCompactNonExistentPartition) {
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},
        {Options::COMPACTION_MIN_FILE_NUM, "2"},
    };

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    CreateTable(fields, /*partition_keys=*/{"f1"}, options);

    auto table_path = TablePath();
    auto data_type = arrow::struct_(fields);

    // Write data only to f1=10
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 1, 11.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, 0));
    }
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Bob", 10, 0, 12.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, 1));
    }

    // Compact only f1=30 which has no data
    ASSERT_OK_AND_ASSIGN(auto compact_messages,
                         AppendCompactCoordinator::Run(table_path, options,
                                                       /*partitions=*/{{{"f1", "30"}}},
                                                       /*file_system=*/nullptr, pool_));
    ASSERT_TRUE(compact_messages.empty());

    // Original data should remain unchanged
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("f1=10/", 0)] = R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1]
])";
    ScanAndVerify(table_path, fields, expected_datas);
}

/// Test that Run fails on a bucketed table (bucket != -1).
TEST_F(AppendCompactCoordinatorTest, TestValidateFailsOnBucketedTable) {
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f0"},
        {Options::FILE_SYSTEM, "local"},
    };

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    CreateTable(fields, /*partition_keys=*/{"f1"}, options);

    ASSERT_NOK_WITH_MSG(
        AppendCompactCoordinator::Run(TablePath(), options, /*partitions=*/{},
                                      /*file_system=*/nullptr, pool_),
        "AppendCompactCoordinator only supports append-only tables with UNAWARE_BUCKET mode");
}

/// Test that Run fails on a dv table
TEST_F(AppendCompactCoordinatorTest, TestValidateFailsOnDvTable) {
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "-1"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::DELETION_VECTORS_ENABLED, "true"}};

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    CreateTable(fields, /*partition_keys=*/{"f1"}, options);

    ASSERT_NOK_WITH_MSG(AppendCompactCoordinator::Run(TablePath(), options, /*partitions=*/{},
                                                      /*file_system=*/nullptr, pool_),
                        "not support for dv in UNAWARE_BUCKET mode");
}

/// Test that compact output files are written to external path when configured.
TEST_F(AppendCompactCoordinatorTest, TestCompactWithExternalPath) {
    auto external_dir = UniqueTestDirectory::Create("local");
    ASSERT_TRUE(external_dir);
    std::string external_path = external_dir->Str();

    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},
        {Options::COMPACTION_MIN_FILE_NUM, "2"},
        {Options::DATA_FILE_EXTERNAL_PATHS, "FILE://" + external_path},
        {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"},
    };

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    CreateTable(fields, /*partition_keys=*/{}, options);

    auto table_path = TablePath();
    auto data_type = arrow::struct_(fields);

    // Write 2 batches
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 1, 11.1],
            ["Bob", 10, 0, 12.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, 0));
    }
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Emily", 10, 0, 13.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, 1));
    }

    // Run compact
    ASSERT_OK_AND_ASSIGN(auto compact_messages,
                         AppendCompactCoordinator::Run(table_path, options, /*partitions=*/{},
                                                       /*file_system=*/nullptr, pool_));
    ASSERT_EQ(compact_messages.size(), 1);
    CheckCommitMessage(compact_messages[0], /*expected_before_files=*/2,
                       /*expected_total_rows=*/3);

    // Verify compact output files have external_path set
    {
        bool found_compact_after_with_external_path = false;
        for (const auto& msg : compact_messages) {
            auto impl = dynamic_cast<CommitMessageImpl*>(msg.get());
            ASSERT_TRUE(impl);
            for (const auto& file_meta : impl->GetCompactIncrement().CompactAfter()) {
                ASSERT_TRUE(file_meta->external_path.has_value())
                    << "Compact output file " << file_meta->file_name
                    << " should have external_path set";
                found_compact_after_with_external_path = true;
            }
        }
        ASSERT_TRUE(found_compact_after_with_external_path)
            << "Should have at least one compact output file";
    }

    // Verify files physically exist in external path directory
    {
        auto filesystem = external_dir->GetFileSystem();
        auto bucket_dir = external_path + "/bucket-0/";
        std::vector<std::unique_ptr<BasicFileStatus>> file_statuses;
        ASSERT_OK(filesystem->ListDir(bucket_dir, &file_statuses));
        ASSERT_EQ(file_statuses.size(), 3)
            << "External path directory should contain compact output files";
    }

    ASSERT_OK(Commit(table_path, compact_messages));

    // Verify data is correct after compaction
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("", 0)] = R"([
[0, "Emily", 10, 0, 13.1],
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1]
])";
    ScanAndVerify(table_path, fields, expected_datas);
}

/// Test that no compaction happens when file sizes exceed compaction_file_size threshold.
/// With a very small target-file-size (100 bytes), compaction_file_size = 100/10*7 = 70 bytes.
/// Parquet files are always larger than 70 bytes, so ScanSmallFiles collects nothing.
TEST_F(AppendCompactCoordinatorTest, TestNoCompactWhenFileSizeExceedsThreshold) {
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},  {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},    {Options::COMPACTION_MIN_FILE_NUM, "2"},
        {Options::TARGET_FILE_SIZE, "100"},
    };

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    CreateTable(fields, /*partition_keys=*/{}, options);

    auto table_path = TablePath();
    auto data_type = arrow::struct_(fields);

    // Write 2 files
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 1, 11.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, 0));
    }
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Bob", 10, 0, 12.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, 1));
    }

    // Run compact: files are too large relative to compaction_file_size (70 bytes)
    ASSERT_OK_AND_ASSIGN(
        auto compact_messages,
        AppendCompactCoordinator::Run(table_path, options,
                                      /*partitions=*/{}, /*file_system=*/nullptr, pool_));
    ASSERT_TRUE(compact_messages.empty());

    // Data should remain unchanged
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("", 0)] = R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1]
])";
    ScanAndVerify(table_path, fields, expected_datas);
}

/// Test that compaction triggers via EnoughContent condition.
/// With min_file_num=100 (so EnoughInputFiles won't trigger) and a large open-file-cost,
/// the total_file_size (file_size + open_file_cost per file) exceeds target_file_size * 2,
/// satisfying EnoughContent and triggering compaction.
TEST_F(AppendCompactCoordinatorTest, TestCompactViaEnoughContent) {
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},
        {Options::COMPACTION_MIN_FILE_NUM, "100"},
        {Options::SOURCE_SPLIT_OPEN_FILE_COST, "256mb"},
    };

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    CreateTable(fields, /*partition_keys=*/{}, options);

    auto table_path = TablePath();
    auto data_type = arrow::struct_(fields);

    // Write 2 files
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 1, 11.1],
            ["Bob", 10, 0, 12.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, 0));
    }
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Emily", 10, 0, 13.1]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, 1));
    }

    // Run compact: 2 files < min_file_num=100, but EnoughContent triggers because
    // total_file_size = 2 * (file_size + 256MB) >= 256MB * 2 = 512MB
    ASSERT_OK_AND_ASSIGN(
        auto compact_messages,
        AppendCompactCoordinator::Run(table_path, options,
                                      /*partitions=*/{}, /*file_system=*/nullptr, pool_));
    ASSERT_EQ(compact_messages.size(), 1);
    CheckCommitMessage(compact_messages[0], /*expected_before_files=*/2,
                       /*expected_total_rows=*/3);

    ASSERT_OK(Commit(table_path, compact_messages));

    // Verify data after compaction
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("", 0)] = R"([
[0, "Emily", 10, 0, 13.1],
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 0, 12.1]
])";
    ScanAndVerify(table_path, fields, expected_datas);
}

/// Test compact with schema evolution (alter table):
///   - Add a new column f4(int32)
///   - Drop column f3(float64)
///   - Change f2 type from int32 to utf8
/// Write data with old schema, alter table, write data with new schema, then compact.
TEST_F(AppendCompactCoordinatorTest, TestCompactWithSchemaEvolution) {
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},
        {Options::COMPACTION_MIN_FILE_NUM, "2"},
    };

    // Schema-0: f0(utf8, id=0), f1(int32, id=1), f2(int32, id=2), f3(float64, id=3)
    arrow::FieldVector fields_v0 = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    CreateTable(fields_v0, /*partition_keys=*/{}, options);

    auto table_path = TablePath();
    auto data_type_v0 = arrow::struct_(fields_v0);

    // Write batch with schema-0
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type_v0, R"([
            ["Alice", 10, 1, 11.1],
            ["Bob", 20, 2, 22.2]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, 0));
    }

    // Alter table: create schema-1
    //   - f2: int32 -> utf8 (id=2)
    //   - f3: dropped
    //   - f0: utf8 (id=0, unchanged)
    //   - f1: int32 (id=1, unchanged)
    //   - f4: new int32 column (id=4)
    auto fs = dir_->GetFileSystem();
    auto schema_manager = std::make_shared<SchemaManager>(fs, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema0, schema_manager->ReadSchema(0));
    auto table_schema1 = std::make_shared<TableSchema>(*table_schema0);
    std::vector<DataField> new_fields = {DataField(1, arrow::field("f1", arrow::int32())),
                                         DataField(2, arrow::field("f2", arrow::utf8())),
                                         DataField(4, arrow::field("f4", arrow::int32())),
                                         DataField(0, arrow::field("f0", arrow::utf8()))};
    table_schema1->id_ = 1;
    table_schema1->fields_ = new_fields;
    table_schema1->highest_field_id_ = 4;
    ASSERT_OK_AND_ASSIGN(std::string schema_content, table_schema1->ToJsonString());
    ASSERT_OK(fs->AtomicStore(schema_manager->ToSchemaPath(1), schema_content));

    auto data_type_v1 = DataField::ConvertDataFieldsToArrowStructType(new_fields);

    // Write batch with schema-1
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type_v1, R"([
            [30, "three", 300, "Carol"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, 1));
    }

    // Run compact: should compact 2 files (one from schema-0, one from schema-1)
    ASSERT_OK_AND_ASSIGN(auto compact_messages,
                         AppendCompactCoordinator::Run(table_path, options, /*partitions=*/{},
                                                       /*file_system=*/nullptr, pool_));
    ASSERT_EQ(compact_messages.size(), 1);
    CheckCommitMessage(compact_messages[0], /*expected_before_files=*/2,
                       /*expected_total_rows=*/3);

    ASSERT_OK(Commit(table_path, compact_messages));

    // Verify data after compaction using the latest schema (schema-1)
    arrow::FieldVector fields_v1 = {
        arrow::field("f1", arrow::int32()), arrow::field("f2", arrow::utf8()),
        arrow::field("f4", arrow::int32()), arrow::field("f0", arrow::utf8())};
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("", 0)] = R"([
[0, 30, "three", 300, "Carol"],
[0, 10, "1", null, "Alice"],
[0, 20, "2", null, "Bob"]
])";
    ScanAndVerify(table_path, fields_v1, expected_datas);
}

}  // namespace paimon::test
