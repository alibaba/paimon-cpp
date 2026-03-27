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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/commit_context.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/defs.h"
#include "paimon/disk/io_manager.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

class PkCompactionInteTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create("local");
    }

    void TearDown() override {
        dir_.reset();
    }

    // ---- Table creation ----

    void CreateTable(const arrow::FieldVector& fields,
                     const std::vector<std::string>& partition_keys,
                     const std::vector<std::string>& primary_keys,
                     const std::map<std::string, std::string>& options) {
        fields_ = fields;
        auto schema = arrow::schema(fields_);
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir_->Str(), options));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema, partition_keys,
                                       primary_keys, options,
                                       /*ignore_if_exists=*/false));
    }

    std::string TablePath() const {
        return PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    }

    // ---- Write helpers ----

    Result<std::vector<std::shared_ptr<CommitMessage>>> WriteArray(
        const std::string& table_path, const std::map<std::string, std::string>& partition,
        int32_t bucket, const std::shared_ptr<arrow::Array>& write_array, int64_t commit_identifier,
        bool is_streaming = true) const {
        auto io_manager = std::shared_ptr<IOManager>(
            IOManager::Create(PathUtil::JoinPath(dir_->Str(), "tmp")).release());
        WriteContextBuilder write_builder(table_path, "commit_user_1");
        write_builder.WithStreamingMode(is_streaming).WithIOManager(io_manager);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto file_store_write,
                               FileStoreWrite::Create(std::move(write_context)));
        ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*write_array, &c_array).ok());
        auto record_batch = std::make_unique<RecordBatch>(
            partition, bucket, std::vector<RecordBatch::RowKind>(), &c_array);
        PAIMON_RETURN_NOT_OK(file_store_write->Write(std::move(record_batch)));
        PAIMON_ASSIGN_OR_RAISE(auto commit_msgs, file_store_write->PrepareCommit(
                                                     /*wait_compaction=*/false, commit_identifier));
        PAIMON_RETURN_NOT_OK(file_store_write->Close());
        return commit_msgs;
    }

    Status Commit(const std::string& table_path,
                  const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        CommitContextBuilder commit_builder(table_path, "commit_user_1");
        std::map<std::string, std::string> commit_options = {
            {"enable-pk-commit-in-inte-test", ""}, {"enable-object-store-commit-in-inte-test", ""}};
        commit_builder.SetOptions(commit_options);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context,
                               commit_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> file_store_commit,
                               FileStoreCommit::Create(std::move(commit_context)));
        return file_store_commit->Commit(commit_msgs);
    }

    Status WriteAndCommit(const std::string& table_path,
                          const std::map<std::string, std::string>& partition, int32_t bucket,
                          const std::shared_ptr<arrow::Array>& write_array,
                          int64_t commit_identifier) {
        PAIMON_ASSIGN_OR_RAISE(auto commit_msgs, WriteArray(table_path, partition, bucket,
                                                            write_array, commit_identifier));
        return Commit(table_path, commit_msgs);
    }

    // ---- Compact helpers ----

    Result<std::vector<std::shared_ptr<CommitMessage>>> CompactAndCommit(
        const std::string& table_path, const std::map<std::string, std::string>& partition,
        int32_t bucket, bool full_compaction, int64_t commit_identifier) {
        auto io_manager = std::shared_ptr<IOManager>(
            IOManager::Create(PathUtil::JoinPath(dir_->Str(), "tmp")).release());
        WriteContextBuilder write_builder(table_path, "commit_user_1");
        write_builder.WithStreamingMode(true).WithIOManager(io_manager);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto file_store_write,
                               FileStoreWrite::Create(std::move(write_context)));
        PAIMON_RETURN_NOT_OK(file_store_write->Compact(partition, bucket, full_compaction));
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::shared_ptr<CommitMessage>> commit_messages,
            file_store_write->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
        PAIMON_RETURN_NOT_OK(file_store_write->Close());
        PAIMON_RETURN_NOT_OK(Commit(table_path, commit_messages));
        return commit_messages;
    }

    // ---- Read & verify helpers ----

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

        ASSERT_EQ(data_splits.size(), expected_data_per_partition_bucket.size());
        for (const auto& split : data_splits) {
            auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
            ASSERT_OK_AND_ASSIGN(std::string partition_str,
                                 helper->PartitionStr(split_impl->Partition()));
            auto iter = expected_data_per_partition_bucket.find(
                std::make_pair(partition_str, split_impl->Bucket()));
            ASSERT_TRUE(iter != expected_data_per_partition_bucket.end())
                << "Unexpected partition=" << partition_str << " bucket=" << split_impl->Bucket();
            ASSERT_OK_AND_ASSIGN(bool success,
                                 helper->ReadAndCheckResult(data_type, {split}, iter->second));
            ASSERT_TRUE(success);
        }
    }

    // Helper: check whether compact commit messages contain new DV index files.
    bool HasDeletionVectorIndexFiles(
        const std::vector<std::shared_ptr<CommitMessage>>& commit_messages) {
        for (const auto& msg : commit_messages) {
            auto impl = dynamic_cast<CommitMessageImpl*>(msg.get());
            if (impl) {
                for (const auto& index_file : impl->GetCompactIncrement().NewIndexFiles()) {
                    if (index_file->IndexType() ==
                        DeletionVectorsIndexFile::DELETION_VECTORS_INDEX) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

 private:
    std::unique_ptr<UniqueTestDirectory> dir_;
    arrow::FieldVector fields_;
};

// Test: deduplicate merge engine with deletion vectors enabled.
// Verifies that a non-full compact produces DV index files when level-0 files
// overlap with high-level files, and that data is correct after DV compact and full compact.
//
// Strategy:
//   1. Write batch1 (large) and commit → level-0 file
//   2. Full compact → file is upgraded to max level
//   3. Write batch2 with overlapping keys and commit → first new level-0 file
//   4. Write batch3 with overlapping keys and commit → second new level-0 file
//   5. Non-full compact → two level-0 files are compacted together to an intermediate level,
//      lookup against max-level file produces DV
//   6. Assert DV index files are present in the compact's commit messages
//   7. ScanAndVerify after DV compact (data read with DV applied)
//   8. Full compact to merge everything
//   9. ScanAndVerify after full compact (all data merged)
TEST_F(PkCompactionInteTest, DeduplicateWithDeletionVectors) {
    // f4 is a large padding field to make the initial file substantially bigger than
    // subsequent small level-0 files, preventing PickForSizeRatio from merging them all.
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64()),
        arrow::field("f4", arrow::utf8())};
    std::vector<std::string> primary_keys = {"f0", "f1", "f2"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f2"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::DELETION_VECTORS_ENABLED, "true"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    // A long padding string (~2KB) to inflate the initial file size.
    std::string padding(2048, 'X');

    // Step 1: Write initial data with large padding field (creates a big level-0 file).
    {
        // clang-format off
        std::string json_data = R"([
["Alice", 10, 0, 1.0, ")" + padding + R"("],
["Bob",   10, 0, 2.0, ")" + padding + R"("],
["Carol", 10, 0, 3.0, ")" + padding + R"("],
["Dave",  10, 0, 4.0, ")" + padding + R"("],
["Eve",   10, 0, 5.0, ")" + padding + R"("]
])";
        // clang-format on
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 2: Full compact → upgrades level-0 file to max level (large file).
    {
        ASSERT_OK_AND_ASSIGN(
            auto compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "First compact should not produce DV index files";
    }

    // Step 3: Write batch2 with overlapping keys and short padding (small level-0 file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 0, 101.0, "u1"],
            ["Bob",   10, 0, 102.0, "u2"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 4: Write batch3 with overlapping keys and short padding (second small level-0 file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Bob",   10, 0, 202.0, "u3"],
            ["Carol", 10, 0, 203.0, "u4"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 5: Non-full compact → ForcePickL0 picks both level-0 files, merges them together
    // to an intermediate level. Lookup against max-level file produces DV for overlapping keys.
    {
        ASSERT_OK_AND_ASSIGN(auto compact_msgs,
                             CompactAndCommit(table_path, {{"f1", "10"}}, 0,
                                              /*full_compaction=*/false, commit_id++));
        ASSERT_TRUE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Non-full compact must produce DV index files for overlapping keys";
    }

    // Step 6: ScanAndVerify after DV compact.
    // Scan reads max-level file first (Dave, Eve after DV filters out Alice/Bob/Carol),
    // then intermediate-level file (Alice, Bob, Carol with updated values).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("f1=10/", 0)] = R"([
[0, "Dave",  10, 0, 4.0, ")" + padding + R"("],
[0, "Eve",   10, 0, 5.0, ")" + padding + R"("],
[0, "Alice", 10, 0, 101.0, "u1"],
[0, "Bob",   10, 0, 202.0, "u3"],
[0, "Carol", 10, 0, 203.0, "u4"]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }

    // Step 7: Full compact to merge everything.
    ASSERT_OK_AND_ASSIGN(
        auto final_compact_msgs,
        CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));

    // Step 8: ScanAndVerify after full compact (globally sorted after merge).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("f1=10/", 0)] = R"([
[0, "Alice", 10, 0, 101.0, "u1"],
[0, "Bob",   10, 0, 202.0, "u3"],
[0, "Carol", 10, 0, 203.0, "u4"],
[0, "Dave",  10, 0, 4.0, ")" + padding + R"("],
[0, "Eve",   10, 0, 5.0, ")" + padding + R"("]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }
}

// Test: PK table compact writes output files to external path.
// Verifies that after configuring external paths with round-robin strategy,
// compact output files (compactAfter) have their external_path set, and the
// files physically exist in the external directory.
TEST_F(PkCompactionInteTest, CompactWithExternalPath) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    std::vector<std::string> primary_keys = {"f0", "f1", "f2"};
    std::vector<std::string> partition_keys = {"f1"};

    // Create external path directories.
    auto external_dir = UniqueTestDirectory::Create("local");
    ASSERT_TRUE(external_dir);
    std::string external_path = external_dir->Str();

    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
        {Options::FILE_SYSTEM, "local"},
        {Options::DATA_FILE_EXTERNAL_PATHS, "FILE://" + external_path},
        {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    // Step 1: Write initial data and commit.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 0, 1.0],
            ["Bob",   10, 0, 2.0],
            ["Carol", 10, 0, 3.0]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 2: Write overlapping data to create a second level-0 file.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 0, 101.0],
            ["Dave",  10, 0, 4.0]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 3: Full compact → merges all files. Compact output should go to external path.
    ASSERT_OK_AND_ASSIGN(
        auto compact_msgs,
        CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));

    // Step 4: Verify compact output files have external_path set.
    {
        bool found_compact_after_with_external_path = false;
        for (const auto& msg : compact_msgs) {
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

    // Step 5: Verify files physically exist in external path directory.
    {
        auto filesystem = external_dir->GetFileSystem();
        auto bucket_dir = external_path + "/f1=10/bucket-0/";
        std::vector<std::unique_ptr<BasicFileStatus>> file_statuses;
        ASSERT_OK(filesystem->ListDir(bucket_dir, &file_statuses));
        ASSERT_FALSE(file_statuses.empty())
            << "External path directory should contain compact output files";
    }

    // Step 6: ScanAndVerify to ensure data is correct after compact.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Alice", 10, 0, 101.0],
            [0, "Bob",   10, 0, 2.0],
            [0, "Carol", 10, 0, 3.0],
            [0, "Dave",  10, 0, 4.0]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }
}

// Test: PK table with aggregation merge engine (min) using all non-nested field types,
// no partitions, primary keys at the front, DV enabled.
// Boolean field uses bool_and aggregation; all other value fields use min.
// Verifies DV index files are produced and data is correct after compact.
//
// Strategy:
//   1. Write batch1 (large padding) and commit → level-0 file
//   2. Full compact → upgrades to max level (large file)
//   3. Write batch2 with overlapping keys → first new level-0 file
//   4. Write batch3 with overlapping keys → second new level-0 file
//   5. Non-full compact → two level-0 files merge, lookup against max-level produces DV
//   6. Assert DV index files are present
//   7. ScanAndVerify after DV compact
//   8. Full compact to merge everything
//   9. ScanAndVerify after full compact
TEST_F(PkCompactionInteTest, AggMinWithAllNonNestedTypes) {
    // f15 is a large padding field to inflate the initial file size for DV strategy.
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),   // PK
                                 arrow::field("f1", arrow::int32()),  // PK
                                 arrow::field("f2", arrow::int8()),
                                 arrow::field("f3", arrow::int16()),
                                 arrow::field("f4", arrow::int32()),
                                 arrow::field("f5", arrow::int64()),
                                 arrow::field("f6", arrow::float32()),
                                 arrow::field("f7", arrow::boolean()),  // bool_and agg
                                 arrow::field("f8", arrow::float64()),
                                 arrow::field("f9", arrow::binary()),
                                 arrow::field("f10", arrow::timestamp(arrow::TimeUnit::NANO)),
                                 arrow::field("f11", arrow::timestamp(arrow::TimeUnit::SECOND)),
                                 arrow::field("f12", arrow::date32()),
                                 arrow::field("f13", arrow::decimal128(10, 2)),
                                 arrow::field("f14", arrow::decimal128(23, 2)),
                                 arrow::field("f15", arrow::utf8())};  // padding field
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {};

    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "1"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::MERGE_ENGINE, "aggregation"},
                                                  {Options::FIELDS_DEFAULT_AGG_FUNC, "min"},
                                                  {Options::DELETION_VECTORS_ENABLED, "true"},
                                                  {"fields.f7.aggregate-function", "bool_and"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    // A long padding string (~2KB) to inflate the initial file size.
    std::string padding(2048, 'X');

    // Step 1: Write initial data with large padding (creates a big level-0 file).
    // key=("Alice",1), key=("Bob",2), key=("Carol",3), key=("Dave",4), key=("Eve",5)
    // Dave and Eve are NOT overwritten by later batches, so DV will only mark Alice/Bob/Carol.
    {
        // clang-format off
        std::string json_data = R"([
["Alice", 1, 10, 100, 1000, 10000, 1.5, true,  2.5, "YWJj", 1000000000, 1000, 100, "12345678.99", "12345678901234567890.99", ")" + padding + R"("],
["Bob",   2, 20, 200, 2000, 20000, 2.5, true,  3.5, "ZGVm", 2000000000, 2000, 200, "99999999.99", "99999999999999999999.99", ")" + padding + R"("],
["Carol", 3, 30, 300, 3000, 30000, 3.5, false, 4.5, "enp6", 3000000000, 3000, 300, "55555555.55", "55555555555555555555.55", ")" + padding + R"("],
["Dave",  4, 40, 400, 4000, 40000, 4.5, true,  5.5, "RGWF", 4000000000, 4000, 400, "44444444.44", "44444444444444444444.44", ")" + padding + R"("],
["Eve",   5, 50, 500, 5000, 50000, 5.5, false, 6.5, "RXZF", 5000000000, 5000, 500, "66666666.66", "66666666666666666666.66", ")" + padding + R"("]
])";
        // clang-format on
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
    }

    // Step 2: Full compact → upgrades level-0 file to max level.
    ASSERT_OK_AND_ASSIGN(
        auto upgrade_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));

    // Step 3: Write second batch with overlapping keys (first new level-0 file).
    // key=("Alice", 1) with smaller values, key=("Bob", 2) with larger values.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 1, 5,  50,  500,  5000,  0.5, false, 1.5, "YQ==", 500000000,  500,  50,  "00000001.00", "00000000000000000001.00", "a"],
            ["Bob",   2, 30, 300, 3000, 30000, 3.5, true,  4.5, "enp6", 3000000000, 3000, 300, "99999999.99", "99999999999999999999.99", "b"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
    }

    // Step 4: Write third batch with overlapping keys (second new level-0 file).
    // key=("Alice", 1) with medium values, key=("Carol", 3) with smaller values.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 1, 8,  80,  800,  8000,  1.0, true,  2.0, "YWI=", 800000000,  800,  80,  "05000000.00", "05000000000000000000.00", "c"],
            ["Carol", 3, 10, 100, 1000, 10000, 1.5, false, 2.5, "YQ==", 1000000000, 1000, 100, "11111111.11", "11111111111111111111.11", "d"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
    }

    // Step 5: Non-full compact → two level-0 files merge, lookup against max-level produces DV.
    ASSERT_OK_AND_ASSIGN(
        auto dv_compact_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/false, commit_id++));

    // Step 6: Assert DV index files are present.
    ASSERT_TRUE(HasDeletionVectorIndexFiles(dv_compact_msgs))
        << "Non-full compact should produce DV index files";

    // Step 7: ScanAndVerify after DV compact.
    // Agg min merges the two level-0 files; DV marks overlapping rows in max-level file.
    // Dave and Eve are untouched in max-level file (no DV).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("", 0)] = R"([
[0, "Dave",  4, 40, 400, 4000, 40000, 4.5, true,  5.5, "RGWF", 4000000000, 4000, 400, "44444444.44", "44444444444444444444.44", ")" + padding + R"("],
[0, "Eve",   5, 50, 500, 5000, 50000, 5.5, false, 6.5, "RXZF", 5000000000, 5000, 500, "66666666.66", "66666666666666666666.66", ")" + padding + R"("],
[0, "Alice", 1, 5,  50,  500,  5000,  0.5, false, 1.5, "YQ==", 500000000,  500,  50,  "00000001.00", "00000000000000000001.00", ")" + padding + R"("],
[0, "Bob",   2, 20, 200, 2000, 20000, 2.5, true,  3.5, "ZGVm", 2000000000, 2000, 200, "99999999.99", "99999999999999999999.99", ")" + padding + R"("],
[0, "Carol", 3, 10, 100, 1000, 10000, 1.5, false, 2.5, "YQ==", 1000000000, 1000, 100, "11111111.11", "11111111111111111111.11", ")" + padding + R"("]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }

    // Step 8: Full compact to merge everything (DV + max-level + intermediate).
    ASSERT_OK_AND_ASSIGN(
        auto full_compact_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));

    // Step 9: ScanAndVerify after full compact.
    // All batches merged with min aggregation. Dave and Eve only have batch1 values.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("", 0)] = R"([
[0, "Alice", 1, 5,  50,  500,  5000,  0.5, false, 1.5, "YQ==", 500000000,  500,  50,  "00000001.00", "00000000000000000001.00", ")" + padding + R"("],
[0, "Bob",   2, 20, 200, 2000, 20000, 2.5, true,  3.5, "ZGVm", 2000000000, 2000, 200, "99999999.99", "99999999999999999999.99", ")" + padding + R"("],
[0, "Carol", 3, 10, 100, 1000, 10000, 1.5, false, 2.5, "YQ==", 1000000000, 1000, 100, "11111111.11", "11111111111111111111.11", ")" + padding + R"("],
[0, "Dave",  4, 40, 400, 4000, 40000, 4.5, true,  5.5, "RGWF", 4000000000, 4000, 400, "44444444.44", "44444444444444444444.44", ")" + padding + R"("],
[0, "Eve",   5, 50, 500, 5000, 50000, 5.5, false, 6.5, "RXZF", 5000000000, 5000, 500, "66666666.66", "66666666666666666666.66", ")" + padding + R"("]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }
}

// Test: PK table with aggregation merge engine (min), primary keys in the MIDDLE of the
// field list (not at the front). Enable DV.
//
// Strategy (same DV pattern as other tests):
//   1. Write batch1 (5 keys, large padding) → level-0
//   2. Full compact → upgrade to max level
//   3. Write batch2 (overlap Alice, Bob) → level-0
//   4. Write batch3 (overlap Alice, Carol) → level-0
//   5. Non-full compact → merge level-0 files, DV on max-level overlapping rows
//   6. Assert DV index files present
//   7. ScanAndVerify after DV compact
//   8. Full compact
//   9. ScanAndVerify after full compact
TEST_F(PkCompactionInteTest, AggMinWithPkInMiddle) {
    // f4 is a large padding field to inflate the initial file size for DV strategy.
    // PK fields f1(utf8) and f2(int32) are deliberately placed in the middle of the schema,
    // and the PK declaration order (f2, f1) differs from the schema order (f1, f2).
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),    // value field (min agg)
        arrow::field("f1", arrow::utf8()),     // PK (schema index 1, pk index 1)
        arrow::field("f2", arrow::int32()),    // PK (schema index 2, pk index 0)
        arrow::field("f3", arrow::float64()),  // value field (min agg)
        arrow::field("f4", arrow::utf8())};    // padding value field (min agg)
    std::vector<std::string> primary_keys = {"f2", "f1"};
    std::vector<std::string> partition_keys = {};

    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},         {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, "local"},           {Options::MERGE_ENGINE, "aggregation"},
        {Options::FIELDS_DEFAULT_AGG_FUNC, "min"}, {Options::DELETION_VECTORS_ENABLED, "true"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    // A long padding string (~2KB) to inflate the initial file size.
    std::string padding(2048, 'X');

    // Step 1: Write initial data with large padding (creates a big level-0 file).
    // Dave and Eve are NOT overwritten by later batches.
    {
        // clang-format off
        std::string json_data = R"([
[100, "Alice", 3, 1.5, ")" + padding + R"("],
[200, "Bob",   5, 2.5, ")" + padding + R"("],
[300, "Carol", 1, 3.5, ")" + padding + R"("],
[400, "Dave",  4, 4.5, ")" + padding + R"("],
[500, "Eve",   2, 5.5, ")" + padding + R"("]
])";
        // clang-format on
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
    }

    // Step 2: Full compact → upgrades level-0 file to max level.
    ASSERT_OK_AND_ASSIGN(
        auto upgrade_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));

    // Step 3: Write batch2 with overlapping keys (first new level-0 file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            [50,  "Alice", 3, 0.5, "a1"],
            [300, "Bob",   5, 3.5, "b1"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
    }

    // Step 4: Write batch3 with overlapping keys (second new level-0 file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            [80,  "Alice", 3, 1.0, "a2"],
            [150, "Carol", 1, 1.5, "c1"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
    }

    // Step 5: Non-full compact → two level-0 files merge, lookup against max-level produces DV.
    ASSERT_OK_AND_ASSIGN(
        auto dv_compact_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/false, commit_id++));

    // Step 6: Assert DV index files are present.
    ASSERT_TRUE(HasDeletionVectorIndexFiles(dv_compact_msgs))
        << "Non-full compact should produce DV index files";

    // Step 7: ScanAndVerify after DV compact.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("", 0)] = R"([
[0, 500, "Eve",   2, 5.5, ")" + padding + R"("],
[0, 400, "Dave",  4, 4.5, ")" + padding + R"("],
[0, 150, "Carol", 1, 1.5, ")" + padding + R"("],
[0, 50,  "Alice", 3, 0.5, ")" + padding + R"("],
[0, 200, "Bob",   5, 2.5, ")" + padding + R"("]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }

    // Step 8: Full compact to merge everything.
    ASSERT_OK_AND_ASSIGN(
        auto full_compact_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));

    // Step 9: ScanAndVerify after full compact.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("", 0)] = R"([
[0, 150, "Carol", 1, 1.5, ")" + padding + R"("],
[0, 500, "Eve",   2, 5.5, ")" + padding + R"("],
[0, 50,  "Alice", 3, 0.5, ")" + padding + R"("],
[0, 400, "Dave",  4, 4.5, ")" + padding + R"("],
[0, 200, "Bob",   5, 2.5, ")" + padding + R"("]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }
}

}  // namespace paimon::test
