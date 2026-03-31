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
        return WriteArrayWithRowKinds(table_path, partition, bucket, write_array,
                                      std::vector<RecordBatch::RowKind>(), commit_identifier,
                                      is_streaming);
    }

    Result<std::vector<std::shared_ptr<CommitMessage>>> WriteArrayWithRowKinds(
        const std::string& table_path, const std::map<std::string, std::string>& partition,
        int32_t bucket, const std::shared_ptr<arrow::Array>& write_array,
        const std::vector<RecordBatch::RowKind>& row_kinds, int64_t commit_identifier,
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
        auto record_batch = std::make_unique<RecordBatch>(partition, bucket, row_kinds, &c_array);
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
                          int64_t commit_identifier, bool streaming_mode = true) {
        PAIMON_ASSIGN_OR_RAISE(auto commit_msgs,
                               WriteArray(table_path, partition, bucket, write_array,
                                          commit_identifier, streaming_mode));
        return Commit(table_path, commit_msgs);
    }

    Status WriteAndCommitWithRowKinds(const std::string& table_path,
                                      const std::map<std::string, std::string>& partition,
                                      int32_t bucket,
                                      const std::shared_ptr<arrow::Array>& write_array,
                                      const std::vector<RecordBatch::RowKind>& row_kinds,
                                      int64_t commit_identifier, bool streaming_mode = true) {
        PAIMON_ASSIGN_OR_RAISE(
            auto commit_msgs, WriteArrayWithRowKinds(table_path, partition, bucket, write_array,
                                                     row_kinds, commit_identifier, streaming_mode));
        return Commit(table_path, commit_msgs);
    }

    // ---- Compact helpers ----

    Result<std::vector<std::shared_ptr<CommitMessage>>> CompactAndCommit(
        const std::string& table_path, const std::map<std::string, std::string>& partition,
        int32_t bucket, bool full_compaction, int64_t commit_identifier,
        bool streaming_mode = true) {
        auto io_manager = std::shared_ptr<IOManager>(
            IOManager::Create(PathUtil::JoinPath(dir_->Str(), "tmp")).release());
        WriteContextBuilder write_builder(table_path, "commit_user_1");
        write_builder.WithStreamingMode(streaming_mode).WithIOManager(io_manager);
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
TEST_F(PkCompactionInteTest, TestDeduplicateWithDv) {
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
        std::string json_data = R"([
            ["Alice", 10, 0, 1.0, ")" +
                                padding + R"("],
            ["Bob",   10, 0, 2.0, ")" +
                                padding + R"("],
            ["Carol", 10, 0, 3.0, ")" +
                                padding + R"("],
            ["Dave",  10, 0, 4.0, ")" +
                                padding + R"("],
            ["Eve",   10, 0, 5.0, ")" +
                                padding + R"("]
        ])";
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
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Dave",  10, 0, 4.0, ")" + padding + R"("],
            [0, "Eve",   10, 0, 5.0, ")" + padding + R"("],
            [0, "Alice", 10, 0, 101.0, "u1"],
            [0, "Bob",   10, 0, 202.0, "u3"],
            [0, "Carol", 10, 0, 203.0, "u4"]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }

    // Step 7: Full compact to merge everything.
    ASSERT_OK_AND_ASSIGN(
        auto final_compact_msgs,
        CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));

    // Step 8: ScanAndVerify after full compact (globally sorted after merge).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Alice", 10, 0, 101.0, "u1"],
            [0, "Bob",   10, 0, 202.0, "u3"],
            [0, "Carol", 10, 0, 203.0, "u4"],
            [0, "Dave",  10, 0, 4.0, ")" + padding + R"("],
            [0, "Eve",   10, 0, 5.0, ")" + padding + R"("]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }
}

TEST_F(PkCompactionInteTest, TestPartialUpdateNoDv) {
    // f2 and f3 are nullable value fields; PartialUpdate keeps the latest non-null value per field.
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32()),
                                 arrow::field("f2", arrow::int32(), /*nullable=*/true),
                                 arrow::field("f3", arrow::float64(), /*nullable=*/true)};
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "1"},
                                                  {Options::BUCKET_KEY, "f0"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::MERGE_ENGINE, "partial-update"},
                                                  {Options::DELETION_VECTORS_ENABLED, "false"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    // Step 1: Write batch_a — only f2 is non-null (f3=null).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 10,   null],
            ["Bob",   10, 20,   null]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 2: Write batch_b — only f3 is non-null (f2=null).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, null, 1.1],
            ["Bob",   10, null, 2.2]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 3: Write batch_c — update f2 for Alice only (f3=null).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 99, null]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 4: Full compact — merges all 3 L0 files into L5.
    {
        ASSERT_OK_AND_ASSIGN(
            auto compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Full compact with partial-update and DV=false must not produce DV index files";
    }

    // Step 5: ScanAndVerify — partial-update merged result.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Alice", 10, 99,   1.1],
            [0, "Bob",   10, 20,   2.2]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }
}

TEST_F(PkCompactionInteTest, TestFirstRowNoDV) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},    {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "f0"},          {Options::FILE_SYSTEM, "local"},
        {Options::MERGE_ENGINE, "first-row"}, {Options::DELETION_VECTORS_ENABLED, "false"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    // Step 1: Write batch_a — first occurrence of Alice and Bob.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 1, 1.0],
            ["Bob",   10, 2, 2.0]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 2: Full compact to push data to L5.
    {
        ASSERT_OK_AND_ASSIGN(
            auto compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Full compact with first-row + DV=false must not produce DV index files";
    }

    // Step 3: Write batch_b — duplicate Alice/Bob (should be ignored) + new Carol.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 10, 10.0],
            ["Bob",   10, 20, 20.0],
            ["Carol", 10, 30, 30.0]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 4: Non-full compact — FirstRowMergeFunctionWrapper filters out duplicate keys.
    {
        ASSERT_OK_AND_ASSIGN(auto compact_msgs,
                             CompactAndCommit(table_path, {{"f1", "10"}}, 0,
                                              /*full_compaction=*/false, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Non-full compact with first-row + DV=false must not produce DV index files";
    }

    // Step 5: ScanAndVerify — FirstRow keeps oldest values for Alice and Bob.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Alice", 10, 1,  1.0],
            [0, "Bob",   10, 2,  2.0],
            [0, "Carol", 10, 30, 30.0]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }
}

TEST_F(PkCompactionInteTest, TestPartialUpdateWithDV) {
    // f2 and f3 are nullable value fields; PartialUpdate keeps the latest non-null value
    // per field. f4 is a large padding field to inflate the initial file size so that
    // PickForSizeRatio does not merge L0 files directly into Lmax.
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32(), /*nullable=*/true),
        arrow::field("f3", arrow::float64(), /*nullable=*/true), arrow::field("f4", arrow::utf8())};
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "1"},
                                                  {Options::BUCKET_KEY, "f0"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::MERGE_ENGINE, "partial-update"},
                                                  {Options::DELETION_VECTORS_ENABLED, "true"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    // A long padding string (~2KB) to inflate the initial file size.
    std::string padding(2048, 'X');

    // Step 1: Write initial data with all fields non-null and large padding.
    {
        std::string json_data = R"([
            ["Alice", 10, 10,   1.0, ")" +
                                padding + R"("],
            ["Bob",   10, 20,   2.0, ")" +
                                padding + R"("],
            ["Carol", 10, 30,   3.0, ")" +
                                padding + R"("],
            ["Dave",  10, 40,   4.0, ")" +
                                padding + R"("],
            ["Eve",   10, 50,   5.0, ")" +
                                padding + R"("]
        ])";
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 2: Full compact → upgrades level-0 file to max level (large file at L5).
    {
        ASSERT_OK_AND_ASSIGN(
            auto compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Initial full compact should not produce DV index files";
    }

    // Step 3: Write batch_b — partial update: update f2 only (f3=null) for Alice and Bob.
    // Small level-0 file overlapping with L5.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 99,   null, "u1"],
            ["Bob",   10, 88,   null, "u2"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 4: Write batch_c — partial update: update f3 only (f2=null) for Bob and Carol.
    // Second small level-0 file overlapping with both batch_b (L0) and L5.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Bob",   10, null, 22.0, "u3"],
            ["Carol", 10, null, 33.0, "u4"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 5: Non-full compact → ForcePickL0 picks both L0 files, merges them to an
    // intermediate level. Lookup against L5 produces DV marking old rows for overlapping
    // keys (Alice, Bob, Carol).
    {
        ASSERT_OK_AND_ASSIGN(auto compact_msgs,
                             CompactAndCommit(table_path, {{"f1", "10"}}, 0,
                                              /*full_compaction=*/false, commit_id++));
        ASSERT_TRUE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Non-full compact with partial-update + DV must produce DV index files";
    }

    // Step 6: ScanAndVerify after DV compact.
    // Partial-update merged result:
    //   Alice: f2=99 (batch_b), f3=1.0 (original, batch_b had null)
    //   Bob:   f2=88 (batch_b, batch_c had null), f3=22.0 (batch_c)
    //   Carol: f2=30 (original, batch_c had null), f3=33.0 (batch_c)
    //   Dave:  unchanged from original (f2=40, f3=4.0)
    //   Eve:   unchanged from original (f2=50, f3=5.0)
    // Scan reads L5 first (Dave, Eve after DV filters out Alice/Bob/Carol),
    // then intermediate-level file (Alice, Bob, Carol with merged values).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Dave",  10, 40,   4.0, ")" + padding +
                                                     R"("],
            [0, "Eve",   10, 50,   5.0, ")" + padding +
                                                     R"("],
            [0, "Alice", 10, 99,   1.0, "u1"],
            [0, "Bob",   10, 88,   22.0, "u3"],
            [0, "Carol", 10, 30,   33.0, "u4"]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }

    // Step 7: Full compact → merge all levels into L5.
    {
        ASSERT_OK_AND_ASSIGN(
            auto final_compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
    }

    // Step 8: ScanAndVerify after full compact (globally sorted after merge).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Alice", 10, 99,   1.0, "u1"],
            [0, "Bob",   10, 88,   22.0, "u3"],
            [0, "Carol", 10, 30,   33.0, "u4"],
            [0, "Dave",  10, 40,   4.0, ")" + padding +
                                                     R"("],
            [0, "Eve",   10, 50,   5.0, ")" + padding +
                                                     R"("]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }
}

TEST_F(PkCompactionInteTest, TestDeduplicateWithDvInAllLevels) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64()),
        arrow::field("f4", arrow::utf8())};
    std::vector<std::string> primary_keys = {"f0", "f1", "f2"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "1"},
                                                  {Options::BUCKET_KEY, "f2"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::DELETION_VECTORS_ENABLED, "true"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    // A long padding string (~2KB) to inflate the initial file size.
    std::string padding(2048, 'X');

    // Step 1: Write initial data with large padding → full compact → single L5 file.
    {
        std::string json_data = R"([
            ["Alice", 10, 0, 1.0, ")" +
                                padding + R"("],
            ["Bob",   10, 0, 2.0, ")" +
                                padding + R"("],
            ["Carol", 10, 0, 3.0, ")" +
                                padding + R"("],
            ["Dave",  10, 0, 4.0, ")" +
                                padding + R"("],
            ["Eve",   10, 0, 5.0, ")" +
                                padding + R"("]
        ])";
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));

        ASSERT_OK_AND_ASSIGN(
            auto compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Initial full compact should not produce DV index files";
    }

    // Step 2: Write batch_2 (overlap Alice/Bob) → non-full compact.
    // L0 merges to intermediate level; DV marks Alice/Bob in L5.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 0, 10.0, "v2a"],
            ["Bob",   10, 0, 20.0, "v2b"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));

        ASSERT_OK_AND_ASSIGN(auto compact_msgs,
                             CompactAndCommit(table_path, {{"f1", "10"}}, 0,
                                              /*full_compaction=*/false, commit_id++));
        ASSERT_TRUE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Non-full compact #1 must produce DV for Alice/Bob in L5";
    }

    // Step 3: Write batch_3 (overlap Bob/Carol) → non-full compact.
    // L0 merges to a lower intermediate level; DV marks Bob in the intermediate file
    // from Step 2, and Carol in L5.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Bob",   10, 0, 200.0, "v3b"],
            ["Carol", 10, 0, 300.0, "v3c"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));

        ASSERT_OK_AND_ASSIGN(auto compact_msgs,
                             CompactAndCommit(table_path, {{"f1", "10"}}, 0,
                                              /*full_compaction=*/false, commit_id++));
        ASSERT_TRUE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Non-full compact #2 must produce DV for Bob (intermediate) and Carol (L5)";
    }

    // Step 4: Write batch_4 (overlap Carol/Dave) → non-full compact.
    // DV marks Carol in the file from Step 3, and Dave in L5.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Carol", 10, 0, 3000.0, "v4c"],
            ["Dave",  10, 0, 4000.0, "v4d"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));

        ASSERT_OK_AND_ASSIGN(auto compact_msgs,
                             CompactAndCommit(table_path, {{"f1", "10"}}, 0,
                                              /*full_compaction=*/false, commit_id++));
        ASSERT_TRUE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Non-full compact #3 must produce DV for Carol (intermediate) and Dave (L5)";
    }

    // Step 5: Write batch_5 (overlap Dave/Eve) → leave at L0 (no compact).
    // This ensures L0 has data while higher levels also have files.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Dave",  10, 0, 40000.0, "v5d"],
            ["Eve",   10, 0, 50000.0, "v5e"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Alice", 10, 0, 10.0, "v2a"],
            [0, "Bob",   10, 0, 200.0, "v3b"],
            [0, "Carol", 10, 0, 3000.0, "v4c"],
            [0, "Dave",  10, 0, 40000.0, "v5d"],
            [0, "Eve",   10, 0, 50000.0, "v5e"]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }
    // Step 6: Full compact → merge all levels into L5.
    {
        ASSERT_OK_AND_ASSIGN(
            auto final_compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
    }
    // Step 7: ScanAndVerify after full compact (globally sorted, all data in L5).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Alice", 10, 0, 10.0, "v2a"],
            [0, "Bob",   10, 0, 200.0, "v3b"],
            [0, "Carol", 10, 0, 3000.0, "v4c"],
            [0, "Dave",  10, 0, 40000.0, "v5d"],
            [0, "Eve",   10, 0, 50000.0, "v5e"]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }
}

TEST_F(PkCompactionInteTest, TestDeduplicateWithForceLookupNoDv) {
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
                                                  {Options::DELETION_VECTORS_ENABLED, "false"},
                                                  {Options::FORCE_LOOKUP, "true"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    // A long padding string (~2KB) to inflate the initial file size.
    std::string padding(2048, 'X');

    // Step 1: Write initial data with large padding field (creates a big level-0 file).
    {
        std::string json_data = R"([
            ["Alice", 10, 0, 1.0, ")" +
                                padding + R"("],
            ["Bob",   10, 0, 2.0, ")" +
                                padding + R"("],
            ["Carol", 10, 0, 3.0, ")" +
                                padding + R"("],
            ["Dave",  10, 0, 4.0, ")" +
                                padding + R"("],
            ["Eve",   10, 0, 5.0, ")" +
                                padding + R"("]
        ])";
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

    // Step 3: Write batch2 with overlapping keys (small level-0 file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 0, 101.0, "u1"],
            ["Bob",   10, 0, 102.0, "u2"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 4: Write batch3 with overlapping keys (second small level-0 file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Bob",   10, 0, 202.0, "u3"],
            ["Carol", 10, 0, 203.0, "u4"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 5: Non-full compact → with forceLookup, LookupMergeTreeCompactRewriter is used.
    // New file in level 4.
    {
        ASSERT_OK_AND_ASSIGN(auto compact_msgs,
                             CompactAndCommit(table_path, {{"f1", "10"}}, 0,
                                              /*full_compaction=*/false, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Non-full compact with forceLookup + DV=false must not produce DV index files";
    }

    // Step 6: ScanAndVerify after non-full compact.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Alice", 10, 0, 101.0, "u1"],
            [0, "Bob",   10, 0, 202.0, "u3"],
            [0, "Carol", 10, 0, 203.0, "u4"],
            [0, "Dave",  10, 0, 4.0, ")" + padding + R"("],
            [0, "Eve",   10, 0, 5.0, ")" + padding + R"("]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }

    // Step 7: Full compact to merge everything into a single L5 file.
    ASSERT_OK_AND_ASSIGN(
        auto final_compact_msgs,
        CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));

    // Step 8: ScanAndVerify after full compact (globally sorted after merge).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Alice", 10, 0, 101.0, "u1"],
            [0, "Bob",   10, 0, 202.0, "u3"],
            [0, "Carol", 10, 0, 203.0, "u4"],
            [0, "Dave",  10, 0, 4.0, ")" + padding + R"("],
            [0, "Eve",   10, 0, 5.0, ")" + padding + R"("]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }
}

TEST_F(PkCompactionInteTest, TestAggregateNoDvWithDropDelete) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "1"},
                                                  {Options::BUCKET_KEY, "f0"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::MERGE_ENGINE, "aggregation"},
                                                  {Options::FIELDS_DEFAULT_AGG_FUNC, "sum"},
                                                  {Options::DELETION_VECTORS_ENABLED, "false"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    // Step 1: Write initial data
    {
        std::string json_data = R"([
            ["Alice", 10, 10,  1.0],
            ["Bob",   10, 20,  2.0],
            ["Carol", 10, 30,  3.0]
        ])";
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 2: Full compact → upgrades L0 file to Lmax.
    {
        ASSERT_OK_AND_ASSIGN(
            auto compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Initial full compact should not produce DV index files";
    }

    // Step 3: Write batch2
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 5,  0.5],
            ["Bob",   10, 10, 1.0]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 4: Non-full compact → L0 merges to an intermediate level.
    // This creates data at an intermediate level while Lmax still has the original data.
    // dropDelete=false here because output_level < NonEmptyHighestLevel.
    {
        ASSERT_OK_AND_ASSIGN(auto compact_msgs,
                             CompactAndCommit(table_path, {{"f1", "10"}}, 0,
                                              /*full_compaction=*/false, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Non-full compact with DV=false must not produce DV index files";
    }

    // Step 5: Write batch3 — mix of INSERT and DELETE rows.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Dave",  10, 40, 4.0],
            ["Alice", 10, 3,  0.1]
        ])")
                         .ValueOrDie();
        std::vector<RecordBatch::RowKind> row_kinds = {RecordBatch::RowKind::INSERT,
                                                       RecordBatch::RowKind::DELETE};
        ASSERT_OK(WriteAndCommitWithRowKinds(table_path, {{"f1", "10"}}, 0, array, row_kinds,
                                             commit_id++));
    }

    // Step 6: Full compact → merges all levels into Lmax.
    // dropDelete=true because output_level == NonEmptyHighestLevel (Lmax).
    {
        ASSERT_OK_AND_ASSIGN(
            auto compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Full compact with aggregation + DV=false must not produce DV index files";
    }

    // Step 7: ScanAndVerify after full compact.
    // All data is aggregated and sorted. DELETE rows are dropped by DropDeleteReader.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Alice", 10, 12,  1.4],
            [0, "Bob",   10, 30,  3.0],
            [0, "Carol", 10, 30,  3.0],
            [0, "Dave",  10, 40,  4.0]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }
}

TEST_F(PkCompactionInteTest, TestAggregateWithNoDvAndOrphanDelete) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "1"},
                                                  {Options::BUCKET_KEY, "f0"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::MERGE_ENGINE, "aggregation"},
                                                  {Options::FIELDS_DEFAULT_AGG_FUNC, "sum"},
                                                  {Options::DELETION_VECTORS_ENABLED, "false"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    // Step 1: Write INSERT rows for Alice and Bob only.
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 100, 10.0],
            ["Bob",   10, 200, 20.0]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 2: Full compact → upgrades L0 to Lmax.
    {
        ASSERT_OK_AND_ASSIGN(
            auto compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs));
    }

    // Step 3: Write a mix of:
    //   - DELETE Alice (partial retract: subtract 30 from f2, 3.0 from f3)
    //   - DELETE Carol (orphan: Carol has no prior INSERT, so this is a retract with no base)
    //   - INSERT Dave (new key, no prior data)
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 30,  3.0],
            ["Carol", 10, 999, 99.9],
            ["Dave",  10, 50,  5.0]
        ])")
                         .ValueOrDie();
        std::vector<RecordBatch::RowKind> row_kinds = {RecordBatch::RowKind::DELETE,
                                                       RecordBatch::RowKind::DELETE,
                                                       RecordBatch::RowKind::INSERT};
        ASSERT_OK(WriteAndCommitWithRowKinds(table_path, {{"f1", "10"}}, 0, array, row_kinds,
                                             commit_id++));
    }

    // Step 4: Full compact → merges all levels into Lmax.
    {
        ASSERT_OK_AND_ASSIGN(auto compact_msgs,
                             CompactAndCommit(table_path, {{"f1", "10"}}, 0,
                                              /*full_compaction=*/false, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Aggregate + DV=false must not produce DV index files";
    }

    // Step 5: ScanAndVerify.
    // Carol must be absent (orphan DELETE dropped). Alice is retracted. Bob and Dave unchanged.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Alice", 10, 70,  7.0],
            [0, "Bob",   10, 200, 20.0],
            [0, "Dave",  10, 50,  5.0]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }
}

TEST_F(PkCompactionInteTest, TestDuplicateWithDvAndOrphanDelete) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64()),
    arrow::field("f4", arrow::utf8())};
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "1"},
                                                  {Options::BUCKET_KEY, "f0"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::MERGE_ENGINE, "deduplicate"},
                                                  {Options::DELETION_VECTORS_ENABLED, "true"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    // A long padding string (~2KB) to inflate the initial file size.
    std::string padding(2048, 'X');

    // Step 1: Write INSERT rows for Alice and Bob only.
    {
        std::string json_data = R"([
            ["Alice", 10, 100, 1.0, ")" +
                                padding + R"("],
            ["Bob",   10, 200, 2.0, ")" +
                                padding + R"("]
        ])";
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++));
    }

    // Step 2: Full compact → upgrades L0 to Lmax (large file).
    {
        ASSERT_OK_AND_ASSIGN(
            auto compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Initial full compact should not produce DV index files";
    }

    // Step 3: Write a mix of DELETE and INSERT
    {
        std::string json_data = R"([
            ["Alice", 10, 30,  3.0, "u1"],
            ["Carol", 10, 999, 99.9, "u2"],
            ["Dave",  10, 50,  5.0, "u3"]
        ])";
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
        std::vector<RecordBatch::RowKind> row_kinds = {RecordBatch::RowKind::DELETE,
                                                       RecordBatch::RowKind::DELETE,
                                                       RecordBatch::RowKind::INSERT};
        ASSERT_OK(WriteAndCommitWithRowKinds(table_path, {{"f1", "10"}}, 0, array, row_kinds,
                                             commit_id++));
    }

    // Step 4: Non-full compact → L0 merges to an intermediate level using LookupRewriter.
    {
        ASSERT_OK_AND_ASSIGN(auto compact_msgs,
                             CompactAndCommit(table_path, {{"f1", "10"}}, 0,
                                              /*full_compaction=*/false, commit_id++));
        ASSERT_TRUE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Non-full compact must produce DV for Alice (has base in Lmax)";
    }

    // Step 5: ScanAndVerify after DV compact.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Bob",   10, 200, 2.0, ")" + padding + R"("],
            [0, "Dave",  10, 50,  5.0, "u3"]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }

    // Step 6: Full compact → merges all levels into Lmax.
    {
        ASSERT_OK_AND_ASSIGN(
            auto final_compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
    }

    // Step 7: ScanAndVerify after full compact (globally sorted).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Bob",   10, 200, 2.0, ")" + padding + R"("],
            [0, "Dave",  10, 50,  5.0, "u3"]
        ])";
        ScanAndVerify(table_path, fields, expected_data);
    }
}

}  // namespace paimon::test
