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
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/commit_context.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/disk/io_manager.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/table/source/table_read.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/data_generator.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

class PkCompactionInteTest : public ::testing::Test,
                             public ::testing::WithParamInterface<std::string> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
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
    void PrepareSimpleKeyValueData(const std::shared_ptr<DataGenerator>& gen, TestHelper* helper,
                                   int64_t* identifier) {
        auto& commit_identifier = *identifier;
        std::vector<BinaryRow> datas_1;
        datas_1.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Alice"), 10, 1, 11.1}, pool_.get()));
        datas_1.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Bob"), 10, 0, 12.1}, pool_.get()));
        datas_1.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Emily"), 10, 0, 13.1}, pool_.get()));
        datas_1.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Tony"), 10, 0, 14.1}, pool_.get()));
        datas_1.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Lucy"), 20, 1, 14.1}, pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto batches_1, gen->SplitArrayByPartitionAndBucket(datas_1));
        ASSERT_EQ(3, batches_1.size());
        ASSERT_OK_AND_ASSIGN(
            auto commit_msgs,
            helper->WriteAndCommit(std::move(batches_1), commit_identifier++, std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
        ASSERT_TRUE(snapshot1);
        ASSERT_EQ(1, snapshot1.value().Id());
        ASSERT_EQ(5, snapshot1.value().TotalRecordCount().value());
        ASSERT_EQ(5, snapshot1.value().DeltaRecordCount().value());

        std::vector<BinaryRow> datas_2;
        datas_2.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Emily"), 10, 0, 15.1}, pool_.get()));
        datas_2.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Bob"), 10, 0, 12.1}, pool_.get()));
        datas_2.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Alex"), 10, 0, 16.1}, pool_.get()));
        datas_2.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Paul"), 20, 1, NullType()}, pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto batches_2, gen->SplitArrayByPartitionAndBucket(datas_2));
        ASSERT_EQ(2, batches_2.size());
        ASSERT_OK_AND_ASSIGN(
            auto commit_msgs_2,
            helper->WriteAndCommit(std::move(batches_2), commit_identifier++, std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot2, helper->LatestSnapshot());
        ASSERT_TRUE(snapshot2);
        ASSERT_EQ(2, snapshot2.value().Id());
        ASSERT_EQ(9, snapshot2.value().TotalRecordCount().value());
        ASSERT_EQ(4, snapshot2.value().DeltaRecordCount().value());

        std::vector<BinaryRow> datas_3;
        datas_3.push_back(
            BinaryRowGenerator::GenerateRow({std::string("David"), 10, 0, 17.1}, pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto batches_3, gen->SplitArrayByPartitionAndBucket(datas_3));
        ASSERT_EQ(1, batches_3.size());
        ASSERT_OK_AND_ASSIGN(
            auto commit_msgs_3,
            helper->WriteAndCommit(std::move(batches_3), commit_identifier++, std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot3, helper->LatestSnapshot());
        ASSERT_TRUE(snapshot3);
        ASSERT_EQ(3, snapshot3.value().Id());
        ASSERT_EQ(10, snapshot3.value().TotalRecordCount().value());
        ASSERT_EQ(1, snapshot3.value().DeltaRecordCount().value());
    }

    Result<std::vector<std::shared_ptr<CommitMessage>>> WriteArray(
        const std::string& table_path, const std::map<std::string, std::string>& partition,
        int32_t bucket, const std::shared_ptr<arrow::Array>& write_array, int64_t commit_identifier,
        bool is_streaming = true, const std::vector<RecordBatch::RowKind>& row_kinds = {},
        bool write_only = true) const {
        auto io_manager = std::shared_ptr<IOManager>(
            IOManager::Create(PathUtil::JoinPath(dir_->Str(), "tmp")).release());
        WriteContextBuilder write_builder(table_path, "commit_user_1");
        write_builder.WithStreamingMode(is_streaming).WithIOManager(io_manager);
        if (write_only) {
            write_builder.AddOption(Options::WRITE_ONLY, "true");
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto file_store_write,
                               FileStoreWrite::Create(std::move(write_context)));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*write_array, &c_array));
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
                          int64_t commit_identifier,
                          const std::vector<RecordBatch::RowKind>& row_kinds = {}) {
        PAIMON_ASSIGN_OR_RAISE(auto commit_msgs,
                               WriteArray(table_path, partition, bucket, write_array,
                                          commit_identifier, /*is_streaming=*/true, row_kinds));
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

        // Group splits by (partition, bucket) to handle multiple splits per bucket.
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

    // Helper: check whether compact commit messages contain new DV index files.
    bool HasDeletionVectorIndexFiles(
        const std::vector<std::shared_ptr<CommitMessage>>& commit_messages) {
        for (const auto& msg : commit_messages) {
            auto impl = dynamic_cast<CommitMessageImpl*>(msg.get());
            EXPECT_TRUE(impl);
            for (const auto& index_file : impl->GetCompactIncrement().NewIndexFiles()) {
                if (index_file->IndexType() == DeletionVectorsIndexFile::DELETION_VECTORS_INDEX) {
                    return true;
                }
            }
        }
        return false;
    }

    // Helper: check whether compact commit messages contain extra lookup files.
    bool HasExtraLookupFiles(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages) {
        for (const auto& msg : commit_messages) {
            auto impl = dynamic_cast<CommitMessageImpl*>(msg.get());
            EXPECT_TRUE(impl);
            for (const auto& file : impl->GetCompactIncrement().CompactAfter()) {
                if (HasExtraLookupFiles(file)) {
                    return true;
                }
            }
        }
        return false;
    }

    bool HasExtraLookupFiles(const std::shared_ptr<DataFileMeta>& file) {
        for (const auto& extra_file : file->extra_files) {
            if (extra_file && StringUtils::EndsWith(extra_file.value(), ".lookup")) {
                return true;
            }
        }
        return false;
    }

    // Helper: build split based on CommitMessage, as we do not support schema evolution in scan.
    std::shared_ptr<Split> BuildSplit(const std::vector<std::shared_ptr<CommitMessage>>& msgs,
                                      int64_t snapshot_id) {
        auto impl = dynamic_cast<CommitMessageImpl*>(msgs[0].get());
        auto file_metas = impl->GetCompactIncrement().CompactAfter();
        EXPECT_EQ(file_metas.size(), 1);
        std::string bucket_path = "fake-bucket-path";
        DataSplitImpl::Builder builder(impl->Partition(), impl->Bucket(), bucket_path,
                                       std::move(file_metas));
        auto split =
            builder.WithSnapshot(snapshot_id).IsStreaming(false).RawConvertible(true).Build();
        EXPECT_TRUE(split.ok()) << split.status().ToString();
        return split.value();
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
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
        [[maybe_unused]] auto upgrade_msgs,
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
        [[maybe_unused]] auto full_compact_msgs,
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
        [[maybe_unused]] auto upgrade_msgs,
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

// Test: deduplicate merge engine with multiple sequence fields whose declaration
// order differs from schema order, PK in the middle, and DV enabled.
//
// Strategy (same DV pattern as other tests):
//   1. Write batch1 (5 keys, large padding) → level-0
//   2. Full compact → upgrade to max level
//   3. Write batch2 (overlap Alice, Bob with higher/lower seq) → level-0
//   4. Write batch3 (overlap Alice, Carol with mixed seq) → level-0
//   5. Non-full compact → merge level-0 files, DV on max-level overlapping rows
//   6. Assert DV index files present
//   7. ScanAndVerify after DV compact
//   8. Full compact
//   9. ScanAndVerify after full compact
TEST_F(PkCompactionInteTest, DeduplicateWithSequenceFieldAndPkInMiddle) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),    // value
        arrow::field("f1", arrow::utf8()),     // PK
        arrow::field("f2", arrow::int32()),    // PK
        arrow::field("s0", arrow::int32()),    // sequence field (declared 2nd)
        arrow::field("s1", arrow::int32()),    // sequence field (declared 1st)
        arrow::field("f3", arrow::float64()),  // value
        arrow::field("f4", arrow::utf8())};    // padding
    std::vector<std::string> primary_keys = {"f1", "f2"};
    std::vector<std::string> partition_keys = {};

    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},  {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, "local"},    {Options::MERGE_ENGINE, "deduplicate"},
        {Options::SEQUENCE_FIELD, "s1,s0"}, {Options::DELETION_VECTORS_ENABLED, "true"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    std::string padding(2048, 'X');

    // Step 1: Write initial data with large padding (creates a big level-0 file).
    // Sequence fields declared as (s1, s0), so comparison order is s1 first, then s0.
    {
        // clang-format off
        std::string json_data = R"([
[100, "Alice", 3, 10, 20, 1.5, ")" + padding + R"("],
[200, "Bob",   5, 30, 10, 2.5, ")" + padding + R"("],
[300, "Carol", 1, 20, 30, 3.5, ")" + padding + R"("],
[400, "Dave",  4, 40, 40, 4.5, ")" + padding + R"("],
[500, "Eve",   2, 50, 50, 5.5, ")" + padding + R"("]
])";
        // clang-format on
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
    }

    // Step 2: Full compact → upgrades level-0 file to max level.
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] auto upgrade_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));

    // Step 3: Write batch2 with overlapping keys (first new level-0 file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            [110, "Alice", 3, 10, 25, 1.1, "a1"],
            [210, "Bob",   5, 25, 10, 2.1, "b1"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
    }

    // Step 4: Write batch3 with overlapping keys (second new level-0 file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            [120, "Alice", 3, 10, 22, 1.2, "a2"],
            [310, "Carol", 1, 20, 25, 3.1, "c1"]
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
    // Scan order: max-level file first (Dave, Eve after DV filters out Alice/Bob/Carol),
    // then intermediate-level file (Alice, Bob, Carol sorted by PK f1 asc, f2 asc).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("", 0)] = R"([
[0, 400, "Dave",  4, 40, 40, 4.5, ")" + padding + R"("],
[0, 500, "Eve",   2, 50, 50, 5.5, ")" + padding + R"("],
[0, 110, "Alice", 3, 10, 25, 1.1, "a1"],
[0, 200, "Bob",   5, 30, 10, 2.5, ")" + padding + R"("],
[0, 300, "Carol", 1, 20, 30, 3.5, ")" + padding + R"("]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }

    // Step 8: Full compact to merge everything.
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] auto full_compact_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));

    // Step 9: ScanAndVerify after full compact (globally sorted by PK: f1 asc, f2 asc).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("", 0)] = R"([
[0, 110, "Alice", 3, 10, 25, 1.1, "a1"],
[0, 200, "Bob",   5, 30, 10, 2.5, ")" + padding + R"("],
[0, 300, "Carol", 1, 20, 30, 3.5, ")" + padding + R"("],
[0, 400, "Dave",  4, 40, 40, 4.5, ")" + padding + R"("],
[0, 500, "Eve",   2, 50, 50, 5.5, ")" + padding + R"("]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }
}

// Test: deduplicate merge engine with nested types (list, struct, map),
// sequence field, and DV enabled.
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
TEST_F(PkCompactionInteTest, DeduplicateNestedTypesWithSequenceField) {
    auto map_type =
        std::make_shared<arrow::MapType>(arrow::field("key", arrow::utf8(), /*nullable=*/false),
                                         arrow::field("value", arrow::int32()));
    arrow::FieldVector fields = {
        arrow::field("pk0", arrow::utf8()),
        arrow::field("pk1", arrow::int32()),
        arrow::field("seq", arrow::int64()),
        arrow::field("list_col", arrow::list(arrow::int32())),
        arrow::field("struct_col", arrow::struct_({arrow::field("a", arrow::utf8()),
                                                   arrow::field("b", arrow::int32())})),
        arrow::field("map_col", map_type),
        arrow::field("padding", arrow::utf8())};
    std::vector<std::string> primary_keys = {"pk0", "pk1"};
    std::vector<std::string> partition_keys = {};

    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"}, {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, "local"},   {Options::MERGE_ENGINE, "deduplicate"},
        {Options::SEQUENCE_FIELD, "seq"},  {Options::DELETION_VECTORS_ENABLED, "true"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    std::string pad(2048, 'P');

    // Step 1: Write initial data with large padding (creates a big level-0 file).
    {
        // clang-format off
        std::string json_data = R"([
["Alice", 1, 100, [1,2,3],   ["hello",10], [["x",1],["y",2]], ")" + pad + R"("],
["Bob",   2, 200, [4,5],     ["world",20], [["a",3]],         ")" + pad + R"("],
["Carol", 3, 300, [6,7,8,9], ["foo",30],   [["m",4],["n",5]], ")" + pad + R"("],
["Dave",  4, 400, [10],      ["bar",40],   [["p",6]],         ")" + pad + R"("],
["Eve",   5, 500, [11,12],   ["baz",50],   [["q",7],["r",8]], ")" + pad + R"("]
])";
        // clang-format on
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
    }

    // Step 2: Full compact → upgrades level-0 file to max level.
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] auto upgrade_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));

    // Step 3: Write batch2 with overlapping keys (first new level-0 file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 1, 150, [10,20],    ["hi",11],   [["z",9]],         "a1"],
            ["Bob",   2, 180, [40,50,60], ["bye",21],  [["b",10],["c",11]], "b1"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
    }

    // Step 4: Write batch3 with overlapping keys (second new level-0 file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 1, 120, [99],       ["mid",12],  [["w",99]],        "a2"],
            ["Carol", 3, 250, [60,70],    ["qux",31],  [["o",12]],        "c1"]
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
    // Scan order: max-level (Dave, Eve after DV), then intermediate (Alice, Bob, Carol).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("", 0)] = R"([
[0, "Dave",  4, 400, [10],      ["bar",40],   [["p",6]],         ")" + pad + R"("],
[0, "Eve",   5, 500, [11,12],   ["baz",50],   [["q",7],["r",8]], ")" + pad + R"("],
[0, "Alice", 1, 150, [10,20],   ["hi",11],    [["z",9]],         "a1"],
[0, "Bob",   2, 200, [4,5],     ["world",20], [["a",3]],         ")" + pad + R"("],
[0, "Carol", 3, 300, [6,7,8,9], ["foo",30],   [["m",4],["n",5]], ")" + pad + R"("]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }

    // Step 8: Full compact to merge everything.
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] auto full_compact_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));

    // Step 9: ScanAndVerify after full compact (globally sorted by PK: pk0 asc, pk1 asc).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("", 0)] = R"([
[0, "Alice", 1, 150, [10,20],   ["hi",11],    [["z",9]],         "a1"],
[0, "Bob",   2, 200, [4,5],     ["world",20], [["a",3]],         ")" + pad + R"("],
[0, "Carol", 3, 300, [6,7,8,9], ["foo",30],   [["m",4],["n",5]], ")" + pad + R"("],
[0, "Dave",  4, 400, [10],      ["bar",40],   [["p",6]],         ")" + pad + R"("],
[0, "Eve",   5, 500, [11,12],   ["baz",50],   [["q",7],["r",8]], ")" + pad + R"("]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }
}

// Test: copy pk_table_with_alter_table (which has schema evolution) to a temp directory,
// write new data using the latest schema (schema-1), trigger full compact on each partition,
// and verify the merged result is correct.
//
// Strategy:
//   1. Copy table to temp dir
//   2. Write new data to partition (1,1) and (0,1) with schema-1 fields
//   3. Full compact partition (1,1)
//   4. Full compact partition (0,1)
//   5. ScanAndVerify all partitions
TEST_F(PkCompactionInteTest, CompactWithSchemaEvolution) {
    // Step 1: Copy pk_table_with_alter_table to temp dir.
    std::string src_table_path = paimon::test::GetDataDir() +
                                 "parquet/pk_table_with_alter_table.db/pk_table_with_alter_table";
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "copied_table");
    ASSERT_TRUE(TestUtil::CopyDirectory(src_table_path, table_path));

    // schema-1 field order (the latest schema):
    //   key1(int), k(string), key_2(int), c(int), d(int,id=8), a(int), key0(int), e(int,id=9)
    arrow::FieldVector fields = {
        arrow::field("key1", arrow::int32()),  arrow::field("k", arrow::utf8()),
        arrow::field("key_2", arrow::int32()), arrow::field("c", arrow::int32()),
        arrow::field("d", arrow::int32()),     arrow::field("a", arrow::int32()),
        arrow::field("key0", arrow::int32()),  arrow::field("e", arrow::int32())};
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 4;  // snapshot-6 has commitIdentifier=3, so start from 4

    // Step 2: Write new data to both partitions.
    // Write to partition (key0=0, key1=1): update Alice's c field.
    // As commit.force-compact = true, write data will force commit.
    std::vector<std::shared_ptr<CommitMessage>> write_msgs_p0;
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            [1, "Alice", 12, 194, 198, 196, 0, 199]
        ])")
                         .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(write_msgs_p0, WriteArray(table_path, {{"key0", "0"}, {"key1", "1"}},
                                                       0, array, commit_id, /*is_streaming=*/true,
                                                       /*row_kinds=*/{}, /*write_only=*/false));
        ASSERT_FALSE(HasExtraLookupFiles(write_msgs_p0));
        ASSERT_OK(Commit(table_path, write_msgs_p0));
        commit_id++;
    }

    // Write to partition (key0=1, key1=1): update Bob and add Frank.
    // As commit.force-compact = true, write data will force commit
    std::vector<std::shared_ptr<CommitMessage>> write_msgs_p1;
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            [1, "Bob",   22, 124, 128, 126, 1, 129],
            [1, "Frank", 92, 904, 908, 906, 1, 909]
        ])")
                         .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(write_msgs_p1, WriteArray(table_path, {{"key0", "1"}, {"key1", "1"}},
                                                       0, array, commit_id, /*is_streaming=*/true,
                                                       /*row_kinds=*/{}, /*write_only=*/false));
        ASSERT_FALSE(HasExtraLookupFiles(write_msgs_p1));
        ASSERT_OK(Commit(table_path, write_msgs_p1));
        commit_id++;
    }

    // Step 5: Read and verify using DataSplits from CompactAfter in write CommitMessages.
    {
        ReadContextBuilder context_builder(table_path);
        context_builder.AddOption(Options::FILE_FORMAT, "parquet");
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

        // Use the latest snapshot id for both splits.
        // The original table has 6 snapshots. Each write with force-compact produces
        // 2 snapshots (append + compact), so 2 writes => 6 + 4 = 10.
        int64_t latest_snapshot_id = 10;
        auto split_p0 = BuildSplit(write_msgs_p0, latest_snapshot_id);
        auto split_p1 = BuildSplit(write_msgs_p1, latest_snapshot_id);

        // Read partition (key0=0, key1=1)
        {
            ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(split_p0));
            ASSERT_OK_AND_ASSIGN(auto result_array,
                                 ReadResultCollector::CollectResult(batch_reader.get()));

            arrow::FieldVector fields_with_row_kind = fields;
            fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                        arrow::field("_VALUE_KIND", arrow::int8()));
            auto result_type = arrow::struct_(fields_with_row_kind);

            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto status = arrow::ipc::internal::json::ChunkedArrayFromJSON(result_type, {R"([
[0, 1, "Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", 2, 4, null, 6, 0, null],
[0, 1, "Alice", 12, 194, 198, 196, 0, 199],
[0, 1, "Paul",  502, 504, 508, 506, 0, 509]
])"},
                                                                           &expected_array);
            ASSERT_TRUE(status.ok());
            ASSERT_TRUE(result_array);
            ASSERT_TRUE(result_array->Equals(*expected_array));
        }

        // Read partition (key0=1, key1=1)
        {
            ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(split_p1));
            ASSERT_OK_AND_ASSIGN(auto result_array,
                                 ReadResultCollector::CollectResult(batch_reader.get()));

            arrow::FieldVector fields_with_row_kind = fields;
            fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                        arrow::field("_VALUE_KIND", arrow::int8()));
            auto result_type = arrow::struct_(fields_with_row_kind);

            std::shared_ptr<arrow::ChunkedArray> expected_array;
            auto status = arrow::ipc::internal::json::ChunkedArrayFromJSON(result_type, {R"([
[0, 1, "Bob",   22, 124, 128, 126, 1, 129],
[0, 1, "Emily", 32, 34, null, 36, 1, null],
[0, 1, "Alex",  52, 514, 518, 516, 1, 519],
[0, 1, "David", 62, 64, null, 66, 1, null],
[0, 1, "Whether I shall turn out to be the hero of my own life.", 72, 74, null, 76, 1, null],
[0, 1, "Frank", 92, 904, 908, 906, 1, 909]
])"},
                                                                           &expected_array);
            ASSERT_TRUE(status.ok());
            ASSERT_TRUE(result_array);
            ASSERT_TRUE(result_array->Equals(*expected_array));
        }
    }
}

// Test: PK table with file-format-per-level and DV.
// Verifies that each level uses the correct file format.
//
// Strategy:
//   1. Write batch1 (large) and commit → level-0 avro file
//   2. Full compact → upgrades to level-5 orc file
//   3. Write batch2 with one overlapping key → level-0 avro file
//   4. Write batch3 with one overlapping key → level-0 avro file
//   5. Non-full compact → two level-0 files merge to level-4 parquet, DV produced
//   6. ScanAndVerify after DV compact
//   7. Full compact → everything merges to level-5 orc
//   8. ScanAndVerify after full compact
TEST_F(PkCompactionInteTest, FileFormatPerLevelWithDV) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64()),
        arrow::field("f4", arrow::utf8())};
    std::vector<std::string> primary_keys = {"f0", "f1", "f2"};
    std::vector<std::string> partition_keys = {"f1"};
    // Add COMPRESSION_NONE for level 5 to produce a large level-5 file,
    // ensuring that the DV index file is added instead of merging level 5 and level 0 into a single
    // file.
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::FILE_FORMAT_PER_LEVEL, "0:avro,5:orc"},
        {Options::FILE_COMPRESSION_PER_LEVEL, "0:zstd,5:none"},
        {"orc.dictionary-key-size-threshold", "0"},
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

    // Helper: verify new files from write messages have expected level and format suffix.
    auto verify_new_files = [](const std::vector<std::shared_ptr<CommitMessage>>& msgs,
                               int32_t expected_level, const std::string& expected_suffix) {
        auto impl = dynamic_cast<CommitMessageImpl*>(msgs[0].get());
        ASSERT_NE(impl, nullptr);
        auto new_files = impl->GetNewFilesIncrement().NewFiles();
        ASSERT_EQ(new_files.size(), 1);
        ASSERT_EQ(new_files[0]->level, expected_level);
        ASSERT_TRUE(StringUtils::EndsWith(new_files[0]->file_name, expected_suffix))
            << "level-" << expected_level << " file should be " << expected_suffix
            << ", got: " << new_files[0]->file_name;
    };

    // Helper: verify compact-after files have expected level and format suffix.
    auto verify_compact_after = [](const std::vector<std::shared_ptr<CommitMessage>>& msgs,
                                   int32_t expected_level, const std::string& expected_suffix) {
        auto impl = dynamic_cast<CommitMessageImpl*>(msgs[0].get());
        ASSERT_NE(impl, nullptr);
        auto compact_after = impl->GetCompactIncrement().CompactAfter();
        ASSERT_EQ(compact_after.size(), 1);
        ASSERT_EQ(compact_after[0]->level, expected_level);
        ASSERT_TRUE(StringUtils::EndsWith(compact_after[0]->file_name, expected_suffix))
            << "level-" << expected_level << " file should be " << expected_suffix
            << ", got: " << compact_after[0]->file_name;
    };

    // Step 1: Write initial data (creates a big level-0 file).
    std::vector<std::shared_ptr<CommitMessage>> write1_msgs;
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
        ASSERT_OK_AND_ASSIGN(write1_msgs,
                             WriteArray(table_path, {{"f1", "10"}}, 0, array, commit_id));
        verify_new_files(write1_msgs, /*expected_level=*/0, ".avro");
        ASSERT_OK(Commit(table_path, write1_msgs));
        commit_id++;
    }

    // Step 2: Full compact → upgrades level-0 avro to level-5 orc.
    {
        ASSERT_OK_AND_ASSIGN(
            auto compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
        verify_compact_after(compact_msgs, /*expected_level=*/5, ".orc");
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "First compact should not produce DV index files";
    }

    // Step 3: Write batch2 with one overlapping key (small level-0 avro file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Alice", 10, 0, 101.0, "u1"]
        ])")
                         .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto write_msgs,
                             WriteArray(table_path, {{"f1", "10"}}, 0, array, commit_id));
        verify_new_files(write_msgs, /*expected_level=*/0, ".avro");
        ASSERT_OK(Commit(table_path, write_msgs));
        commit_id++;
    }

    // Step 4: Write batch3 with one overlapping key (second small level-0 avro file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Bob", 10, 0, 202.0, "u2"]
        ])")
                         .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto write_msgs,
                             WriteArray(table_path, {{"f1", "10"}}, 0, array, commit_id));
        verify_new_files(write_msgs, /*expected_level=*/0, ".avro");
        ASSERT_OK(Commit(table_path, write_msgs));
        commit_id++;
    }

    // Step 5: Non-full compact → merges two level-0 avro files to an intermediate level.
    // The intermediate level (level-4) is not in FILE_FORMAT_PER_LEVEL, so it uses the
    // default FILE_FORMAT (parquet). Lookup against level-5 orc file produces DV.
    {
        ASSERT_OK_AND_ASSIGN(auto compact_msgs,
                             CompactAndCommit(table_path, {{"f1", "10"}}, 0,
                                              /*full_compaction=*/false, commit_id++));

        verify_compact_after(compact_msgs, /*expected_level=*/4, ".parquet");
        ASSERT_TRUE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Non-full compact must produce DV index files for overlapping keys";
    }

    // Step 6: ScanAndVerify after DV compact.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("f1=10/", 0)] = R"([
[0, "Carol", 10, 0, 3.0, ")" + padding + R"("],
[0, "Dave",  10, 0, 4.0, ")" + padding + R"("],
[0, "Eve",   10, 0, 5.0, ")" + padding + R"("],
[0, "Alice", 10, 0, 101.0, "u1"],
[0, "Bob",   10, 0, 202.0, "u2"]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }

    // Step 7: Full compact → merges everything to level-5 orc.
    {
        ASSERT_OK_AND_ASSIGN(
            auto compact_msgs,
            CompactAndCommit(table_path, {{"f1", "10"}}, 0, /*full_compaction=*/true, commit_id++));
        verify_compact_after(compact_msgs, /*expected_level=*/5, ".orc");
    }

    // Step 8: ScanAndVerify after full compact.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("f1=10/", 0)] = R"([
[0, "Alice", 10, 0, 101.0, "u1"],
[0, "Bob",   10, 0, 202.0, "u2"],
[0, "Carol", 10, 0, 3.0, ")" + padding + R"("],
[0, "Dave",  10, 0, 4.0, ")" + padding + R"("],
[0, "Eve",   10, 0, 5.0, ")" + padding + R"("]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }
}

// Test: continuous streaming writes with background compact in a single FileStoreWrite,
// then full compact. Verifies data consistency before and after full compact.
//
// Strategy:
//   1. Create a streaming FileStoreWrite instance.
//   2. Round 1: write 5 rows → wait compact → PrepareCommit → Commit.
//   3. Round 2: write 3 overlapping rows → wait compact → PrepareCommit → Commit.
//   4. Round 3: write 2 new rows → wait compact → PrepareCommit → Commit.
//   5. ScanAndVerify to capture expected data after streaming writes.
//   6. Full compact → ScanAndVerify again, data must be identical.
TEST_F(PkCompactionInteTest, ContinuousWriteWithBackgroundCompact) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32()),
                                 arrow::field("f2", arrow::float64())};
    std::vector<std::string> primary_keys = {"f0"};
    std::vector<std::string> partition_keys = {};
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"}, {Options::BUCKET, "1"}, {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    std::map<std::string, std::string> partition = {};
    int32_t bucket = 0;

    // Step 1-4: Streaming writes with background compact in one FileStoreWrite.
    {
        auto io_manager = std::shared_ptr<IOManager>(
            IOManager::Create(PathUtil::JoinPath(dir_->Str(), "tmp")).release());
        WriteContextBuilder write_builder(table_path, "commit_user_1");
        write_builder.WithStreamingMode(true).WithIOManager(io_manager);
        ASSERT_OK_AND_ASSIGN(auto write_context, write_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));

        auto write_batch = [&](const std::string& json_data) -> Status {
            auto array =
                arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
            ArrowArray c_array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
            auto record_batch = std::make_unique<RecordBatch>(
                partition, bucket, std::vector<RecordBatch::RowKind>(), &c_array);
            return file_store_write->Write(std::move(record_batch));
        };

        auto compact_and_commit = [&](bool full_compaction) -> Status {
            PAIMON_RETURN_NOT_OK(file_store_write->Compact(partition, bucket, full_compaction));
            PAIMON_ASSIGN_OR_RAISE(auto commit_msgs, file_store_write->PrepareCommit(
                                                         /*wait_compaction=*/true, commit_id++));
            return Commit(table_path, commit_msgs);
        };

        // Round 1: initial 5 rows.
        ASSERT_OK(write_batch(R"([
            ["Alice", 1, 1.0],
            ["Bob",   2, 2.0],
            ["Carol", 3, 3.0],
            ["Dave",  4, 4.0],
            ["Eve",   5, 5.0]
        ])"));
        ASSERT_OK(compact_and_commit(/*full_compaction=*/false));

        // Round 2: overwrite 3 existing keys.
        ASSERT_OK(write_batch(R"([
            ["Alice", 1, 101.0],
            ["Bob",   2, 202.0],
            ["Carol", 3, 303.0]
        ])"));
        ASSERT_OK(compact_and_commit(/*full_compaction=*/false));

        // Round 3: add 2 new keys.
        ASSERT_OK(write_batch(R"([
            ["Frank", 6, 6.0],
            ["Grace", 7, 7.0]
        ])"));
        ASSERT_OK(compact_and_commit(/*full_compaction=*/false));

        ASSERT_OK(file_store_write->Close());
    }

    // After deduplication: Alice=101, Bob=202, Carol=303, Dave=4, Eve=5, Frank=6, Grace=7.
    std::map<std::pair<std::string, int32_t>, std::string> expected_data;
    expected_data[std::make_pair("", 0)] = R"([
[0, "Alice", 1, 101.0],
[0, "Bob",   2, 202.0],
[0, "Carol", 3, 303.0],
[0, "Dave",  4, 4.0],
[0, "Eve",   5, 5.0],
[0, "Frank", 6, 6.0],
[0, "Grace", 7, 7.0]
])";

    // Step 5: ScanAndVerify before full compact.
    ScanAndVerify(table_path, fields, expected_data);

    // Step 6: Full compact.
    ASSERT_OK_AND_ASSIGN(
        auto compact_msgs,
        CompactAndCommit(table_path, partition, bucket, /*full_compaction=*/true, commit_id++));

    // Step 7: ScanAndVerify after full compact — data must be identical.
    ScanAndVerify(table_path, fields, expected_data);
}

// Test: PK + DV deduplicate with row kinds (DELETE, UPDATE_BEFORE, UPDATE_AFTER).
// Verifies that deleted rows are truly removed and not returned by scan/read.
//
// Strategy:
//   1. Write 5 rows with large padding (all INSERT) → level-0 file.
//   2. Full compact → upgrades to max level (large file).
//   3. Write UPDATE_BEFORE + UPDATE_AFTER for "Bob", DELETE for "Carol" (small level-0 file).
//   4. Non-full compact → lookup against max-level file produces DV.
//   5. ScanAndVerify: "Carol" must be gone, "Bob" must have updated value.
//   6. Full compact → ScanAndVerify again, data must be identical.
TEST_F(PkCompactionInteTest, DeduplicateWithRowKindAndDV) {
    // f3 is a large padding field to make the initial file substantially bigger.
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::float64()), arrow::field("f3", arrow::utf8())};
    std::vector<std::string> primary_keys = {"f0"};
    std::vector<std::string> partition_keys = {};
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "1"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::DELETION_VECTORS_ENABLED, "true"}};
    CreateTable(fields, partition_keys, primary_keys, options);
    std::string table_path = TablePath();
    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    std::map<std::string, std::string> partition = {};
    int32_t bucket = 0;

    // A long padding string (~2KB) to inflate the initial file size.
    std::string padding(2048, 'X');

    // Step 1: Write 5 rows with large padding (creates a big level-0 file).
    {
        // clang-format off
        std::string json_data = R"([
["Alice", 1, 1.0, ")" + padding + R"("],
["Bob",   2, 2.0, ")" + padding + R"("],
["Carol", 3, 3.0, ")" + padding + R"("],
["Dave",  4, 4.0, ")" + padding + R"("],
["Eve",   5, 5.0, ")" + padding + R"("]
])";
        // clang-format on
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, partition, bucket, array, commit_id++));
    }

    // Step 2: Full compact → upgrades level-0 file to max level (large file).
    {
        ASSERT_OK_AND_ASSIGN(
            auto compact_msgs,
            CompactAndCommit(table_path, partition, bucket, /*full_compaction=*/true, commit_id++));
        ASSERT_FALSE(HasDeletionVectorIndexFiles(compact_msgs))
            << "First compact should not produce DV index files";
    }

    // Step 3: Write UPDATE_BEFORE + UPDATE_AFTER for "Bob", DELETE for "Carol" (small file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["Bob",   2, 2.0,   "u1"],
            ["Bob",   2, 202.0, "u2"],
            ["Carol", 3, 3.0,   "d1"]
        ])")
                         .ValueOrDie();
        std::vector<RecordBatch::RowKind> row_kinds = {RecordBatch::RowKind::UPDATE_BEFORE,
                                                       RecordBatch::RowKind::UPDATE_AFTER,
                                                       RecordBatch::RowKind::DELETE};
        ASSERT_OK(WriteAndCommit(table_path, partition, bucket, array, commit_id++, row_kinds));
    }

    // Step 4: Non-full compact → lookup against max-level file produces DV.
    {
        ASSERT_OK_AND_ASSIGN(auto compact_msgs,
                             CompactAndCommit(table_path, partition, bucket,
                                              /*full_compaction=*/false, commit_id++));
        ASSERT_TRUE(HasDeletionVectorIndexFiles(compact_msgs))
            << "Non-full compact must produce DV index files for overlapping keys";
    }

    // Step 5: ScanAndVerify after DV compact — "Carol" must be gone, "Bob" updated.
    // Data order depends on file layout: max-level file (Alice, Dave, Eve) + level-4 (Bob).
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("", 0)] = R"([
[0, "Alice", 1, 1.0, ")" + padding + R"("],
[0, "Dave",  4, 4.0, ")" + padding + R"("],
[0, "Eve",   5, 5.0, ")" + padding + R"("],
[0, "Bob",   2, 202.0, "u2"]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }

    // Step 6: Full compact → merges everything into one sorted file.
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] auto full_compact_msgs,
        CompactAndCommit(table_path, partition, bucket, /*full_compaction=*/true, commit_id++));

    // Step 7: ScanAndVerify after full compact — all data in one sorted file.
    {
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
        // clang-format off
        expected_data[std::make_pair("", 0)] = R"([
[0, "Alice", 1, 1.0, ")" + padding + R"("],
[0, "Bob",   2, 202.0, "u2"],
[0, "Dave",  4, 4.0, ")" + padding + R"("],
[0, "Eve",   5, 5.0, ")" + padding + R"("]
])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }
}

// Test: write and compact on branch.
// Copies append_table_with_rt_branch.db to a temp dir, writes data to branch-rt, then compacts.
// Since we don't support branch commit and scan, we only verify the commit messages.
TEST_F(PkCompactionInteTest, WriteAndCompactWithBranch) {
    // Step 1: Copy the table with branch to a temp directory.
    std::string src_table_path =
        paimon::test::GetDataDir() +
        "parquet/append_table_with_rt_branch.db/append_table_with_rt_branch";
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "branch_table");
    ASSERT_TRUE(TestUtil::CopyDirectory(src_table_path, table_path));

    arrow::FieldVector fields = {arrow::field("dt", arrow::utf8()),
                                 arrow::field("name", arrow::utf8()),
                                 arrow::field("amount", arrow::int32())};
    auto data_type = arrow::struct_(fields);

    // Step 2: Write data to the branch-rt and compact.
    auto io_manager = std::shared_ptr<IOManager>(
        IOManager::Create(PathUtil::JoinPath(dir_->Str(), "tmp")).release());
    WriteContextBuilder write_builder(table_path, "commit_user_1");
    write_builder.WithStreamingMode(true).WithIOManager(io_manager).WithBranch("rt");
    ASSERT_OK_AND_ASSIGN(auto write_context, write_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    // dt=20240726, bucket-0 only has ["20240726", "cherry", 3] one row
    auto write_array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["20240726", "cherry", 30],
            ["20240726", "grape", 40]
        ])")
                           .ValueOrDie();
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*write_array, &c_array).ok());
    auto record_batch =
        std::make_unique<RecordBatch>(std::map<std::string, std::string>{{"dt", "20240726"}}, 0,
                                      std::vector<RecordBatch::RowKind>(), &c_array);
    ASSERT_OK(file_store_write->Write(std::move(record_batch)));

    // Compact branch-rt
    ASSERT_OK(file_store_write->Compact({{"dt", "20240726"}}, 0, /*full_compaction=*/true));

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, file_store_write->PrepareCommit(
                                               /*wait_compaction=*/true, /*commit_identifier=*/10));
    ASSERT_OK(file_store_write->Close());

    // Step 3: Verify commit messages.
    ASSERT_EQ(commit_msgs.size(), 1);
    auto impl = dynamic_cast<CommitMessageImpl*>(commit_msgs[0].get());
    ASSERT_NE(impl, nullptr);

    auto new_files = impl->GetNewFilesIncrement().NewFiles();
    auto compact_before = impl->GetCompactIncrement().CompactBefore();
    auto compact_after = impl->GetCompactIncrement().CompactAfter();

    // Write produced 1 new file at level-0 with 2 rows
    ASSERT_EQ(new_files.size(), 1);
    ASSERT_EQ(new_files[0]->level, 0);
    ASSERT_EQ(new_files[0]->row_count, 2);

    // Compact merged 2 level-0 files (1 existing + 1 newly written)
    ASSERT_EQ(compact_before.size(), 2);
    ASSERT_EQ(compact_before[0]->level, 0);
    ASSERT_EQ(compact_before[1]->level, 0);

    // Compact produced 1 level-5 file.
    // The compact_after file has only 2 rows because the original branch-rt
    // partition (dt=20240726, bucket-0) contains only 1 row ("cherry", amount=3),
    // and we wrote 2 new rows ("cherry" + "grape"). Full compact merges the
    // 2 level-0 files into 1 level-5 file with all rows combined.
    ASSERT_EQ(compact_after.size(), 1);
    ASSERT_EQ(compact_after[0]->level, 5);
    ASSERT_EQ(compact_after[0]->row_count, 2);

    // Step 4: Fake a DataSplit from compact_after to read and verify the compacted data.
    {
        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.WithBranch("rt");
        ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

        // Build a fake DataSplit using the compact_after file metadata.
        auto fake_split = BuildSplit(commit_msgs, /*snapshot_id=*/1);
        ASSERT_OK_AND_ASSIGN(auto batch_reader,
                             table_read->CreateReader(std::shared_ptr<Split>(fake_split)));
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::CollectResult(batch_reader.get()));

        arrow::FieldVector fields_with_row_kind = fields;
        fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                    arrow::field("_VALUE_KIND", arrow::int8()));
        auto result_type = arrow::struct_(fields_with_row_kind);

        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto status = arrow::ipc::internal::json::ChunkedArrayFromJSON(result_type, {R"([
[0, "20240726", "cherry", 30],
[0, "20240726", "grape", 40]
])"},
                                                                       &expected_array);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(result_array);
        ASSERT_TRUE(result_array->Equals(*expected_array));
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
        // clang-format off
        std::string json_data = R"([
            ["Alice", 10, 10,   1.0, ")" + padding + R"("],
            ["Bob",   10, 20,   2.0, ")" + padding + R"("],
            ["Carol", 10, 30,   3.0, ")" + padding + R"("],
            ["Dave",  10, 40,   4.0, ")" + padding + R"("],
            ["Eve",   10, 50,   5.0, ")" + padding + R"("]
        ])";
        // clang-format on
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
        // clang-format off
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Dave",  10, 40,   4.0, ")" + padding + R"("],
            [0, "Eve",   10, 50,   5.0, ")" + padding + R"("],
            [0, "Alice", 10, 99,   1.0, "u1"],
            [0, "Bob",   10, 88,   22.0, "u3"],
            [0, "Carol", 10, 30,   33.0, "u4"]
        ])";
        // clang-format on
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
        // clang-format off
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Alice", 10, 99,   1.0, "u1"],
            [0, "Bob",   10, 88,   22.0, "u3"],
            [0, "Carol", 10, 30,   33.0, "u4"],
            [0, "Dave",  10, 40,   4.0, ")" + padding +
                                                     R"("],
            [0, "Eve",   10, 50,   5.0, ")" + padding +
                                                     R"("]
        ])";
        // clang-format on
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
        // clang-format off
        std::map<std::pair<std::string, int32_t>, std::string> expected_data;
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
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++, row_kinds));
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
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++, row_kinds));
    }

    // Step 4: Non full compact → merges all levels into Lmax.
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
        // clang-format off
        std::string json_data = R"([
            ["Alice", 10, 100, 1.0, ")" + padding + R"("],
            ["Bob",   10, 200, 2.0, ")" + padding + R"("]
        ])";
        // clang-format on
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
        ASSERT_OK(WriteAndCommit(table_path, {{"f1", "10"}}, 0, array, commit_id++, row_kinds));
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
        // clang-format off
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Bob",   10, 200, 2.0, ")" + padding + R"("],
            [0, "Dave",  10, 50,  5.0, "u3"]
        ])";
        // clang-format on
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
        // clang-format off
        expected_data[std::make_pair("f1=10/", 0)] = R"([
            [0, "Bob",   10, 200, 2.0, ")" + padding + R"("],
            [0, "Dave",  10, 50,  5.0, "u3"]
        ])";
        // clang-format on
        ScanAndVerify(table_path, fields, expected_data);
    }
}

TEST_F(PkCompactionInteTest, TestKeyValueTableCompactionWithIOException) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);

    std::vector<std::string> primary_keys = {"f0", "f1", "f2"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f2"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::DELETION_VECTORS_ENABLED, "false"}};

    bool compaction_run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 600; ++i) {
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);

        ASSERT_OK_AND_ASSIGN(auto helper,
                             TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys,
                                                options, /*is_streaming_mode=*/true));
        ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                             helper->LatestSchema());
        ASSERT_TRUE(table_schema);

        auto gen = std::make_shared<DataGenerator>(table_schema.value(), pool_);
        int64_t commit_identifier = 0;
        PrepareSimpleKeyValueData(gen, helper.get(), &commit_identifier);

        std::vector<BinaryRow> data;
        data.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Lily"), 10, 0, 17.1}, pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto batches, gen->SplitArrayByPartitionAndBucket(data));
        ASSERT_EQ(1, batches.size());

        ASSERT_OK_AND_ASSIGN(
            auto helper2,
            TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                               /*is_streaming_mode=*/true, /*ignore_if_exists=*/true));

        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);

        CHECK_HOOK_STATUS(helper2->write_->Write(std::move(batches[0])), i);
        CHECK_HOOK_STATUS(helper2->write_->Compact(/*partition=*/{{"f1", "10"}}, /*bucket=*/1,
                                                   /*full_compaction=*/true),
                          i);

        Result<std::vector<std::shared_ptr<CommitMessage>>> commit_messages =
            helper2->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier);
        CHECK_HOOK_STATUS(commit_messages.status(), i);
        CHECK_HOOK_STATUS(helper2->commit_->Commit(commit_messages.value(), commit_identifier), i);

        compaction_run_complete = true;
        io_hook->Clear();

        ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> latest_snapshot, helper2->LatestSnapshot());
        ASSERT_TRUE(latest_snapshot);
        ASSERT_EQ(Snapshot::CommitKind::Compact(), latest_snapshot->GetCommitKind());
        break;
    }

    ASSERT_TRUE(compaction_run_complete);
}

TEST_P(PkCompactionInteTest, TestKeyValueTableStreamWriteFullCompaction) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);

    std::vector<std::string> primary_keys = {"f0", "f1", "f2"};
    std::vector<std::string> partition_keys = {"f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, file_format},
                                                  {Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f2"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::DELETION_VECTORS_ENABLED, "false"}};

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir_->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);
    auto gen = std::make_shared<DataGenerator>(table_schema.value(), pool_);
    int64_t commit_identifier = 0;
    PrepareSimpleKeyValueData(gen, helper.get(), &commit_identifier);
    std::vector<BinaryRow> datas_4;
    datas_4.push_back(
        BinaryRowGenerator::GenerateRow({std::string("Lily"), 10, 0, 17.1}, pool_.get()));
    ASSERT_OK_AND_ASSIGN(auto batches_4, gen->SplitArrayByPartitionAndBucket(datas_4));
    ASSERT_EQ(1, batches_4.size());

    ASSERT_OK(helper->write_->Write(std::move(batches_4[0])));
    ASSERT_OK(helper->write_->Compact(/*partition=*/{{"f1", "10"}}, /*bucket=*/1,
                                      /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> commit_messages,
        helper->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
    ASSERT_OK(helper->commit_->Commit(commit_messages, commit_identifier));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot5, helper->LatestSnapshot());
    ASSERT_EQ(5, snapshot5.value().Id());
    ASSERT_EQ(9, snapshot5.value().TotalRecordCount().value());
    ASSERT_EQ(-2, snapshot5.value().DeltaRecordCount().value());
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot5.value().GetCommitKind());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits.size(), 3);
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("f1=10/", 0)] = R"([
[0, "Alice", 10, 1, 11.1]
])";

    expected_datas[std::make_pair("f1=10/", 1)] = R"([
[0, "Alex", 10, 0, 16.1],
[0, "Bob", 10, 0, 12.1],
[0, "David", 10, 0, 17.1],
[0, "Emily", 10, 0, 15.1],
[0, "Lily", 10, 0, 17.1],
[0, "Tony", 10, 0, 14.1]
])";

    expected_datas[std::make_pair("f1=20/", 0)] = R"([
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, null]
])";

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    for (const auto& split : data_splits) {
        auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
        ASSERT_OK_AND_ASSIGN(std::string partition_str,
                             helper->PartitionStr(split_impl->Partition()));
        auto iter = expected_datas.find(std::make_pair(partition_str, split_impl->Bucket()));
        ASSERT_TRUE(iter != expected_datas.end());
        ASSERT_OK_AND_ASSIGN(bool success,
                             helper->ReadAndCheckResult(data_type, {split}, iter->second));
        ASSERT_TRUE(success);
    }
}

// Test: PK table with aggregation merge engine (min), Enable DV, enable remote file.
//
// Strategy (same DV pattern as other tests):
//   1. Write batch1 (5 keys, large padding) → level-0
//   2. Full compact → upgrade to max level → generate .lookup file
//   3. Write batch2 (overlap Alice, Bob) → level-0
//   4. Write batch3 (overlap Alice, Carol) → level-0
//   5. Non-full compact → merge level-0 files, DV on max-level overlapping rows, use .lookup file
//   6. Assert DV index files present
//   7. ScanAndVerify after DV compact
//   8. Full compact
//   9. ScanAndVerify after full compact
TEST_F(PkCompactionInteTest, RemoteLookupFileWithExternalPath) {
    // f4 is a large padding field to inflate the initial file size for DV strategy.
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),    // value field (min agg)
        arrow::field("f1", arrow::utf8()),     // PK (schema index 1, pk index 1)
        arrow::field("f2", arrow::int32()),    // PK (schema index 2, pk index 0)
        arrow::field("f3", arrow::float64()),  // value field (min agg)
        arrow::field("f4", arrow::utf8())};    // padding value field (min agg)
    std::vector<std::string> primary_keys = {"f2", "f1"};
    std::vector<std::string> partition_keys = {};

    // Create external path directories.
    auto external_dir = UniqueTestDirectory::Create("local");
    ASSERT_TRUE(external_dir);
    std::string external_path = external_dir->Str();

    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "1"},
        {Options::FILE_SYSTEM, "local"},
        {Options::MERGE_ENGINE, "aggregation"},
        {Options::FIELDS_DEFAULT_AGG_FUNC, "min"},
        {Options::DELETION_VECTORS_ENABLED, "true"},
        {Options::LOOKUP_REMOTE_FILE_ENABLED, "true"},
        {Options::LOOKUP_REMOTE_LEVEL_THRESHOLD, "1"},
        {Options::DATA_FILE_EXTERNAL_PATHS, "FILE://" + external_path},
        {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"}};
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
        [[maybe_unused]] auto upgrade_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));
    ASSERT_TRUE(HasExtraLookupFiles(upgrade_msgs));

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
    ASSERT_TRUE(HasExtraLookupFiles(dv_compact_msgs));

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
    ASSERT_TRUE(HasExtraLookupFiles(full_compact_msgs));

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

// Test: PK table with aggregation merge engine (min), Enable DV, enable remote file.
//
// Strategy (same DV pattern as other tests):
//   1. Write batch1 (5 keys, large padding) → level-0
//   2. Full compact → upgrade to max level → generate .lookup file
//   3. Alter table, write schema-1
//   4. Write batch2 (overlap Alice, Bob) → level-0
//   5. Write batch3 (overlap Alice, Carol) → level-0
//   6. Non-full compact → merge level-0 files, DV on max-level overlapping rows, not use .lookup
//   file
//   7. Assert DV index files present
//   8. ScanAndVerify after DV compact
//   9. Full compact
//   10. ScanAndVerify after full compact
TEST_F(PkCompactionInteTest, RemoteLookupFileWithSchemaEvolution) {
    // f4 is a large padding field to inflate the initial file size for DV strategy.
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),    // value field (min agg)
        arrow::field("f1", arrow::utf8()),     // PK (schema index 1, pk index 1)
        arrow::field("f2", arrow::int32()),    // PK (schema index 2, pk index 0)
        arrow::field("f3", arrow::float64()),  // value field (min agg)
        arrow::field("f4", arrow::utf8())};    // padding value field (min agg)
    std::vector<std::string> primary_keys = {"f2", "f1"};
    std::vector<std::string> partition_keys = {};

    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "1"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::MERGE_ENGINE, "aggregation"},
                                                  {Options::FIELDS_DEFAULT_AGG_FUNC, "min"},
                                                  {Options::DELETION_VECTORS_ENABLED, "true"},
                                                  {Options::LOOKUP_REMOTE_FILE_ENABLED, "true"},
                                                  {Options::LOOKUP_REMOTE_LEVEL_THRESHOLD, "1"}};
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
        [[maybe_unused]] auto upgrade_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));
    ASSERT_TRUE(HasExtraLookupFiles(upgrade_msgs));

    // Step 3: Alter table
    auto fs = dir_->GetFileSystem();
    auto schema_manager = std::make_shared<SchemaManager>(fs, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema0, schema_manager->ReadSchema(0));
    auto table_schema1 = std::make_shared<TableSchema>(*table_schema0);
    std::vector<DataField> new_fields = {
        DataField(3, arrow::field("f3", arrow::utf8())),   // value field (min agg), double->string
        DataField(0, arrow::field("f0", arrow::utf8())),   // value field (min agg), int32->string
        DataField(1, arrow::field("f1", arrow::utf8())),   // PK (schema index 1, pk index 1)
        DataField(2, arrow::field("f2", arrow::int32())),  // PK (schema index 2, pk index 0)
        DataField(4, arrow::field("f4", arrow::utf8()))};  // padding value field (min agg)

    table_schema1->id_ = 1;
    table_schema1->fields_ = new_fields;
    ASSERT_OK_AND_ASSIGN(std::string schema_content, table_schema1->ToJsonString());
    ASSERT_OK(fs->AtomicStore(schema_manager->ToSchemaPath(1), schema_content));
    data_type = DataField::ConvertDataFieldsToArrowStructType(new_fields);

    // Step 4: Write batch2 with overlapping keys (first new level-0 file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["0.5", "50",  "Alice", 3, "a1"],
            ["3.5", "300", "Bob",   5, "b1"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
    }

    // Step 5: Write batch3 with overlapping keys (second new level-0 file).
    {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            ["1.0", "80",  "Alice", 3, "a2"],
            ["1.5", "150", "Carol", 1, "c1"]
        ])")
                         .ValueOrDie();
        ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
    }

    // Step 6: Non-full compact → two level-0 files merge, lookup against max-level produces DV.
    ASSERT_OK_AND_ASSIGN(
        auto dv_compact_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/false, commit_id++));

    // Step 7: Assert DV index files are present.
    ASSERT_TRUE(HasDeletionVectorIndexFiles(dv_compact_msgs))
        << "Non-full compact should produce DV index files";
    ASSERT_TRUE(HasExtraLookupFiles(dv_compact_msgs));

    // Step 8: Fake a DataSplit from compact_after to read and verify the compacted data.
    std::vector<DataField> fields_with_row_kind = new_fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(), SpecialFields::ValueKind());
    auto data_type_with_row_kind =
        DataField::ConvertDataFieldsToArrowStructType(fields_with_row_kind);
    {
        auto split = BuildSplit(dv_compact_msgs, /*snapshot_id=*/4);
        ASSERT_OK_AND_ASSIGN(auto helper,
                             TestHelper::Create(table_path, options, /*is_streaming_mode=*/false));
        // clang-format off
        std::string expected_data = R"([
[0, "1.5", "150", "Carol", 1, ")" + padding + R"("],
[0, "0.5", "100", "Alice", 3, ")" + padding + R"("],
[0, "2.5", "200", "Bob",   5, ")" + padding + R"("]
])";
        // clang-format on
        ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(data_type_with_row_kind,
                                                                      {split}, expected_data));
        ASSERT_TRUE(success);
    }

    // Step 9: Full compact to merge everything.
    ASSERT_OK_AND_ASSIGN(
        auto full_compact_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));
    ASSERT_TRUE(HasExtraLookupFiles(full_compact_msgs));

    // Step 10: ScanAndVerify after full compact.
    {
        auto split = BuildSplit(full_compact_msgs, /*snapshot_id=*/5);
        ASSERT_OK_AND_ASSIGN(auto helper,
                             TestHelper::Create(table_path, options, /*is_streaming_mode=*/false));
        // clang-format off
        std::string expected_data = R"([
[0, "1.5", "150", "Carol", 1, ")" + padding + R"("],
[0, "5.5", "500", "Eve",   2, ")" + padding + R"("],
[0, "0.5", "100", "Alice", 3, ")" + padding + R"("],
[0, "4.5", "400", "Dave",  4, ")" + padding + R"("],
[0, "2.5", "200", "Bob",   5, ")" + padding + R"("]
])";
        // clang-format on
        ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(data_type_with_row_kind,
                                                                      {split}, expected_data));
        ASSERT_TRUE(success);
    }
}

// Test: Compatibility for lookup file generated by java.
//
// Strategy (same DV pattern as other tests):
//   1. Copy data from existing table generated by java which contains a level-5 (with lookup file)
//   and two level-0.
//   2. Non-full compact → merge level-0 files, DV on max-level overlapping rows, use .lookup file
//   3. Assert DV index files present
//   4. ScanAndVerify after DV compact
//   5. Full compact
//   6. ScanAndVerify after full compact
TEST_P(PkCompactionInteTest, TestLookupCompatibility) {
    auto file_format = GetParam();
    if (file_format == "lance" || file_format == "avro") {
        return;
    }
    // Step 1: Copy pk_compact_lookup table to temp dir.
    std::string src_table_path =
        paimon::test::GetDataDir() + "/" + file_format + "/pk_compact_lookup.db/pk_compact_lookup/";
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "copied_table");
    ASSERT_TRUE(TestUtil::CopyDirectory(src_table_path, table_path));
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),    // value field (min agg)
        arrow::field("f1", arrow::utf8()),     // PK (schema index 1, pk index 1)
        arrow::field("f2", arrow::int32()),    // PK (schema index 2, pk index 0)
        arrow::field("f3", arrow::float64()),  // value field (min agg)
        arrow::field("f4", arrow::utf8())};    // padding value field (min agg)

    int64_t commit_id = 6;
    // Step 2: Non-full compact → two level-0 files merge, lookup against max-level produces DV.
    ASSERT_OK_AND_ASSIGN(
        auto dv_compact_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/false, commit_id++));

    // Step 2: Assert DV index files are present.
    ASSERT_TRUE(HasDeletionVectorIndexFiles(dv_compact_msgs))
        << "Non-full compact should produce DV index files";
    ASSERT_TRUE(HasExtraLookupFiles(dv_compact_msgs));

    // Step 3: ScanAndVerify after DV compact.
    std::string padding(2048, 'X');
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

    // Step 4: Full compact to merge everything.
    ASSERT_OK_AND_ASSIGN(
        auto full_compact_msgs,
        CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));
    ASSERT_TRUE(HasExtraLookupFiles(full_compact_msgs));

    // Step 5: ScanAndVerify after full compact.
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

TEST_F(PkCompactionInteTest, PkDvAndAggWithIOException) {
    // f4 is a large padding field to inflate the initial file size for DV strategy.
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),    // value field (min agg)
        arrow::field("f1", arrow::utf8()),     // PK (schema index 1, pk index 1)
        arrow::field("f2", arrow::int32()),    // PK (schema index 2, pk index 0)
        arrow::field("f3", arrow::float64()),  // value field (min agg)
        arrow::field("f4", arrow::utf8())};    // padding value field (min agg)
    std::vector<std::string> primary_keys = {"f2", "f1"};
    std::vector<std::string> partition_keys = {};

    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "1"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::MERGE_ENGINE, "aggregation"},
                                                  {Options::FIELDS_DEFAULT_AGG_FUNC, "min"},
                                                  {Options::DELETION_VECTORS_ENABLED, "true"},
                                                  {Options::LOOKUP_REMOTE_FILE_ENABLED, "true"},
                                                  {Options::LOOKUP_REMOTE_LEVEL_THRESHOLD, "1"}};

    auto data_type = arrow::struct_(fields);
    int64_t commit_id = 0;

    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 800; i += RandomNumber(1, 23)) {
        dir_ = UniqueTestDirectory::Create("local");
        std::string table_path = TablePath();
        CreateTable(fields, partition_keys, primary_keys, options);

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
            auto array =
                arrow::ipc::internal::json::ArrayFromJSON(data_type, json_data).ValueOrDie();
            ASSERT_OK(WriteAndCommit(table_path, {}, 0, array, commit_id++));
        }

        // Step 2: Full compact → upgrades level-0 file to max level.
        ASSERT_OK_AND_ASSIGN(
            auto upgrade_msgs,
            CompactAndCommit(table_path, {}, 0, /*full_compaction=*/true, commit_id++));
        ASSERT_TRUE(HasExtraLookupFiles(upgrade_msgs));
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
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        auto dv_compact_msgs =
            CompactAndCommit(table_path, {}, 0, /*full_compaction=*/false, commit_id++);
        CHECK_HOOK_STATUS(dv_compact_msgs.status(), i);
        io_hook->Clear();

        // Step 6: Assert DV index files are present.
        ASSERT_TRUE(HasDeletionVectorIndexFiles(dv_compact_msgs.value()))
            << "Non-full compact should produce DV index files";
        ASSERT_TRUE(HasExtraLookupFiles(dv_compact_msgs.value()));

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
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

std::vector<std::string> GetTestValuesForCompactionInteTest() {
    std::vector<std::string> values;
    values.emplace_back("parquet");
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back("orc");
#endif
#ifdef PAIMON_ENABLE_LANCE
    values.emplace_back("lance");
#endif
#ifdef PAIMON_ENABLE_AVRO
    values.emplace_back("avro");
#endif
    return values;
}

INSTANTIATE_TEST_SUITE_P(FileFormat, PkCompactionInteTest,
                         ::testing::ValuesIn(GetTestValuesForCompactionInteTest()));

}  // namespace paimon::test
