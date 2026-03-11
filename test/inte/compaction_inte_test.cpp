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
#include <vector>

#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/append/bucketed_append_compact_manager.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/append_only_file_store_write.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/executor.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/result.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/data_generator.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

class CompactionInteTest : public testing::Test, public ::testing::WithParamInterface<std::string> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void PrepareSimpleAppendData(const std::shared_ptr<DataGenerator>& gen, TestHelper* helper,
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

 private:
    std::shared_ptr<MemoryPool> pool_;
};

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

INSTANTIATE_TEST_SUITE_P(FileFormat, CompactionInteTest,
                         ::testing::ValuesIn(GetTestValuesForCompactionInteTest()));

TEST_P(CompactionInteTest, TestAppendTableStreamWriteFullCompaction) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);

    std::vector<std::string> primary_keys = {};
    std::vector<std::string> partition_keys = {"f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
        {Options::FILE_SYSTEM, "local"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);
    auto gen = std::make_shared<DataGenerator>(table_schema.value(), pool_);
    int64_t commit_identifier = 0;
    PrepareSimpleAppendData(gen, helper.get(), &commit_identifier);
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
    ASSERT_EQ(11, snapshot5.value().TotalRecordCount().value());
    ASSERT_EQ(0, snapshot5.value().DeltaRecordCount().value());
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot5.value().GetCommitKind());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits.size(), 3);
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("f1=10/", 0)] = R"([
[0, "Alice", 10, 1, 11.1]
])";

    expected_datas[std::make_pair("f1=10/", 1)] = R"([
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Emily", 10, 0, 15.1],
[0, "Bob", 10, 0, 12.1],
[0, "Alex", 10, 0, 16.1],
[0, "David", 10, 0, 17.1],
[0, "Lily", 10, 0, 17.1]
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

TEST_P(CompactionInteTest, TestAppendTableStreamWriteBestEffortCompaction) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);

    std::vector<std::string> primary_keys = {};
    std::vector<std::string> partition_keys = {"f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
        {Options::FILE_SYSTEM, "local"},
        {Options::COMPACTION_MIN_FILE_NUM, "3"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);
    auto gen = std::make_shared<DataGenerator>(table_schema.value(), pool_);
    int64_t commit_identifier = 0;
    PrepareSimpleAppendData(gen, helper.get(), &commit_identifier);
    std::vector<BinaryRow> datas_4;
    datas_4.push_back(
        BinaryRowGenerator::GenerateRow({std::string("Lily"), 10, 0, 17.1}, pool_.get()));
    ASSERT_OK_AND_ASSIGN(auto batches_4, gen->SplitArrayByPartitionAndBucket(datas_4));
    ASSERT_EQ(1, batches_4.size());

    ASSERT_OK(helper->write_->Write(std::move(batches_4[0])));
    ASSERT_OK(helper->write_->Compact(/*partition=*/{{"f1", "10"}}, /*bucket=*/1,
                                      /*full_compaction=*/false));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> commit_messages,
        helper->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
    ASSERT_OK(helper->commit_->Commit(commit_messages, commit_identifier));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot5, helper->LatestSnapshot());
    ASSERT_EQ(5, snapshot5.value().Id());
    ASSERT_EQ(11, snapshot5.value().TotalRecordCount().value());
    ASSERT_EQ(0, snapshot5.value().DeltaRecordCount().value());
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot5.value().GetCommitKind());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits.size(), 3);
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("f1=10/", 0)] = R"([
[0, "Alice", 10, 1, 11.1]
])";

    expected_datas[std::make_pair("f1=10/", 1)] = R"([
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Emily", 10, 0, 15.1],
[0, "Bob", 10, 0, 12.1],
[0, "Alex", 10, 0, 16.1],
[0, "David", 10, 0, 17.1],
[0, "Lily", 10, 0, 17.1]
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

TEST_P(CompactionInteTest, TestAppendTableStreamWriteCompactionWithExternalPath) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto external_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(external_dir);
    std::string external_test_dir = "FILE://" + external_dir->Str();

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);

    std::vector<std::string> primary_keys = {};
    std::vector<std::string> partition_keys = {"f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
        {Options::FILE_SYSTEM, "local"},
        {Options::DATA_FILE_EXTERNAL_PATHS, external_test_dir},
        {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"}};
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);
    auto gen = std::make_shared<DataGenerator>(table_schema.value(), pool_);
    int64_t commit_identifier = 0;
    PrepareSimpleAppendData(gen, helper.get(), &commit_identifier);
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
    ASSERT_EQ(11, snapshot5.value().TotalRecordCount().value());
    ASSERT_EQ(0, snapshot5.value().DeltaRecordCount().value());
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot5.value().GetCommitKind());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits.size(), 3);
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("f1=10/", 0)] = R"([
[0, "Alice", 10, 1, 11.1]
])";

    expected_datas[std::make_pair("f1=10/", 1)] = R"([
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Emily", 10, 0, 15.1],
[0, "Bob", 10, 0, 12.1],
[0, "Alex", 10, 0, 16.1],
[0, "David", 10, 0, 17.1],
[0, "Lily", 10, 0, 17.1]
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

TEST_F(CompactionInteTest, TestAppendTableWriteAlterTableWithCompaction) {
    std::string test_data_path =
        paimon::test::GetDataDir() +
        "/orc/append_table_with_alter_table.db/append_table_with_alter_table/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    arrow::FieldVector fields = {
        arrow::field("key0", arrow::int32()), arrow::field("key1", arrow::int32()),
        arrow::field("k", arrow::int32()),    arrow::field("c", arrow::int32()),
        arrow::field("d", arrow::int32()),    arrow::field("a", arrow::int32()),
        arrow::field("e", arrow::int32()),
    };
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "orc"},
                                                  {Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::FILE_SYSTEM, "local"}};
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(table_path, options, /*is_streaming_mode=*/true));
    // scan with empty split
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> empty_splits,
                         helper->NewScan(StartupMode::Latest(), /*snapshot_id=*/std::nullopt));
    ASSERT_TRUE(empty_splits.empty());

    int64_t commit_identifier = 0;
    auto data_type = arrow::struct_(fields);
    std::string data = R"([[1, 1, 116, 113, 567, 115, 668]])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch,
        TestHelper::MakeRecordBatch(data_type, data, {{"key0", "1"}, {"key1", "1"}}, /*bucket=*/0,
                                    /*row_kinds=*/{}));
    // for append only unaware bucket table, previous files will be ignored
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({1, 1, 116, 113, 567, 115, 668},
                                          {1, 1, 116, 113, 567, 115, 668}, {0, 0, 0, 0, 0, 0, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/1,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment({file_meta}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message = std::make_shared<CommitMessageImpl>(
        BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()), /*bucket=*/0,
        /*total_bucket=*/-1, data_increment, CompactIncrement({}, {}, {}));
    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages = {
        expected_commit_message};
    ASSERT_OK(
        helper->WriteAndCommit(std::move(batch), commit_identifier++, expected_commit_messages));

    ASSERT_OK(helper->write_->Compact(/*partition=*/{{"key0", "0"}, {"key1", "1"}}, /*bucket=*/0,
                                      /*full_compaction=*/true));
    ASSERT_OK(helper->write_->Compact(/*partition=*/{{"key0", "1"}, {"key1", "1"}}, /*bucket=*/0,
                                      /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> commit_messages,
        helper->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
    ASSERT_OK(helper->commit_->Commit(commit_messages, commit_identifier));

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot);

    ASSERT_EQ(1, snapshot.value().SchemaId());
    ASSERT_EQ(4, snapshot.value().Id());

    // read
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits, helper->Scan());
    ASSERT_EQ(data_splits.size(), 1);

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type_with_row_kind = arrow::struct_(fields_with_row_kind);
    std::string expected_data = R"([[0, 1, 1, 116, 113, 567, 115, 668]])";
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(data_type_with_row_kind,
                                                                  data_splits, expected_data));
    ASSERT_TRUE(success);
}

}  // namespace paimon::test
