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
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/global_index/bitmap/bitmap_global_index_factory.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/global_index_scan.h"
#include "paimon/global_index/row_range_global_index_writer.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
/// This is a sdk end-to-end test for global index.
class GlobalIndexTest : public ::testing::Test, public ::testing::WithParamInterface<std::string> {
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create("local");
        int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
        std::srand(seed);
    }
    void TearDown() override {
        dir_.reset();
    }

    void CreateTable(const std::vector<std::string>& partition_keys,
                     const std::shared_ptr<arrow::Schema>& schema,
                     const std::map<std::string, std::string>& options) const {
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir_->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema, partition_keys,
                                       /*primary_keys=*/{}, options,
                                       /*ignore_if_exists=*/false));
    }

    void CreateTable(const std::vector<std::string>& partition_keys) const {
        std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                      {Options::FILE_FORMAT, GetParam()},
                                                      {Options::FILE_SYSTEM, "local"},
                                                      {Options::ROW_TRACKING_ENABLED, "true"},
                                                      {Options::DATA_EVOLUTION_ENABLED, "true"}};
        return CreateTable(partition_keys, arrow::schema(fields_), options);
    }

    void CreateTable() const {
        return CreateTable(/*partition_keys=*/{});
    }

    Result<std::vector<std::shared_ptr<CommitMessage>>> WriteArray(
        const std::string& table_path, const std::map<std::string, std::string>& partition,
        const std::vector<std::string>& write_cols,
        const std::shared_ptr<arrow::Array>& write_array) const {
        // write
        WriteContextBuilder write_builder(table_path, "commit_user_1");
        write_builder.WithWriteSchema(write_cols);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto file_store_write,
                               FileStoreWrite::Create(std::move(write_context)));
        ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*write_array, &c_array).ok());
        auto record_batch = std::make_unique<RecordBatch>(
            partition, /*bucket=*/0,
            /*row_kinds=*/std::vector<RecordBatch::RowKind>(), &c_array);
        PAIMON_RETURN_NOT_OK(file_store_write->Write(std::move(record_batch)));
        PAIMON_ASSIGN_OR_RAISE(auto commit_msgs,
                               file_store_write->PrepareCommit(
                                   /*wait_compaction=*/false, /*commit_identifier=*/0));
        PAIMON_RETURN_NOT_OK(file_store_write->Close());
        return commit_msgs;
    }

    Result<std::vector<std::shared_ptr<CommitMessage>>> WriteArray(
        const std::string& table_path, const std::vector<std::string>& write_cols,
        const std::shared_ptr<arrow::Array>& write_array) const {
        return WriteArray(table_path, /*partition=*/{}, write_cols, write_array);
    }

    Status Commit(const std::string& table_path,
                  const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        // commit
        CommitContextBuilder commit_builder(table_path, "commit_user_1");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context,
                               commit_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> file_store_commit,
                               FileStoreCommit::Create(std::move(commit_context)));
        return file_store_commit->Commit(commit_msgs);
    }

    Result<std::shared_ptr<DataSplitImpl>> ScanData(
        const std::string& table_path,
        const std::vector<std::map<std::string, std::string>>& partition_filters) const {
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.SetPartitionFilter(partition_filters);
        PAIMON_ASSIGN_OR_RAISE(auto scan_context, scan_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_scan, TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(auto result_plan, table_scan->CreatePlan());
        EXPECT_EQ(result_plan->Splits().size(), 1);
        return std::dynamic_pointer_cast<DataSplitImpl>(result_plan->Splits()[0]);
    }

 private:
    std::unique_ptr<UniqueTestDirectory> dir_;
    arrow::FieldVector fields_ = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()),
        arrow::field("f3", arrow::float64()),
    };
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_P(GlobalIndexTest, TestWriteLuminaIndex) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::list(arrow::float32()))};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> lumina_options = {
        {"lumina.dimension", "4"},
        {"lumina.indextype", "bruteforce"},
        {"lumina.distance.metric", "l2"},
        {"lumina.encoding.type", "encoding.rawf32"},
        {"lumina.search.threadcount", "10"}};

    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, GetParam()},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};

    CreateTable(/*partition_keys=*/{}, schema, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    std::vector<std::string> write_cols = schema->field_names();
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["a", [0.0, 0.0, 0.0, 0.0]],
        ["b", [0.0, 1.0, 0.0, 1.0]],
        ["c", [1.0, 0.0, 1.0, 0.0]],
        ["d", [1.0, 1.0, 1.0, 1.0]]

    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    ASSERT_OK_AND_ASSIGN(auto split, ScanData(table_path, /*partition_filters=*/{}));
    ASSERT_OK_AND_ASSIGN(auto index_commit_msg, RowRangeGlobalIndexWriter::WriteIndex(
                                                    table_path, "f1", "lumina",
                                                    std::make_shared<IndexedSplitImpl>(
                                                        split, std::vector<Range>({Range(0, 3)})),
                                                    /*options=*/lumina_options, pool_));
    auto index_commit_msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(index_commit_msg);
    ASSERT_TRUE(index_commit_msg_impl);

    // check commit message
    GlobalIndexMeta expected_global_index_meta(
        /*row_range_start=*/0, /*row_range_end=*/3, /*index_field_id=*/1,
        /*extra_field_ids=*/std::nullopt, /*index_meta=*/nullptr);
    auto expected_index_file_meta =
        std::make_shared<IndexFileMeta>("lumina", /*file_name=*/"fake_index_file", /*file_size=*/10,
                                        /*row_count=*/4, expected_global_index_meta);
    DataIncrement expected_data_increment({expected_index_file_meta});
    auto expected_commit_message = std::make_shared<CommitMessageImpl>(
        /*partition=*/BinaryRow::EmptyRow(), /*bucket=*/0, /*total_buckets=*/std::nullopt,
        expected_data_increment, CompactIncrement({}, {}, {}));
    ASSERT_TRUE(expected_commit_message->TEST_Equal(*index_commit_msg_impl));
}

TEST_P(GlobalIndexTest, TestWriteIndex) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    std::vector<std::string> write_cols = schema->field_names();
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Emily", 10, 0, 13.1],
["Tony", 10, 0, 14.1],
["Lucy", 20, 1, 15.1],
["Bob", 10, 1, 16.1],
["Tony", 20, 0, 17.1],
["Alice", 20, null, 18.1]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    ASSERT_OK_AND_ASSIGN(auto split, ScanData(table_path, /*partition_filters=*/{}));
    ASSERT_OK_AND_ASSIGN(auto index_commit_msg, RowRangeGlobalIndexWriter::WriteIndex(
                                                    table_path, "f0", "bitmap",
                                                    std::make_shared<IndexedSplitImpl>(
                                                        split, std::vector<Range>({Range(0, 7)})),
                                                    /*options=*/{}, pool_));
    auto index_commit_msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(index_commit_msg);
    ASSERT_TRUE(index_commit_msg_impl);

    // check commit message
    GlobalIndexMeta expected_global_index_meta(
        /*row_range_start=*/0, /*row_range_end=*/7, /*index_field_id=*/0,
        /*extra_field_ids=*/std::nullopt, /*index_meta=*/nullptr);
    auto expected_index_file_meta =
        std::make_shared<IndexFileMeta>("bitmap", /*file_name=*/"fake_index_file", /*file_size=*/10,
                                        /*row_count=*/8, expected_global_index_meta);
    DataIncrement expected_data_increment({expected_index_file_meta});
    auto expected_commit_message = std::make_shared<CommitMessageImpl>(
        /*partition=*/BinaryRow::EmptyRow(), /*bucket=*/0, /*total_buckets=*/std::nullopt,
        expected_data_increment, CompactIncrement({}, {}, {}));
    ASSERT_TRUE(expected_commit_message->TEST_Equal(*index_commit_msg_impl));
}

TEST_P(GlobalIndexTest, TestWriteIndexWithPartition) {
    CreateTable(/*partition_keys=*/{"f1"});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    std::vector<std::string> write_cols = schema->field_names();
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Emily", 10, 0, 13.1],
["Tony", 10, 0, 14.1],
["Bob", 10, 1, 16.1]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         WriteArray(table_path, {{"f1", "10"}}, write_cols, src_array1));
    ASSERT_OK(Commit(table_path, commit_msgs1));

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Lucy", 20, 1, 15.1],
["Tony", 20, 0, 17.1],
["Alice", 20, null, 18.1]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2,
                         WriteArray(table_path, {{"f1", "20"}}, write_cols, src_array2));
    ASSERT_OK(Commit(table_path, commit_msgs2));

    auto build_index_and_check =
        [&](const std::vector<std::map<std::string, std::string>>& partition,
            const Range& expected_range, const BinaryRow& expected_partition_row) {
            ASSERT_OK_AND_ASSIGN(auto split, ScanData(table_path, partition));
            ASSERT_OK_AND_ASSIGN(
                auto index_commit_msg,
                RowRangeGlobalIndexWriter::WriteIndex(
                    table_path, "f0", "bitmap",
                    std::make_shared<IndexedSplitImpl>(split, std::vector<Range>({expected_range})),
                    /*options=*/{}, pool_));
            auto index_commit_msg_impl =
                std::dynamic_pointer_cast<CommitMessageImpl>(index_commit_msg);
            ASSERT_TRUE(index_commit_msg_impl);

            // check commit message
            GlobalIndexMeta expected_global_index_meta(
                /*row_range_start=*/expected_range.from, /*row_range_end=*/expected_range.to,
                /*index_field_id=*/0,
                /*extra_field_ids=*/std::nullopt, /*index_meta=*/nullptr);
            auto expected_index_file_meta = std::make_shared<IndexFileMeta>(
                "bitmap", /*file_name=*/"fake_index_file", /*file_size=*/10,
                /*row_count=*/expected_range.Count(), expected_global_index_meta);
            DataIncrement expected_data_increment({expected_index_file_meta});
            auto expected_commit_message = std::make_shared<CommitMessageImpl>(
                /*partition=*/expected_partition_row,
                //                /*partition=*/BinaryRowGenerator::GenerateRow({20}, pool_.get()),
                /*bucket=*/0,
                /*total_buckets=*/std::nullopt, expected_data_increment,
                CompactIncrement({}, {}, {}));
            ASSERT_TRUE(expected_commit_message->TEST_Equal(*index_commit_msg_impl));
        };

    // build index for f1=20 partition
    build_index_and_check({{{"f1", "20"}}}, Range(5, 7),
                          BinaryRowGenerator::GenerateRow({20}, pool_.get()));
    // build index for f1=10 partition
    build_index_and_check({{{"f1", "10"}}}, Range(0, 4),
                          BinaryRowGenerator::GenerateRow({10}, pool_.get()));
}

TEST_P(GlobalIndexTest, TestScanIndex) {
    if (GetParam() == "lance") {
        return;
    }

    std::string table_path = paimon::test::GetDataDir() + "/" + GetParam() +
                             "/append_with_global_index.db/append_with_global_index";
    ASSERT_OK_AND_ASSIGN(auto global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                 /*partitions=*/std::nullopt, /*options=*/{},
                                                 /*file_system=*/nullptr, pool_));
    ASSERT_OK_AND_ASSIGN(std::set<Range> ranges, global_index_scan->GetRowRangeList());
    ASSERT_EQ(ranges, std::set<Range>({Range(0, 7)}));
    ASSERT_OK_AND_ASSIGN(auto range_scanner, global_index_scan->CreateRangeScan(Range(0, 7)));
    // test index reader
    // test f0 field
    ASSERT_OK_AND_ASSIGN(auto index_reader, range_scanner->CreateReader("f0", "bitmap"));
    ASSERT_OK_AND_ASSIGN(auto index_result,
                         index_reader->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
    ASSERT_EQ(index_result->ToString(), "{0,7}");
    // test f0, f1, f2 fields
    ASSERT_OK_AND_ASSIGN(auto evaluator, range_scanner->CreateIndexEvaluator());
    {
        // test with non predicate
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(nullptr));
        ASSERT_FALSE(index_result);
    }
    {
        // test equal predicate for f0
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{0,7}");
    }
    {
        // test not equal predicate for f0
        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                       Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{1,2,3,4,5,6}");
    }
    {
        // test equal predicate for f1
        auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                 FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{4,6,7}");
    }
    {
        // test equal predicate for f2
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(1));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{0,1,4,5}");
    }
    {
        // test is null predicate
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{7}");
    }
    {
        // test is not null predicate
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{0,1,2,3,4,5,6}");
    }
    {
        // test in predicate
        auto predicate = PredicateBuilder::In(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
            {Literal(FieldType::STRING, "Alice", 5), Literal(FieldType::STRING, "Bob", 3),
             Literal(FieldType::STRING, "Lucy", 4)});
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{0,1,4,5,7}");
    }
    {
        // test not in predicate
        auto predicate = PredicateBuilder::NotIn(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
            {Literal(FieldType::STRING, "Alice", 5), Literal(FieldType::STRING, "Bob", 3),
             Literal(FieldType::STRING, "Lucy", 4)});
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{2,3,6}");
    }
    {
        // test and predicate
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({f0_predicate, f1_predicate}));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{7}");
    }
    {
        // test or predicate
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({f0_predicate, f1_predicate}));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{0,4,6,7}");
    }
    {
        // test non-result
        auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                 FieldType::INT, Literal(30));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{}");
    }
    {
        // test early stopping
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(10));
        auto f2_predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                    FieldType::INT, Literal(6));
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));

        ASSERT_OK_AND_ASSIGN(auto predicate,
                             PredicateBuilder::And({f1_predicate, f2_predicate, f0_predicate}));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{}");
    }
    {
        // test greater than predicate which bitmap index is not support, will return all range
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{0,1,2,3,4,5,6,7}");
    }
    {
        // test a predicate for field with no index
        auto f3_predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                    FieldType::DOUBLE, Literal(1.2));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(f3_predicate));
        ASSERT_FALSE(index_result);
    }
}

TEST_P(GlobalIndexTest, TestScanIndexWithSpecificSnapshot) {
    if (GetParam() == "lance") {
        return;
    }

    std::string table_path = paimon::test::GetDataDir() + "/" + GetParam() +
                             "/append_with_global_index.db/append_with_global_index";
    // snapshot 2 has f0 index
    ASSERT_OK_AND_ASSIGN(auto global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/2l,
                                                 /*partitions=*/std::nullopt, /*options=*/{},
                                                 /*file_system=*/nullptr, pool_));
    ASSERT_OK_AND_ASSIGN(std::set<Range> ranges, global_index_scan->GetRowRangeList());
    ASSERT_EQ(ranges, std::set<Range>({Range(0, 7)}));
    ASSERT_OK_AND_ASSIGN(auto range_scanner, global_index_scan->CreateRangeScan(Range(0, 7)));
    // test index reader
    // test f0 field
    ASSERT_OK_AND_ASSIGN(auto index_reader, range_scanner->CreateReader("f0", "bitmap"));
    ASSERT_OK_AND_ASSIGN(auto index_result,
                         index_reader->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
    ASSERT_EQ(index_result->ToString(), "{0,7}");
    // test f1 field
    ASSERT_OK_AND_ASSIGN(auto index_reader2, range_scanner->CreateReader("f1", "bitmap"));
    ASSERT_FALSE(index_reader2);

    // test evaluator
    ASSERT_OK_AND_ASSIGN(auto evaluator, range_scanner->CreateIndexEvaluator());
    {
        // test and predicate
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({f0_predicate, f1_predicate}));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(index_result.value()->ToString(), "{0,7}");
    }
    {
        // test or predicate
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({f0_predicate, f1_predicate}));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_FALSE(index_result);
    }
}

TEST_P(GlobalIndexTest, TestScanIndexWithSpecificSnapshotWithNoIndex) {
    if (GetParam() == "lance") {
        return;
    }

    std::string table_path = paimon::test::GetDataDir() + "/" + GetParam() +
                             "/append_with_global_index.db/append_with_global_index";
    // snapshot 1 has no index
    ASSERT_OK_AND_ASSIGN(auto global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/1l,
                                                 /*partitions=*/std::nullopt, /*options=*/{},
                                                 /*file_system=*/nullptr, pool_));
    ASSERT_OK_AND_ASSIGN(std::set<Range> ranges, global_index_scan->GetRowRangeList());
    ASSERT_TRUE(ranges.empty());

    ASSERT_OK_AND_ASSIGN(auto range_scanner, global_index_scan->CreateRangeScan(Range(0, 7)));
    // test index reader
    ASSERT_OK_AND_ASSIGN(auto index_reader, range_scanner->CreateReader("f0", "bitmap"));
    ASSERT_FALSE(index_reader);

    // test evaluator
    ASSERT_OK_AND_ASSIGN(auto evaluator, range_scanner->CreateIndexEvaluator());
    auto predicate =
        PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                   Literal(FieldType::STRING, "Alice", 5));
    ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
    ASSERT_FALSE(index_result);
}

TEST_P(GlobalIndexTest, TestScanIndexWithRange) {
    if (GetParam() == "lance") {
        return;
    }

    std::string table_path = paimon::test::GetDataDir() + "/" + GetParam() +
                             "/append_with_global_index.db/append_with_global_index";
    ASSERT_OK_AND_ASSIGN(auto global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                 /*partitions=*/std::nullopt, /*options=*/{},
                                                 /*file_system=*/nullptr, pool_));
    ASSERT_OK_AND_ASSIGN(std::set<Range> ranges, global_index_scan->GetRowRangeList());
    ASSERT_EQ(ranges, std::set<Range>({Range(0, 7)}));
    {
        ASSERT_OK_AND_ASSIGN(auto range_scanner, global_index_scan->CreateRangeScan(Range(0, 3)));
        // test index reader
        ASSERT_OK_AND_ASSIGN(auto index_reader, range_scanner->CreateReader("f0", "bitmap"));
        ASSERT_OK_AND_ASSIGN(auto index_result,
                             index_reader->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
        ASSERT_EQ(index_result->ToString(), "{0,7}");

        // test evaluator
        ASSERT_OK_AND_ASSIGN(auto evaluator, range_scanner->CreateIndexEvaluator());
        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                       Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto evaluator_result, evaluator->Evaluate(predicate));
        ASSERT_EQ(evaluator_result.value()->ToString(), "{1,2,3,4,5,6}");
    }
    {
        ASSERT_OK_AND_ASSIGN(auto range_scanner, global_index_scan->CreateRangeScan(Range(10, 13)));
        // test index reader
        ASSERT_OK_AND_ASSIGN(auto index_reader, range_scanner->CreateReader("f0", "bitmap"));
        ASSERT_FALSE(index_reader);
        // test evaluator
        ASSERT_OK_AND_ASSIGN(auto evaluator, range_scanner->CreateIndexEvaluator());
        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                       Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
        ASSERT_FALSE(index_result);
    }
}

TEST_P(GlobalIndexTest, TestScanIndexWithPartition) {
    if (GetParam() == "lance") {
        return;
    }

    // only f1=10 has index
    std::string table_path =
        paimon::test::GetDataDir() + "/" + GetParam() +
        "/append_with_global_index_with_partition.db/append_with_global_index_with_partition";
    auto check_result =
        [&](const std::optional<std::vector<std::map<std::string, std::string>>>& partitions) {
            ASSERT_OK_AND_ASSIGN(auto global_index_scan,
                                 GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                         partitions, /*options=*/{},
                                                         /*file_system=*/nullptr, pool_));
            ASSERT_OK_AND_ASSIGN(std::set<Range> ranges, global_index_scan->GetRowRangeList());
            ASSERT_EQ(ranges, std::set<Range>({Range(0, 4)}));
            ASSERT_OK_AND_ASSIGN(auto range_scanner,
                                 global_index_scan->CreateRangeScan(Range(0, 4)));
            // test index reader
            ASSERT_OK_AND_ASSIGN(auto index_reader, range_scanner->CreateReader("f0", "bitmap"));
            ASSERT_OK_AND_ASSIGN(auto index_result,
                                 index_reader->VisitEqual(Literal(FieldType::STRING, "Bob", 3)));
            ASSERT_EQ(index_result->ToString(), "{1,4}");

            // test evaluator
            ASSERT_OK_AND_ASSIGN(auto evaluator, range_scanner->CreateIndexEvaluator());
            {
                // null result as f2 does not have index
                auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                         FieldType::INT, Literal(1));

                ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
                ASSERT_FALSE(index_result);
            }
            {
                // test not equal predicate for Bob
                auto predicate = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                            FieldType::STRING,
                                                            Literal(FieldType::STRING, "Bob", 3));
                ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
                ASSERT_EQ(index_result.value()->ToString(), "{0,2,3}");
            }
            {
                // test equal predicate for Alice
                auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                         FieldType::STRING,
                                                         Literal(FieldType::STRING, "Alice", 5));
                ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
                ASSERT_EQ(index_result.value()->ToString(), "{0}");
            }
        };

    std::vector<std::map<std::string, std::string>> partitions = {{{"f1", "10"}}};
    check_result(partitions);
    check_result(std::nullopt);
}

TEST_P(GlobalIndexTest, TestScanUnregisteredIndex) {
    if (GetParam() == "lance") {
        return;
    }
    auto factory_creator = FactoryCreator::GetInstance();
    factory_creator->TEST_Unregister("bitmap-global");
    ScopeGuard guard([&factory_creator]() {
        factory_creator->Register("bitmap-global", (new BitmapGlobalIndexFactory));
    });

    std::string table_path = paimon::test::GetDataDir() + "/" + GetParam() +
                             "/append_with_global_index.db/append_with_global_index";
    ASSERT_OK_AND_ASSIGN(auto global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                 /*partitions=*/std::nullopt, /*options=*/{},
                                                 /*file_system=*/nullptr, pool_));
    ASSERT_OK_AND_ASSIGN(auto range_scanner, global_index_scan->CreateRangeScan(Range(0, 7)));
    ASSERT_OK_AND_ASSIGN(auto index_reader, range_scanner->CreateReader("f0", "bitmap"));
    ASSERT_FALSE(index_reader);

    ASSERT_OK_AND_ASSIGN(auto evaluator, range_scanner->CreateIndexEvaluator());
    auto predicate =
        PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                   Literal(FieldType::STRING, "Bob", 3));

    ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
    ASSERT_FALSE(index_result);
}

TEST_P(GlobalIndexTest, TestWriteCommitScanReadIndex) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    std::vector<std::string> write_cols = schema->field_names();
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Emily", 10, 0, 13.1],
["Tony", 10, 0, 14.1],
["Lucy", 20, 1, 15.1],
["Bob", 10, 1, 16.1],
["Tony", 20, 0, 17.1],
["Alice", 20, null, 18.1]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    ASSERT_OK_AND_ASSIGN(auto split, ScanData(table_path, /*partition_filters=*/{}));
    ASSERT_OK_AND_ASSIGN(auto index_commit_msg, RowRangeGlobalIndexWriter::WriteIndex(
                                                    table_path, "f0", "bitmap",
                                                    std::make_shared<IndexedSplitImpl>(
                                                        split, std::vector<Range>({Range(0, 7)})),
                                                    /*options=*/{}, pool_));
    ASSERT_OK(Commit(table_path, {index_commit_msg}));

    ASSERT_OK_AND_ASSIGN(auto global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                 /*partitions=*/std::nullopt, /*options=*/{},
                                                 /*file_system=*/nullptr, pool_));
    ASSERT_OK_AND_ASSIGN(std::set<Range> ranges, global_index_scan->GetRowRangeList());
    ASSERT_EQ(ranges, std::set<Range>({Range(0, 7)}));
    ASSERT_OK_AND_ASSIGN(auto range_scanner, global_index_scan->CreateRangeScan(Range(0, 7)));
    ASSERT_OK_AND_ASSIGN(auto index_reader, range_scanner->CreateReader("f0", "bitmap"));
    ASSERT_OK_AND_ASSIGN(auto index_result,
                         index_reader->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
    ASSERT_EQ(index_result->ToString(), "{0,7}");
}

TEST_P(GlobalIndexTest, TestWriteCommitScanReadIndexWithPartition) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::list(arrow::float32())),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    std::map<std::string, std::string> lumina_options = {
        {"lumina.dimension", "4"},
        {"lumina.indextype", "bruteforce"},
        {"lumina.distance.metric", "l2"},
        {"lumina.encoding.type", "encoding.rawf32"},
        {"lumina.search.threadcount", "10"}};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, GetParam()},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};
    CreateTable(/*partition_keys=*/{"f2"}, schema, options);

    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    std::vector<std::string> write_cols = schema->field_names();
    auto write_data_and_index = [&](const std::shared_ptr<arrow::Array>& src_array,
                                    const std::map<std::string, std::string>& partition,
                                    const Range& expected_range) {
        ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                             WriteArray(table_path, partition, write_cols, src_array));
        ASSERT_OK(Commit(table_path, commit_msgs));

        ASSERT_OK_AND_ASSIGN(auto split, ScanData(table_path, /*partition_filters=*/{partition}));
        // write bitmap index
        ASSERT_OK_AND_ASSIGN(
            auto bitmap_commit_msg,
            RowRangeGlobalIndexWriter::WriteIndex(
                table_path, "f0", "bitmap",
                std::make_shared<IndexedSplitImpl>(split, std::vector<Range>({expected_range})),
                /*options=*/{}, pool_));
        ASSERT_OK(Commit(table_path, {bitmap_commit_msg}));
        // write and commit lumina index
        ASSERT_OK_AND_ASSIGN(
            auto lumina_commit_msg,
            RowRangeGlobalIndexWriter::WriteIndex(
                table_path, "f1", "lumina",
                std::make_shared<IndexedSplitImpl>(split, std::vector<Range>({expected_range})),
                lumina_options, pool_));
        ASSERT_OK(Commit(table_path, {lumina_commit_msg}));
    };

    // write partition f2 = 10
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["Alice", [0.0, 0.0, 0.0, 0.0], 10, 11.1],
["Bob", [0.0, 1.0, 0.0, 1.0], 10, 12.1],
["Emily", [1.0, 0.0, 1.0, 0.0], 10, 13.1],
["Tony", [1.0, 1.0, 1.0, 1.0], 10, 14.1]
    ])")
            .ValueOrDie());
    write_data_and_index(src_array1, {{"f2", "10"}}, Range(0, 3));

    // write partition f2 = 20
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["Lucy", [10.0, 10.0, 10.0, 10.0], 20, 15.1],
["Bob", [10.0, 11.0, 10.0, 11.0], 20, 16.1],
["Tony", [11.0, 10.0, 11.0, 10.0], 20, 17.1],
["Alice", [11.0, 11.0, 11.0, 11.0], 20, 18.1],
["Paul", [10.0, 10.0, 10.0, 10.0], 20, 19.1]
    ])")
            .ValueOrDie());
    write_data_and_index(src_array2, {{"f2", "20"}}, Range(4, 8));

    auto scan_and_check_result =
        [&](const std::map<std::string, std::string>& partition, const Range& expected_range,
            GlobalIndexReader::TopKPreFilter filter, int32_t k, const std::string bitmap_result,
            const std::string lumina_result) {
            std::vector<std::map<std::string, std::string>> partitions = {partition};
            ASSERT_OK_AND_ASSIGN(auto global_index_scan,
                                 GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                         partitions, lumina_options,
                                                         /*file_system=*/nullptr, pool_));
            ASSERT_OK_AND_ASSIGN(std::set<Range> ranges, global_index_scan->GetRowRangeList());
            ASSERT_EQ(ranges, std::set<Range>({expected_range}));

            ASSERT_OK_AND_ASSIGN(auto range_scanner,
                                 global_index_scan->CreateRangeScan(expected_range));

            // check bitmap index
            ASSERT_OK_AND_ASSIGN(auto evaluator, range_scanner->CreateIndexEvaluator());

            auto predicate1 =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Alice", 5));
            auto predicate2 =
                PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                        Literal(FieldType::STRING, "Paul", 4));
            ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({predicate1, predicate2}));

            ASSERT_OK_AND_ASSIGN(auto index_result, evaluator->Evaluate(predicate));
            ASSERT_TRUE(index_result);
            ASSERT_EQ(index_result.value()->ToString(), bitmap_result);

            // check lumina index
            ASSERT_OK_AND_ASSIGN(auto lumina_reader, range_scanner->CreateReader("f1", "lumina"));

            std::vector<float> query = {1.0f, 1.0f, 1.0f, 1.1f};
            ASSERT_OK_AND_ASSIGN(auto topk_result, lumina_reader->VisitTopK(k, query, filter,
                                                                            /*predicate*/ nullptr));
            ASSERT_EQ(topk_result->ToString(), lumina_result);
        };

    auto filter1 = [](int64_t id) -> bool { return id == 0; };
    scan_and_check_result({{"f2", "10"}}, Range(0, 3), filter1, /*k=*/2, "{0}",
                          "row ids: {0}, scores: {4.21}");
    auto filter2 = [](int64_t id) -> bool { return id == 7 || id == 8; };
    scan_and_check_result({{"f2", "20"}}, Range(4, 8), filter2, /*k=*/1, "{7,8}",
                          "row ids: {8}, scores: {322.21}");

    {
        // test invalid range input
        ASSERT_OK_AND_ASSIGN(auto global_index_scan,
                             GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                     /*partitions=*/std::nullopt, lumina_options,
                                                     /*file_system=*/nullptr, pool_));
        ASSERT_NOK_WITH_MSG(global_index_scan->CreateRangeScan(Range(0, 8)),
                            "input range contain multiple partitions, fail to create range scan");
    }
}

std::vector<std::string> GetTestValuesForGlobalIndexTest() {
    std::vector<std::string> values = {"parquet"};
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back("orc");
#endif
#ifdef PAIMON_ENABLE_LANCE
    values.emplace_back("lance");
#endif
    return values;
}

INSTANTIATE_TEST_SUITE_P(FileFormat, GlobalIndexTest,
                         ::testing::ValuesIn(GetTestValuesForGlobalIndexTest()));

}  // namespace paimon::test
