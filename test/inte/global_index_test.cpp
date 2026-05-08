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
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/global_index/bitmap/bitmap_global_index_factory.h"
#include "paimon/common/global_index/union_global_index_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/global_index_scan_impl.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/global_index/global_index_result.h"
#include "paimon/global_index/global_index_scan.h"
#include "paimon/global_index/global_index_write_task.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
// string: FileFormat, bool: UseSpecificFileSystem
using ParamType = std::tuple<std::string, bool>;
/// This is a sdk end-to-end test for global index.
class GlobalIndexTest : public ::testing::Test, public ::testing::WithParamInterface<ParamType> {
    void SetUp() override {
        file_format_ = std::get<0>(GetParam());
        dir_ = UniqueTestDirectory::Create("local");
        if (std::get<1>(GetParam())) {
            fs_ = dir_->GetFileSystem();
        }
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

        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir_->Str(), {}, fs_));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema, partition_keys,
                                       /*primary_keys=*/{}, options,
                                       /*ignore_if_exists=*/false));
    }

    void CreateTable(const std::vector<std::string>& partition_keys) const {
        std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                      {Options::FILE_FORMAT, file_format_},
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
        write_builder.WithFileSystem(fs_);
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
        commit_builder.WithFileSystem(fs_);
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
        scan_context_builder.WithFileSystem(fs_);
        PAIMON_ASSIGN_OR_RAISE(auto scan_context, scan_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_scan, TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(auto result_plan, table_scan->CreatePlan());
        EXPECT_EQ(result_plan->Splits().size(), 1);
        return std::dynamic_pointer_cast<DataSplitImpl>(result_plan->Splits()[0]);
    }

    Status WriteIndex(const std::string& table_path,
                      const std::vector<std::map<std::string, std::string>>& partition_filters,
                      const std::string& index_field_name, const std::string& index_type,
                      const std::map<std::string, std::string>& options, const Range& range) {
        PAIMON_ASSIGN_OR_RAISE(auto split, ScanData(table_path, partition_filters));
        PAIMON_ASSIGN_OR_RAISE(auto index_commit_msg, GlobalIndexWriteTask::WriteIndex(
                                                          table_path, index_field_name, index_type,
                                                          std::make_shared<IndexedSplitImpl>(
                                                              split, std::vector<Range>({range})),
                                                          options, pool_, fs_));
        return Commit(table_path, {index_commit_msg});
    }

    Result<std::shared_ptr<Plan>> ScanGlobalIndexAndData(
        const std::string& table_path, const std::shared_ptr<Predicate>& predicate,
        const std::map<std::string, std::string>& options = {},
        const std::shared_ptr<GlobalIndexResult>& index_result = nullptr) const {
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.SetPredicate(predicate)
            .SetOptions(options)
            .SetGlobalIndexResult(index_result)
            .WithFileSystem(fs_);
        PAIMON_ASSIGN_OR_RAISE(auto scan_context, scan_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_scan, TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(auto result_plan, table_scan->CreatePlan());
        return result_plan;
    }

    Result<std::shared_ptr<Plan>> ScanDataWithIndexResult(
        const std::string& table_path, const std::vector<Range>& row_ranges,
        const std::map<int64_t, float>& id_to_score) const {
        std::shared_ptr<GlobalIndexResult> index_result;
        if (id_to_score.empty()) {
            index_result = BitmapGlobalIndexResult::FromRanges(row_ranges);
        } else {
            RoaringBitmap64 bitmap;
            for (const auto& range : row_ranges) {
                bitmap.AddRange(range.from, range.to + 1);
            }
            std::vector<float> scores;
            for (auto iter = bitmap.Begin(); iter != bitmap.End(); ++iter) {
                scores.push_back(id_to_score.at(*iter));
            }
            index_result = std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap),
                                                                           std::move(scores));
        }
        return ScanGlobalIndexAndData(table_path, /*predicate=*/nullptr,
                                      /*options=*/{}, index_result);
    }

    Status ReadData(const std::string& table_path, const std::vector<std::string>& read_schema,
                    const std::shared_ptr<arrow::Array>& expected_array,
                    const std::shared_ptr<Predicate>& predicate,
                    const std::shared_ptr<Plan>& result_plan) const {
        auto splits = result_plan->Splits();
        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.SetReadSchema(read_schema).SetPredicate(predicate).WithFileSystem(fs_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context,
                               read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(auto batch_reader, table_read->CreateReader(splits));
        PAIMON_ASSIGN_OR_RAISE(auto read_result,
                               ReadResultCollector::CollectResult(batch_reader.get()));

        if (!expected_array) {
            if (read_result) {
                return Status::Invalid("expected array is empty, but read result is not empty");
            }
            return Status::OK();
        }
        auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
        if (!expected_chunk_array->ApproxEquals(*read_result,
                                                arrow::EqualOptions::Defaults().atol(1E-2))) {
            std::cout << "result=" << read_result->ToString() << std::endl
                      << "expected=" << expected_chunk_array->ToString() << std::endl;
            return Status::Invalid("expected array and result array not equal");
        }
        return Status::OK();
    }

 private:
    std::string file_format_;
    std::unique_ptr<UniqueTestDirectory> dir_;
    arrow::FieldVector fields_ = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()),
        arrow::field("f3", arrow::float64()),
    };
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = nullptr;
};

#ifdef PAIMON_ENABLE_LUMINA

TEST_P(GlobalIndexTest, TestWriteLuminaIndex) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::list(arrow::float32()))};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> lumina_options = {{"lumina.index.dimension", "4"},
                                                         {"lumina.index.type", "bruteforce"},
                                                         {"lumina.distance.metric", "l2"},
                                                         {"lumina.encoding.type", "rawf32"},
                                                         {"lumina.search.parallel_number", "10"}};

    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format_},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};

    CreateTable(/*partition_keys=*/{}, schema, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    std::vector<std::string> write_cols = schema->field_names();
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["a", [0.0, 0.0, 0.0, 0.0]],
        ["b", [0.0, 1.0, 0.0, 1.0]],
        ["c", [1.0, 0.0, 1.0, 0.0]],
        ["d", [1.0, 1.0, 1.0, 1.0]]

    ])")
                         .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    ASSERT_OK_AND_ASSIGN(auto split, ScanData(table_path, /*partition_filters=*/{}));
    ASSERT_OK_AND_ASSIGN(auto index_commit_msg, GlobalIndexWriteTask::WriteIndex(
                                                    table_path, "f1", "lumina",
                                                    std::make_shared<IndexedSplitImpl>(
                                                        split, std::vector<Range>({Range(0, 3)})),
                                                    /*options=*/lumina_options, pool_));
    auto index_commit_msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(index_commit_msg);
    ASSERT_TRUE(index_commit_msg_impl);

    // check commit message
    std::string index_meta_json =
        R"({"distance.metric":"l2","encoding.type":"rawf32","index.dimension":"4","index.type":"bruteforce","search.parallel_number":"10"})";
    GlobalIndexMeta expected_global_index_meta(
        /*row_range_start=*/0, /*row_range_end=*/3, /*index_field_id=*/1,
        /*extra_field_ids=*/std::nullopt, std::make_shared<Bytes>(index_meta_json, pool_.get()));
    auto expected_index_file_meta =
        std::make_shared<IndexFileMeta>("lumina", /*file_name=*/"fake_index_file", /*file_size=*/10,
                                        /*row_count=*/4, /*dv_ranges=*/std::nullopt,
                                        /*external_path=*/std::nullopt, expected_global_index_meta);
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
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Emily", 10, 0, 13.1],
["Tony", 10, 0, 14.1],
["Lucy", 20, 1, 15.1],
["Bob", 10, 1, 16.1],
["Tony", 20, 0, 17.1],
["Alice", 20, null, 18.1]
    ])")
                         .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    ASSERT_OK_AND_ASSIGN(auto split, ScanData(table_path, /*partition_filters=*/{}));
    ASSERT_OK_AND_ASSIGN(auto index_commit_msg, GlobalIndexWriteTask::WriteIndex(
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
                                        /*row_count=*/8, /*dv_ranges=*/std::nullopt,
                                        /*external_path=*/std::nullopt, expected_global_index_meta);
    DataIncrement expected_data_increment({expected_index_file_meta});
    auto expected_commit_message = std::make_shared<CommitMessageImpl>(
        /*partition=*/BinaryRow::EmptyRow(), /*bucket=*/0, /*total_buckets=*/std::nullopt,
        expected_data_increment, CompactIncrement({}, {}, {}));
    ASSERT_TRUE(expected_commit_message->TEST_Equal(*index_commit_msg_impl));

    {
        // test invalid write task with none-registered index type
        ASSERT_NOK_WITH_MSG(
            GlobalIndexWriteTask::WriteIndex(
                table_path, "f0", "invalid",
                std::make_shared<IndexedSplitImpl>(split, std::vector<Range>({Range(0, 7)})),
                /*options=*/{}, pool_),
            "Unknown index type invalid, may not registered");
    }
    {
        // test invalid multiple ranges
        ASSERT_NOK_WITH_MSG(GlobalIndexWriteTask::WriteIndex(
                                table_path, "f0", "bitmap",
                                std::make_shared<IndexedSplitImpl>(
                                    split, std::vector<Range>({Range(0, 6), Range(7, 7)})),
                                /*options=*/{}, pool_),
                            "GlobalIndexWriteTask only supports a single contiguous range.");
    }
}

TEST_P(GlobalIndexTest, TestWriteIndexWithPartition) {
    CreateTable(/*partition_keys=*/{"f1"});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    std::vector<std::string> write_cols = schema->field_names();
    auto src_array1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Emily", 10, 0, 13.1],
["Tony", 10, 0, 14.1],
["Bob", 10, 1, 16.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         WriteArray(table_path, {{"f1", "10"}}, write_cols, src_array1));
    ASSERT_OK(Commit(table_path, commit_msgs1));

    auto src_array2 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Lucy", 20, 1, 15.1],
["Tony", 20, 0, 17.1],
["Alice", 20, null, 18.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2,
                         WriteArray(table_path, {{"f1", "20"}}, write_cols, src_array2));
    ASSERT_OK(Commit(table_path, commit_msgs2));

    auto build_index_and_check =
        [&](const std::vector<std::map<std::string, std::string>>& partition,
            const Range& expected_range, const BinaryRow& expected_partition_row) {
            ASSERT_OK_AND_ASSIGN(auto split, ScanData(table_path, partition));
            ASSERT_OK_AND_ASSIGN(
                auto index_commit_msg,
                GlobalIndexWriteTask::WriteIndex(
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
                /*row_count=*/expected_range.Count(), /*dv_ranges=*/std::nullopt,
                /*external_path=*/std::nullopt, expected_global_index_meta);
            DataIncrement expected_data_increment({expected_index_file_meta});
            auto expected_commit_message = std::make_shared<CommitMessageImpl>(
                /*partition=*/expected_partition_row,
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
#endif

TEST_P(GlobalIndexTest, TestScanIndex) {
    if (file_format_ == "lance" || file_format_ == "avro") {
        return;
    }

    std::string table_path = paimon::test::GetDataDir() + "/" + file_format_ +
                             "/append_with_global_index.db/append_with_global_index";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexScan> global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                 /*partitions=*/std::nullopt, /*options=*/{}, fs_,
                                                 /*executor=*/nullptr, pool_));
    // test index reader
    // test f0 field
    ASSERT_OK_AND_ASSIGN(auto index_readers, global_index_scan->CreateReaders("f0", std::nullopt));
    ASSERT_EQ(index_readers.size(), 1u);
    ASSERT_OK_AND_ASSIGN(auto index_result,
                         index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
    ASSERT_EQ(index_result->ToString(), "{0,7}");
    // test f0, f1, f2 fields
    auto global_index_scan_impl = std::dynamic_pointer_cast<GlobalIndexScanImpl>(global_index_scan);
    {
        // test with non predicate
        ASSERT_OK_AND_ASSIGN(auto index_result,
                             global_index_scan_impl->Scan(/*predicate=*/nullptr));
        ASSERT_FALSE(index_result);
    }
    {
        // test equal predicate for f0
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{0,7}");
    }
    {
        // test not equal predicate for f0
        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                       Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{1,2,3,4,5,6}");
    }
    {
        // test equal predicate for f1
        auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                 FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{4,6,7}");
    }
    {
        // test equal predicate for f2
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(1));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{0,1,4,5}");
    }
    {
        // test is null predicate
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{7}");
    }
    {
        // test is not null predicate
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{0,1,2,3,4,5,6}");
    }
    {
        // test in predicate
        auto predicate = PredicateBuilder::In(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
            {Literal(FieldType::STRING, "Alice", 5), Literal(FieldType::STRING, "Bob", 3),
             Literal(FieldType::STRING, "Lucy", 4)});
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{0,1,4,5,7}");
    }
    {
        // test not in predicate
        auto predicate = PredicateBuilder::NotIn(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
            {Literal(FieldType::STRING, "Alice", 5), Literal(FieldType::STRING, "Bob", 3),
             Literal(FieldType::STRING, "Lucy", 4)});
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{2,3,6}");
    }
    {
        // test and predicate
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({f0_predicate, f1_predicate}));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{7}");
    }
    {
        // test or predicate
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({f0_predicate, f1_predicate}));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{0,4,6,7}");
    }
    {
        // test non-result
        auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                 FieldType::INT, Literal(30));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{}");
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
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{}");
    }
    {
        // test greater than predicate which bitmap index is not support, will return all range
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_FALSE(index_result);
    }
    {
        // test greater or equal predicate which bitmap index is not support, will return all range
        auto predicate = PredicateBuilder::GreaterOrEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                          FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_FALSE(index_result);
    }
    {
        // test less than predicate which bitmap index is not support, will return all range
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_FALSE(index_result);
    }
    {
        // test less or equal predicate which bitmap index is not support, will return all range
        auto predicate = PredicateBuilder::LessOrEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_FALSE(index_result);
    }
    {
        // test a predicate for field with no index
        auto f3_predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                    FieldType::DOUBLE, Literal(1.2));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(f3_predicate));
        ASSERT_FALSE(index_result);
    }
}

TEST_P(GlobalIndexTest, TestScanIndexWithSpecificSnapshot) {
    if (file_format_ == "lance" || file_format_ == "avro") {
        return;
    }

    std::string table_path = paimon::test::GetDataDir() + "/" + file_format_ +
                             "/append_with_global_index.db/append_with_global_index";
    // snapshot 2 has f0 index
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexScan> global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/2l,
                                                 /*partitions=*/std::nullopt, /*options=*/{}, fs_,
                                                 /*executor=*/nullptr, pool_));
    // test index reader
    // test f0 field
    ASSERT_OK_AND_ASSIGN(auto index_readers, global_index_scan->CreateReaders("f0", std::nullopt));
    ASSERT_EQ(index_readers.size(), 1u);
    ASSERT_OK_AND_ASSIGN(auto index_result,
                         index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
    ASSERT_EQ(index_result->ToString(), "{0,7}");
    // test f1 field
    ASSERT_OK_AND_ASSIGN(auto index_readers2, global_index_scan->CreateReaders("f1", std::nullopt));
    ASSERT_EQ(index_readers2.size(), 0u);

    auto global_index_scan_impl = std::dynamic_pointer_cast<GlobalIndexScanImpl>(global_index_scan);

    {
        // test and predicate
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({f0_predicate, f1_predicate}));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(index_result->ToString(), "{0,7}");
    }
    {
        // test or predicate
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({f0_predicate, f1_predicate}));
        ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
        ASSERT_FALSE(index_result);
    }
}

TEST_P(GlobalIndexTest, TestScanIndexWithSpecificSnapshotWithNoIndex) {
    if (file_format_ == "lance" || file_format_ == "avro") {
        return;
    }

    std::string table_path = paimon::test::GetDataDir() + "/" + file_format_ +
                             "/append_with_global_index.db/append_with_global_index";
    // snapshot 1 has no index
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexScan> global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/1l,
                                                 /*partitions=*/std::nullopt, /*options=*/{}, fs_,
                                                 /*executor=*/nullptr, pool_));
    // test index reader
    ASSERT_OK_AND_ASSIGN(auto index_readers, global_index_scan->CreateReaders("f0", std::nullopt));
    ASSERT_EQ(index_readers.size(), 0u);

    auto global_index_scan_impl = std::dynamic_pointer_cast<GlobalIndexScanImpl>(global_index_scan);

    auto predicate =
        PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                   Literal(FieldType::STRING, "Alice", 5));
    ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
    ASSERT_FALSE(index_result);
}

TEST_P(GlobalIndexTest, TestScanIndexWithRange) {
    if (file_format_ == "lance" || file_format_ == "avro") {
        return;
    }

    std::string table_path = paimon::test::GetDataDir() + "/" + file_format_ +
                             "/append_with_global_index.db/append_with_global_index";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexScan> global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                 /*partitions=*/std::nullopt, /*options=*/{}, fs_,
                                                 /*executor=*/nullptr, pool_));
    auto global_index_scan_impl = std::dynamic_pointer_cast<GlobalIndexScanImpl>(global_index_scan);
    {
        // test index reader
        ASSERT_OK_AND_ASSIGN(auto index_readers,
                             global_index_scan->CreateReaders("f0", std::nullopt));
        ASSERT_EQ(index_readers.size(), 1u);
        ASSERT_OK_AND_ASSIGN(auto index_result,
                             index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
        ASSERT_EQ(index_result->ToString(), "{0,7}");

        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                       Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto evaluator_result, global_index_scan_impl->Scan(predicate));
        ASSERT_EQ(evaluator_result->ToString(), "{1,2,3,4,5,6}");
    }
    {
        // invalid range
        ASSERT_OK_AND_ASSIGN(RowRangeIndex row_range_index, RowRangeIndex::Create({Range(10, 13)}));
        ASSERT_OK_AND_ASSIGN(auto index_readers,
                             global_index_scan->CreateReaders("f0", row_range_index));
        ASSERT_EQ(index_readers.size(), 0u);
    }
}

TEST_P(GlobalIndexTest, TestScanIndexWithPartition) {
    if (file_format_ == "lance" || file_format_ == "avro") {
        return;
    }

    // only f1=10 has index
    std::string table_path =
        paimon::test::GetDataDir() + "/" + file_format_ +
        "/append_with_global_index_with_partition.db/append_with_global_index_with_partition";
    auto check_result =
        [&](const std::optional<std::vector<std::map<std::string, std::string>>>& partitions) {
            ASSERT_OK_AND_ASSIGN(
                std::shared_ptr<GlobalIndexScan> global_index_scan,
                GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt, partitions,
                                        /*options=*/{}, fs_, /*executor=*/nullptr, pool_));
            // test index reader
            ASSERT_OK_AND_ASSIGN(RowRangeIndex row_range_index,
                                 RowRangeIndex::Create({Range(0, 4)}));
            ASSERT_OK_AND_ASSIGN(auto index_readers,
                                 global_index_scan->CreateReaders("f0", row_range_index));
            ASSERT_EQ(index_readers.size(), 1u);
            ASSERT_OK_AND_ASSIGN(auto index_result, index_readers[0]->VisitEqual(
                                                        Literal(FieldType::STRING, "Bob", 3)));
            ASSERT_EQ(index_result->ToString(), "{1,4}");

            auto global_index_scan_impl =
                std::dynamic_pointer_cast<GlobalIndexScanImpl>(global_index_scan);

            {
                // null result as f2 does not have index
                auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                         FieldType::INT, Literal(1));

                ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
                ASSERT_FALSE(index_result);
            }
            {
                // test not equal predicate for Bob
                ASSERT_OK_AND_ASSIGN(auto index_result, index_readers[0]->VisitNotEqual(
                                                            Literal(FieldType::STRING, "Bob", 3)));
                ASSERT_EQ(index_result->ToString(), "{0,2,3}");
            }
            {
                // test equal predicate for Alice
                ASSERT_OK_AND_ASSIGN(auto index_result, index_readers[0]->VisitEqual(Literal(
                                                            FieldType::STRING, "Alice", 5)));
                ASSERT_EQ(index_result->ToString(), "{0}");
            }
        };

    std::vector<std::map<std::string, std::string>> partitions = {{{"f1", "10"}}};
    check_result(partitions);
    check_result(std::nullopt);
}

TEST_P(GlobalIndexTest, TestScanUnregisteredIndex) {
    if (file_format_ == "lance" || file_format_ == "avro") {
        return;
    }
    auto factory_creator = FactoryCreator::GetInstance();
    factory_creator->TEST_Unregister("bitmap-global");
    ScopeGuard guard([&factory_creator]() {
        factory_creator->Register("bitmap-global", (new BitmapGlobalIndexFactory));
    });

    std::string table_path = paimon::test::GetDataDir() + "/" + file_format_ +
                             "/append_with_global_index.db/append_with_global_index";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexScan> global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                 /*partitions=*/std::nullopt, /*options=*/{}, fs_,
                                                 /*executor=*/nullptr, pool_));
    ASSERT_OK_AND_ASSIGN(auto index_readers, global_index_scan->CreateReaders("f0", std::nullopt));
    ASSERT_EQ(index_readers.size(), 0u);

    auto global_index_scan_impl = std::dynamic_pointer_cast<GlobalIndexScanImpl>(global_index_scan);
    auto predicate =
        PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                   Literal(FieldType::STRING, "Bob", 3));

    ASSERT_OK_AND_ASSIGN(auto index_result, global_index_scan_impl->Scan(predicate));
    ASSERT_FALSE(index_result);
}

TEST_P(GlobalIndexTest, TestWriteCommitScanReadIndex) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    std::vector<std::string> write_cols = schema->field_names();
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Emily", 10, 0, 13.1],
["Tony", 10, 0, 14.1],
["Lucy", 20, 1, 15.1],
["Bob", 10, 1, 16.1],
["Tony", 20, 0, 17.1],
["Alice", 20, null, 18.1]
    ])")
                         .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f0", "bitmap",
                         /*options=*/{}, Range(0, 7)));

    ASSERT_OK_AND_ASSIGN(auto global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                 /*partitions=*/std::nullopt, /*options=*/{}, fs_,
                                                 /*executor=*/nullptr, pool_));
    ASSERT_OK_AND_ASSIGN(auto index_readers, global_index_scan->CreateReaders("f0", std::nullopt));
    ASSERT_EQ(index_readers.size(), 1u);
    ASSERT_OK_AND_ASSIGN(auto index_result,
                         index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
    ASSERT_EQ(index_result->ToString(), "{0,7}");
}

#ifdef PAIMON_ENABLE_LUMINA
TEST_P(GlobalIndexTest, TestWriteCommitScanReadIndexWithPartition) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::list(arrow::float32())),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    std::map<std::string, std::string> lumina_options = {{"lumina.index.dimension", "4"},
                                                         {"lumina.index.type", "bruteforce"},
                                                         {"lumina.distance.metric", "l2"},
                                                         {"lumina.encoding.type", "rawf32"},
                                                         {"lumina.search.parallel_number", "10"}};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format_},
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

        // write bitmap index
        ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{partition}, "f0", "bitmap",
                             /*options=*/{}, expected_range));
        // write and commit lumina index
        ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{partition}, "f1", "lumina",
                             /*options=*/lumina_options, expected_range));
    };

    // write partition f2 = 10
    auto src_array1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["Alice", [0.0, 0.0, 0.0, 0.0], 10, 11.1],
["Bob", [0.0, 1.0, 0.0, 1.0], 10, 12.1],
["Emily", [1.0, 0.0, 1.0, 0.0], 10, 13.1],
["Tony", [1.0, 1.0, 1.0, 1.0], 10, 14.1]
    ])")
                          .ValueOrDie();
    write_data_and_index(src_array1, {{"f2", "10"}}, Range(0, 3));

    // write partition f2 = 20
    auto src_array2 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["Lucy", [10.0, 10.0, 10.0, 10.0], 20, 15.1],
["Bob", [10.0, 11.0, 10.0, 11.0], 20, 16.1],
["Tony", [11.0, 10.0, 11.0, 10.0], 20, 17.1],
["Alice", [11.0, 11.0, 11.0, 11.0], 20, 18.1],
["Paul", [10.0, 10.0, 10.0, 10.0], 20, 19.1]
    ])")
                          .ValueOrDie();
    write_data_and_index(src_array2, {{"f2", "20"}}, Range(4, 8));

    auto scan_and_check_result = [&](const std::map<std::string, std::string>& partition,
                                     const std::optional<RowRangeIndex>& row_range_index,
                                     VectorSearch::PreFilter filter, int32_t limit,
                                     const std::string& bitmap_result,
                                     const std::string& lumina_result,
                                     const std::shared_ptr<arrow::Array>& expected_array,
                                     const std::map<int64_t, float>& id_to_score) {
        std::vector<std::map<std::string, std::string>> partitions = {partition};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<GlobalIndexScan> global_index_scan,
            GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt, partitions,
                                    lumina_options, fs_, /*executor=*/nullptr, pool_));
        // check bitmap index
        ASSERT_OK_AND_ASSIGN(auto readers, global_index_scan->CreateReaders("f0", row_range_index));
        ASSERT_EQ(readers.size(), 1u);
        ASSERT_OK_AND_ASSIGN(auto result1,
                             readers[0]->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
        ASSERT_OK_AND_ASSIGN(auto result2,
                             readers[0]->VisitEqual(Literal(FieldType::STRING, "Paul", 4)));
        ASSERT_OK_AND_ASSIGN(auto index_result, result1->Or(result2));
        ASSERT_EQ(index_result->ToString(), bitmap_result);

        // check lumina index
        ASSERT_OK_AND_ASSIGN(auto lumina_readers,
                             global_index_scan->CreateReaders("f1", row_range_index));
        ASSERT_EQ(lumina_readers.size(), 1u);
        auto lumina_reader = lumina_readers[0];
        std::vector<float> query = {1.0f, 1.0f, 1.0f, 1.1f};
        auto vector_search = std::make_shared<VectorSearch>(
            "f1", limit, query, filter,
            /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/lumina_options);
        ASSERT_OK_AND_ASSIGN(auto scored_result, lumina_reader->VisitVectorSearch(vector_search));
        ASSERT_EQ(scored_result->ToString(), lumina_result);

        // check read array
        std::vector<std::string> read_field_names = schema->field_names();
        read_field_names.push_back("_INDEX_SCORE");
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, /*predicate=*/nullptr,
                                                               /*options=*/{}, scored_result));
        ASSERT_OK(ReadData(table_path, read_field_names, expected_array,
                           /*predicate=*/nullptr, plan));
    };

    auto result_fields = fields;
    result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
    result_fields.push_back(SpecialFields::IndexScore().ArrowField());
    std::map<int64_t, float> id_to_score1 = {{0, 4.21f}, {1, 2.01f}, {2, 2.21f}, {3, 0.01f}};
    std::map<int64_t, float> id_to_score2 = {
        {0, 322.21f}, {1, 360.01f}, {2, 360.21f}, {3, 398.01}, {4, 322.21f}};

    {
        // test scan and read for f2=10
        auto filter = [](int64_t id) -> bool { return id == 0; };
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", [0.0, 0.0, 0.0, 0.0], 10, 11.1, 4.21]
    ])")
                .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(RowRangeIndex row_range_index, RowRangeIndex::Create({Range(0, 3)}));
        scan_and_check_result({{"f2", "10"}}, row_range_index, filter, /*limit=*/2, "{0}",
                              "row ids: {0}, scores: {4.21}", expected_array, id_to_score1);
    }
    {
        // test scan and read for f2=20
        auto filter = [](int64_t id) -> bool { return id == 3 || id == 4; };
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Paul", [10.0, 10.0, 10.0, 10.0], 20, 19.1, 322.21]
    ])")
                .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(RowRangeIndex row_range_index, RowRangeIndex::Create({Range(4, 8)}));
        scan_and_check_result({{"f2", "20"}}, row_range_index, filter, /*limit=*/1, "{7,8}",
                              "row ids: {8}, scores: {322.21}", expected_array, id_to_score2);
    }
    {
        // test invalid partition input
        ASSERT_NOK_WITH_MSG(
            GlobalIndexScan::Create(
                table_path, /*snapshot_id=*/std::nullopt,
                /*partitions=*/std::vector<std::map<std::string, std::string>>(), lumina_options,
                fs_, /*executor=*/nullptr, pool_),
            "invalid input partition, supposed to be null or at least one partition");
    }
}

TEST_P(GlobalIndexTest, TestWriteCommitScanReadIndexWithScore) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::list(arrow::float32())),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    std::map<std::string, std::string> lumina_options = {{"lumina.index.dimension", "4"},
                                                         {"lumina.index.type", "bruteforce"},
                                                         {"lumina.distance.metric", "l2"},
                                                         {"lumina.encoding.type", "rawf32"},
                                                         {"lumina.search.parallel_number", "10"}};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format_},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};
    CreateTable(/*partition_keys=*/{}, schema, options);

    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    std::vector<std::string> write_cols = schema->field_names();

    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["Alice", [0.0, 0.0, 0.0, 0.0], 10, 11.1],
["Bob", [0.0, 1.0, 0.0, 1.0], 10, 12.1],
["Emily", [1.0, 0.0, 1.0, 0.0], 10, 13.1],
["Tony", [1.0, 1.0, 1.0, 1.0], 10, 14.1],
["Lucy", [10.0, 10.0, 10.0, 10.0], 20, 15.1],
["Bob", [10.0, 11.0, 10.0, 11.0], 20, 16.1],
["Tony", [11.0, 10.0, 11.0, 10.0], 20, 17.1],
["Alice", [11.0, 11.0, 11.0, 11.0], 20, 18.1],
["Paul", [10.0, 10.0, 10.0, 10.0], 20, 19.1]
    ])")
                         .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write and commit lumina index
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f1", "lumina",
                         /*options=*/lumina_options, Range(0, 8)));

    auto scan_and_check_result = [&](const std::vector<Range>& read_row_ranges,
                                     const std::shared_ptr<arrow::Array>& expected_array,
                                     const std::map<int64_t, float>& id_to_score) {
        // check read array
        std::vector<std::string> read_field_names = schema->field_names();
        read_field_names.push_back("_INDEX_SCORE");
        ASSERT_OK_AND_ASSIGN(auto plan,
                             ScanDataWithIndexResult(table_path, read_row_ranges, id_to_score));
        ASSERT_OK(ReadData(table_path, read_field_names, expected_array,
                           /*predicate=*/nullptr, plan));
    };

    auto result_fields = fields;
    result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
    result_fields.push_back(SpecialFields::IndexScore().ArrowField());
    std::map<int64_t, float> id_to_score = {{0, 4.21f},   {1, 2.01f},   {2, 2.21f},
                                            {3, 0.01f},   {4, 322.21f}, {5, 360.01f},
                                            {6, 360.21f}, {7, 398.01},  {8, 322.21f}};
    {
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", [0.0, 0.0, 0.0, 0.0], 10, 11.1, 4.21],
[0, "Bob", [0.0, 1.0, 0.0, 1.0], 10, 12.1, 2.01],
[0, "Emily", [1.0, 0.0, 1.0, 0.0], 10, 13.1, 2.21],
[0, "Tony", [1.0, 1.0, 1.0, 1.0], 10, 14.1, 0.01],
[0, "Lucy", [10.0, 10.0, 10.0, 10.0], 20, 15.1, 322.21],
[0, "Bob", [10.0, 11.0, 10.0, 11.0], 20, 16.1, 360.01],
[0, "Tony", [11.0, 10.0, 11.0, 10.0], 20, 17.1, 360.21],
[0, "Alice", [11.0, 11.0, 11.0, 11.0], 20, 18.1, 398.01],
[0, "Paul", [10.0, 10.0, 10.0, 10.0], 20, 19.1, 322.21]
    ])")
                .ValueOrDie();
        scan_and_check_result({Range(0, 8)}, expected_array, id_to_score);
    }
    {
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Emily", [1.0, 0.0, 1.0, 0.0], 10, 13.1, 2.21],
[0, "Tony", [1.0, 1.0, 1.0, 1.0], 10, 14.1, 0.01],
[0, "Alice", [11.0, 11.0, 11.0, 11.0], 20, 18.1, 398.01],
[0, "Paul", [10.0, 10.0, 10.0, 10.0], 20, 19.1, 322.21]
    ])")
                .ValueOrDie();
        scan_and_check_result({Range(2, 3), Range(7, 8)}, expected_array, id_to_score);
    }
    {
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Bob", [10.0, 11.0, 10.0, 11.0], 20, 16.1, 360.01]
    ])")
                .ValueOrDie();
        scan_and_check_result({Range(5, 5)}, expected_array, id_to_score);
    }
    {
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Emily", [1.0, 0.0, 1.0, 0.0], 10, 13.1, null],
[0, "Tony", [1.0, 1.0, 1.0, 1.0], 10, 14.1, null],
[0, "Alice", [11.0, 11.0, 11.0, 11.0], 20, 18.1, null],
[0, "Paul", [10.0, 10.0, 10.0, 10.0], 20, 19.1, null]
    ])")
                .ValueOrDie();
        scan_and_check_result({Range(2, 3), Range(7, 8)}, expected_array, /*id_to_score=*/{});
    }
}
#endif

TEST_P(GlobalIndexTest, TestDataEvolutionBatchScan) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);
    // write and commit data
    std::vector<std::string> write_cols = schema->field_names();
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Emily", 10, 0, 13.1],
["Tony", 10, 0, 14.1],
["Lucy", 20, 1, 15.1],
["Bob", 10, 1, 16.1],
["Tony", 20, 0, 17.1],
["Alice", 20, null, 18.1]
    ])")
                         .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    auto result_fields = fields_;
    result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
    auto expected_all_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Lucy", 20, 1, 15.1],
[0, "Bob", 10, 1, 16.1],
[0, "Tony", 20, 0, 17.1],
[0, "Alice", 20, null, 18.1]
    ])")
            .ValueOrDie();

    {
        // read when no index is built
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        ASSERT_OK(ReadData(table_path, write_cols, expected_all_array, predicate, plan));
    }

    // write and commit global index
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f0", "bitmap", /*options=*/{},
                         Range(0, 7)));

    // scan and read with global index
    {
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));

        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1],
[0, "Alice", 20, null, 18.1]
    ])")
                .ValueOrDie();
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice2", 6));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));

        ASSERT_OK(ReadData(table_path, write_cols, /*expected_array=*/nullptr, predicate, plan));
    }
    {
        auto predicate1 =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto predicate2 =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Bob", 3));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({predicate1, predicate2}));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));

        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Bob", 10, 1, 16.1],
[0, "Alice", 20, null, 18.1]
    ])")
                .ValueOrDie();
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, /*predicate=*/nullptr));
        ASSERT_OK(
            ReadData(table_path, write_cols, expected_all_array, /*predicate=*/nullptr, plan));
    }
    {
        // f1 does not have global index
        auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                 FieldType::INT, Literal(19));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        ASSERT_OK(ReadData(table_path, write_cols, expected_all_array, predicate, plan));
    }
    {
        // disable global index
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(
            auto plan,
            ScanGlobalIndexAndData(table_path, predicate, {{"global-index.enabled", "false"}}));
        ASSERT_OK(ReadData(table_path, write_cols, expected_all_array, predicate, plan));
    }
}

TEST_P(GlobalIndexTest, TestDataEvolutionBatchScanWithOnlyOnePartitionHasIndex) {
    CreateTable(/*partition_keys=*/{"f1"});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);
    // write and commit data
    std::vector<std::string> write_cols = schema->field_names();
    auto src_array1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Emily", 10, 0, 13.1],
["Tony", 10, 0, 14.1],
["Bob", 10, 1, 16.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         WriteArray(table_path, {{"f1", "10"}}, write_cols, src_array1));
    ASSERT_OK(Commit(table_path, commit_msgs1));

    auto src_array2 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Lucy", 20, 1, 15.1],
["Tony", 20, 0, 17.1],
["Alice", 20, null, 18.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2,
                         WriteArray(table_path, {{"f1", "20"}}, write_cols, src_array2));
    ASSERT_OK(Commit(table_path, commit_msgs2));

    // write and commit global index for f1 = 10
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{{{"f1", "10"}}}, "f0", "bitmap",
                         /*options=*/{}, Range(0, 4)));

    auto result_fields = fields_;
    result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
    {
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1]
    ])")
                .ValueOrDie();

        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
}

TEST_P(GlobalIndexTest, TestDataEvolutionBatchScanWithTwoIndexInDiffTwoPartition) {
    CreateTable(/*partition_keys=*/{"f1"});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);
    // write and commit data
    std::vector<std::string> write_cols = schema->field_names();
    auto src_array1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Emily", 10, 0, 13.1],
["Tony", 10, 0, 14.1],
["Bob", 10, 1, 16.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         WriteArray(table_path, {{"f1", "10"}}, write_cols, src_array1));
    ASSERT_OK(Commit(table_path, commit_msgs1));

    auto src_array2 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Lucy", 20, 1, 15.1],
["Tony", 20, 0, 17.1],
["Alice", 20, null, 18.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2,
                         WriteArray(table_path, {{"f1", "20"}}, write_cols, src_array2));
    ASSERT_OK(Commit(table_path, commit_msgs2));

    // write and commit global index for f1 = 10
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{{{"f1", "10"}}}, "f0", "bitmap",
                         /*options=*/{}, Range(0, 4)));

    // write and commit global index for f1 = 20
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{{{"f1", "20"}}}, "f2", "bitmap",
                         /*options=*/{}, Range(5, 7)));

    auto result_fields = fields_;
    result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
    {
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1]
    ])")
                .ValueOrDie();

        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(1));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Lucy", 20, 1, 15.1]
    ])")
                .ValueOrDie();

        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        // only f1 = 10 partition has f0 index, query predicate1 results in ["Alice", 10, 1, 11.1]
        // only f2 = 20 partition has f2 index, query predicate2 results in ["Lucy", 20, 1, 15.1]
        auto predicate1 =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto predicate2 = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                  FieldType::INT, Literal(1));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({predicate1, predicate2}));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        ASSERT_OK(ReadData(table_path, write_cols, /*expected_array=*/nullptr, predicate, plan));
    }
    {
        // predicate2 is partition filter
        auto predicate1 =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto predicate2 = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                  FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({predicate1, predicate2}));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1]
    ])")
                .ValueOrDie();

        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
}

TEST_P(GlobalIndexTest, TestDataEvolutionBatchScanWithTwoPartitionAllWithIndex) {
    CreateTable(/*partition_keys=*/{"f1"});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);
    // write and commit data
    std::vector<std::string> write_cols = schema->field_names();
    auto src_array1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Emily", 10, 0, 13.1],
["Tony", 10, 0, 14.1],
["Bob", 10, 1, 16.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         WriteArray(table_path, {{"f1", "10"}}, write_cols, src_array1));
    ASSERT_OK(Commit(table_path, commit_msgs1));

    auto src_array2 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Lucy", 20, 1, 15.1],
["Tony", 20, 0, 17.1],
["Alice", 20, null, 18.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2,
                         WriteArray(table_path, {{"f1", "20"}}, write_cols, src_array2));
    ASSERT_OK(Commit(table_path, commit_msgs2));

    // write and commit global index for f1 = 10
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{{{"f1", "10"}}}, "f0", "bitmap",
                         /*options=*/{}, Range(0, 4)));

    // write and commit global index for f1 = 20
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{{{"f1", "20"}}}, "f0", "bitmap",
                         /*options=*/{}, Range(5, 7)));

    auto result_fields = fields_;
    result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
    {
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1],
[0, "Alice", 20, null, 18.1]
    ])")
                .ValueOrDie();

        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        // predicate2 is partition filter
        auto predicate1 =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto predicate2 = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                  FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({predicate1, predicate2}));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1]
    ])")
                .ValueOrDie();

        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        // predicate2 is partition filter
        auto predicate1 =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto predicate2 = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                     FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({predicate1, predicate2}));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1]
    ])")
                .ValueOrDie();

        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        // predicate2 is partition filter
        auto predicate1 =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto predicate2 = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                     FieldType::INT, Literal(30));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({predicate1, predicate2}));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1],
[0, "Alice", 20, null, 18.1]
    ])")
                .ValueOrDie();

        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
}

TEST_P(GlobalIndexTest, TestDataEvolutionBatchScanWithPartitionWithTwoFields) {
    CreateTable(/*partition_keys=*/{"f1", "f2"});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);
    // write and commit data
    std::vector<std::string> write_cols = schema->field_names();

    auto src_array1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Bob", 10, 1, 16.1]
   ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, {{"f1", "10"}, {"f2", "1"}},
                                                       write_cols, src_array1));
    ASSERT_OK(Commit(table_path, commit_msgs1));

    auto src_array2 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Lucy", 20, 1, 15.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, {{"f1", "20"}, {"f2", "1"}},
                                                       write_cols, src_array2));
    ASSERT_OK(Commit(table_path, commit_msgs2));

    auto src_array3 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Emily", 10, 0, 13.1],
["Tony", 10, 0, 14.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, WriteArray(table_path, {{"f1", "10"}, {"f2", "0"}},
                                                       write_cols, src_array3));
    ASSERT_OK(Commit(table_path, commit_msgs3));

    // build index
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{{{"f1", "10"}, {"f2", "1"}}}, "f0",
                         "bitmap",
                         /*options=*/{}, Range(0, 2)));

    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{{{"f1", "20"}, {"f2", "1"}}}, "f0",
                         "bitmap",
                         /*options=*/{}, Range(3, 3)));

    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{{{"f1", "10"}, {"f2", "0"}}}, "f0",
                         "bitmap",
                         /*options=*/{}, Range(4, 5)));

    auto result_fields = fields_;
    result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
    {
        auto predicate1 = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                  FieldType::INT, Literal(10));
        auto predicate2 =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Bob", 3));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({predicate1, predicate2}));

        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Bob", 10, 1, 12.1],
[0, "Bob", 10, 1, 16.1]
    ])")
                .ValueOrDie();
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        auto predicate1 = PredicateBuilder::GreaterThan(
            /*field_index=*/1, /*field_name=*/"f1", FieldType::INT, Literal(10));
        auto predicate2 = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                  FieldType::INT, Literal(1));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({predicate1, predicate2}));

        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Bob", 10, 1, 16.1],
[0, "Lucy", 20, 1, 15.1]
    ])")
                .ValueOrDie();
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        auto predicate1 = PredicateBuilder::GreaterThan(
            /*field_index=*/1, /*field_name=*/"f1", FieldType::INT, Literal(10));
        auto predicate2 = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                  FieldType::INT, Literal(1));
        ASSERT_OK_AND_ASSIGN(auto or_predicate, PredicateBuilder::Or({predicate1, predicate2}));

        auto predicate3 =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Emily", 5));
        ASSERT_OK_AND_ASSIGN(auto and_predicate, PredicateBuilder::And({predicate3, or_predicate}));

        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, and_predicate));
        ASSERT_OK(
            ReadData(table_path, write_cols, /*expected_array=*/nullptr, and_predicate, plan));
    }
}

#ifdef PAIMON_ENABLE_LUMINA
TEST_P(GlobalIndexTest, TestScanIndexWithTwoIndexes) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::list(arrow::float32())),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    std::map<std::string, std::string> lumina_options = {{"lumina.index.dimension", "4"},
                                                         {"lumina.index.type", "bruteforce"},
                                                         {"lumina.distance.metric", "l2"},
                                                         {"lumina.encoding.type", "rawf32"},
                                                         {"lumina.search.parallel_number", "10"}};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format_},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};
    CreateTable(/*partition_keys=*/{}, schema, options);

    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    std::vector<std::string> write_cols = schema->field_names();

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["Alice", [0.0, 0.0, 0.0, 0.0], 10, 11.1],
["Bob", [0.0, 1.0, 0.0, 1.0], 10, 12.1],
["Emily", [1.0, 0.0, 1.0, 0.0], 10, 13.1],
["Tony", [1.0, 1.0, 1.0, 1.0], 10, 14.1],
["Lucy", [10.0, 10.0, 10.0, 10.0], 20, 15.1],
["Bob", [10.0, 11.0, 10.0, 11.0], 20, 16.1],
["Tony", [11.0, 10.0, 11.0, 10.0], 20, 17.1],
["Alice", [11.0, 11.0, 11.0, 11.0], 20, 18.1],
["Paul", [10.0, 10.0, 10.0, 10.0], 20, 19.1]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write and commit bitmap global index
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f0", "bitmap",
                         /*options=*/{}, Range(0, 8)));

    // write and commit lumina global index
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f1", "lumina",
                         /*options=*/lumina_options, Range(0, 8)));

    ASSERT_OK_AND_ASSIGN(
        auto global_index_scan,
        GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                /*partitions=*/std::nullopt,
                                /*options=*/lumina_options, fs_, /*executor=*/nullptr, pool_));
    // query f0
    ASSERT_OK_AND_ASSIGN(auto index_readers, global_index_scan->CreateReaders("f0", std::nullopt));
    ASSERT_EQ(index_readers.size(), 1);
    ASSERT_OK_AND_ASSIGN(auto index_result,
                         index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
    ASSERT_EQ(index_result->ToString(), "{0,7}");

    // query f1
    ASSERT_OK_AND_ASSIGN(index_readers, global_index_scan->CreateReaders("f1", std::nullopt));
    ASSERT_EQ(index_readers.size(), 1);
    std::vector<float> query = {11.0f, 11.0f, 11.0f, 11.0f};
    ASSERT_OK_AND_ASSIGN(
        auto scored_result,
        index_readers[0]->VisitVectorSearch(std::make_shared<VectorSearch>(
            "f1", 1, query, /*filter=*/nullptr,
            /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/lumina_options)));
    ASSERT_EQ(scored_result->ToString(), "row ids: {7}, scores: {0.00}");

    // query f2
    ASSERT_OK_AND_ASSIGN(index_readers, global_index_scan->CreateReaders("f2", std::nullopt));
    ASSERT_EQ(index_readers.size(), 0);
}
#endif

TEST_P(GlobalIndexTest, TestDataEvolutionBatchScanWithExternalPath) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::list(arrow::float32())),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format_},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};
    CreateTable(/*partition_keys=*/{}, schema, options);

    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    std::vector<std::string> write_cols = schema->field_names();

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["Alice", [0.0, 0.0, 0.0, 0.0], 10, 11.1],
["Bob", [0.0, 1.0, 0.0, 1.0], 10, 12.1],
["Emily", [1.0, 0.0, 1.0, 0.0], 10, 13.1],
["Tony", [1.0, 1.0, 1.0, 1.0], 10, 14.1],
["Lucy", [10.0, 10.0, 10.0, 10.0], 20, 15.1],
["Bob", [10.0, 11.0, 10.0, 11.0], 20, 16.1],
["Tony", [11.0, 10.0, 11.0, 10.0], 20, 17.1],
["Alice", [11.0, 11.0, 11.0, 11.0], 20, 18.1],
["Paul", [10.0, 10.0, 10.0, 10.0], 20, 19.1]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write and commit bitmap global index
    auto external_dir1 = UniqueTestDirectory::Create("local");
    std::map<std::string, std::string> index_options = {
        {"global-index.external-path", "FILE://" + external_dir1->Str()}};
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f0", "bitmap", index_options,
                         Range(0, 8)));

    auto result_fields = fields;
    result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());

    // test scan and read
    auto predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                Literal(FieldType::STRING, "Alice", 5));
    ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate, index_options));

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", [0.0, 0.0, 0.0, 0.0], 10, 11.1],
[0, "Alice", [11.0, 11.0, 11.0, 11.0], 20, 18.1]
    ])")
            .ValueOrDie();

    ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
}

TEST_P(GlobalIndexTest, TestIOException) {
    if (file_format_ == "lance") {
        return;
    }
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::list(arrow::float32())),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};

    auto schema = arrow::schema(fields);
    std::vector<std::string> write_cols = schema->field_names();
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["Alice", [0.0, 0.0, 0.0, 0.0], 10, 11.1],
["Bob", [0.0, 1.0, 0.0, 1.0], 10, 12.1],
["Alice", [1.0, 0.0, 1.0, 0.0], 10, 13.1],
["Tony", [1.0, 1.0, 1.0, 1.0], 10, 14.1]
    ])")
                         .ValueOrDie();

    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format_},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};
    std::string table_path;
    bool write_run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 2000; i += paimon::test::RandomNumber(20, 30)) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        dir_ = UniqueTestDirectory::Create("local");
        // create table and write data
        CreateTable(/*partition_keys=*/{}, schema, options);
        table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
        ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
        ASSERT_OK(Commit(table_path, commit_msgs));

        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        // write bitmap index
        auto bitmap_index_write_status =
            WriteIndex(table_path, /*partition_filters=*/{}, "f0", "bitmap",
                       /*options=*/{}, Range(0, 3));
        CHECK_HOOK_STATUS(bitmap_index_write_status, i);
        write_run_complete = true;
        break;
    }
    ASSERT_TRUE(write_run_complete);

    // read for bitmap
    bool read_run_complete = false;
    for (size_t i = 0; i < 2000; i += paimon::test::RandomNumber(20, 30)) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);

        auto result_fields = fields;
        result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());

        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", [0.0, 0.0, 0.0, 0.0], 10, 11.1],
[0, "Alice", [1.0, 0.0, 1.0, 0.0], 10, 13.1]
    ])")
                .ValueOrDie();

        auto plan_result = ScanGlobalIndexAndData(table_path, predicate);
        CHECK_HOOK_STATUS_WITHOUT_MESSAGE_CHECK(plan_result.status());
        auto plan = std::move(plan_result).value();
        auto read_status = ReadData(table_path, write_cols, expected_array, predicate, plan);
        CHECK_HOOK_STATUS(read_status, i);
        read_run_complete = true;
        break;
    }
    ASSERT_TRUE(read_run_complete);
}

TEST_P(GlobalIndexTest, TestDataEvolutionBatchScanWithRangeBitmap) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);
    std::vector<std::string> write_cols = schema->field_names();
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Emily", 15, 0, 13.1],
["Tony", 20, 0, 14.1],
["Lucy", 20, 1, 15.1],
["Bob", 25, 1, 16.1],
["Tony", 30, 0, 17.1],
["Alice", 30, null, 18.1]
    ])")
                         .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    auto result_fields = fields_;
    result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
    auto expected_all_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Emily", 15, 0, 13.1],
[0, "Tony", 20, 0, 14.1],
[0, "Lucy", 20, 1, 15.1],
[0, "Bob", 25, 1, 16.1],
[0, "Tony", 30, 0, 17.1],
[0, "Alice", 30, null, 18.1]
    ])")
            .ValueOrDie();

    {
        // read when no index is built
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        ASSERT_OK(ReadData(table_path, write_cols, expected_all_array, predicate, plan));
    }

    // write and commit range-bitmap global index on f1 (int32)
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f1", "range-bitmap", /*options=*/{},
                         Range(0, 7)));

    // scan and read with range-bitmap global index
    {
        // f1 < 20: rows with f1=10,10,15 -> Alice(10), Bob(10), Emily(15)
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));

        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Emily", 15, 0, 13.1]
    ])")
                .ValueOrDie();
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        // f1 >= 25: rows with f1=25,30,30 -> Bob(25), Tony(30), Alice(30)
        auto predicate = PredicateBuilder::GreaterOrEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                          FieldType::INT, Literal(25));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));

        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Bob", 25, 1, 16.1],
[0, "Tony", 30, 0, 17.1],
[0, "Alice", 30, null, 18.1]
    ])")
                .ValueOrDie();
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        // f1 > 30: no rows match
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::INT, Literal(30));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));

        ASSERT_OK(ReadData(table_path, write_cols, /*expected_array=*/nullptr, predicate, plan));
    }
    {
        // f1 <= 10: rows with f1=10,10 -> Alice(10), Bob(10)
        auto predicate = PredicateBuilder::LessOrEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));

        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1]
    ])")
                .ValueOrDie();
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        // no predicate: return all rows
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, /*predicate=*/nullptr));
        ASSERT_OK(
            ReadData(table_path, write_cols, expected_all_array, /*predicate=*/nullptr, plan));
    }
    {
        // f0 does not have range-bitmap index, should return all rows
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));
        ASSERT_OK(ReadData(table_path, write_cols, expected_all_array, predicate, plan));
    }
}

TEST_P(GlobalIndexTest, TestDataEvolutionBatchScanWithRangeBitmapAndBitmap) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);
    std::vector<std::string> write_cols = schema->field_names();
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Emily", 15, 0, 13.1],
["Tony", 20, 0, 14.1],
["Lucy", 20, 1, 15.1],
["Bob", 25, 1, 16.1],
["Tony", 30, 0, 17.1],
["Alice", 30, null, 18.1]
    ])")
                         .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    auto result_fields = fields_;
    result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
    auto expected_all_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Emily", 15, 0, 13.1],
[0, "Tony", 20, 0, 14.1],
[0, "Lucy", 20, 1, 15.1],
[0, "Bob", 25, 1, 16.1],
[0, "Tony", 30, 0, 17.1],
[0, "Alice", 30, null, 18.1]
    ])")
            .ValueOrDie();

    // write and commit bitmap global index on f0 (string)
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f0", "bitmap", /*options=*/{},
                         Range(0, 7)));

    // write and commit range-bitmap global index on f1 (int32)
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f1", "range-bitmap", /*options=*/{},
                         Range(0, 7)));

    // scan and read with both indexes
    {
        // bitmap: f0 == "Alice" AND range-bitmap: f1 < 20
        // Alice has f1=10 and f1=30, only f1=10 < 20 -> Alice(10)
        auto predicate1 =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto predicate2 = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                     FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({predicate1, predicate2}));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));

        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1]
    ])")
                .ValueOrDie();
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        // bitmap: f0 == "Bob" AND range-bitmap: f1 >= 20
        // Bob has f1=10 and f1=25, only f1=25 >= 20 -> Bob(25)
        auto predicate1 =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Bob", 3));
        auto predicate2 = PredicateBuilder::GreaterOrEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({predicate1, predicate2}));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));

        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Bob", 25, 1, 16.1]
    ])")
                .ValueOrDie();
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        // bitmap: (f0 == "Alice" OR f0 == "Tony") AND range-bitmap: f1 > 20
        // Alice: f1=10,30 -> f1=30 > 20; Tony: f1=20,30 -> f1=30 > 20
        auto predicate_alice =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        auto predicate_tony =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Tony", 4));
        ASSERT_OK_AND_ASSIGN(auto or_predicate,
                             PredicateBuilder::Or({predicate_alice, predicate_tony}));
        auto range_predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                             FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate,
                             PredicateBuilder::And({or_predicate, range_predicate}));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));

        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Tony", 30, 0, 17.1],
[0, "Alice", 30, null, 18.1]
    ])")
                .ValueOrDie();
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        // only bitmap predicate: f0 == "Emily"
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Emily", 5));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));

        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Emily", 15, 0, 13.1]
    ])")
                .ValueOrDie();
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        // only range-bitmap predicate: f1 <= 15
        auto predicate = PredicateBuilder::LessOrEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::INT, Literal(15));
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, predicate));

        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1],
[0, "Bob", 10, 1, 12.1],
[0, "Emily", 15, 0, 13.1]
    ])")
                .ValueOrDie();
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, predicate, plan));
    }
    {
        // no predicate: return all rows
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, /*predicate=*/nullptr));
        ASSERT_OK(
            ReadData(table_path, write_cols, expected_all_array, /*predicate=*/nullptr, plan));
    }
}

#ifdef PAIMON_ENABLE_LUCENE
TEST_P(GlobalIndexTest, TestLuceneWriteCommitScanReadIndexWithScore) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32())};
    auto tmp_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(tmp_dir);
    std::map<std::string, std::string> lucene_options = {
        {"lucene-fts.write.omit-term-freq-and-position", "false"},
        {"lucene-fts.write.tmp.directory", tmp_dir->Str()}};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format_},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};
    CreateTable(/*partition_keys=*/{}, schema, options);

    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    std::vector<std::string> write_cols = schema->field_names();

    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["This is an test document.", 0],
["This is an new document document document.", 1],
["Document document document document test.", 2],
["unordered user-defined doc id", 3]
    ])")
                         .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write and commit lucene index
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f0", "lucene-fts",
                         /*options=*/lucene_options, Range(0, 3)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexScan> global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                 /*partitions=*/std::nullopt, /*options=*/{}, fs_,
                                                 /*executor=*/nullptr, pool_));
    // test f0 field
    ASSERT_OK_AND_ASSIGN(auto index_readers,
                         global_index_scan->CreateReaders("f0", /*row_range_index=*/std::nullopt));
    ASSERT_EQ(index_readers.size(), 1u);
    auto index_reader = index_readers[0];
    {
        ASSERT_OK_AND_ASSIGN(auto index_result,
                             index_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "document", FullTextSearch::SearchType::MATCH_ALL,
                                 /*pre_filter=*/std::nullopt)));
        ASSERT_TRUE(index_result->ToString().find("row ids: {0,1,2}") != std::string::npos);
    }
    {
        std::optional<RoaringBitmap64> pre_filter = RoaringBitmap64::From({1, 2, 3});
        ASSERT_OK_AND_ASSIGN(
            auto index_result,
            index_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                "f0",
                /*limit=*/10, "document", FullTextSearch::SearchType::MATCH_ALL, pre_filter)));
        ASSERT_TRUE(index_result->ToString().find("row ids: {1,2}") != std::string::npos);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto index_result,
                             index_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                                 "f0",
                                 /*limit=*/10, "*or*er*", FullTextSearch::SearchType::WILDCARD,
                                 /*pre_filter=*/std::nullopt)));
        ASSERT_TRUE(index_result->ToString().find("row ids: {3}") != std::string::npos);
    }
}
#endif

TEST_P(GlobalIndexTest, TestBTreeWriteCommitScanReadIndex) {
    // BTreeGlobalIndexWriter requires keys to be written in monotonically increasing order.
    // Therefore the source data must be pre-sorted by the indexed column (f0, string).
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    std::vector<std::string> write_cols = schema->field_names();

    // Data sorted by f0 (string, ascending): Alice < Bob < Bob < Emily < Lucy < Tony < Tony
    // The last row has f0=null which is treated separately by the null bitmap.
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Bob", 20, 0, 16.1],
["Emily", 10, 0, 13.1],
["Lucy", 20, 1, 15.1],
["Tony", 10, 0, 14.1],
["Tony", 20, 0, 17.1],
[null, 20, null, 18.1]
    ])")
                         .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // Write btree-global index on f0
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f0", "btree",
                         /*options=*/{}, Range(0, 7)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexScan> global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                 /*partitions=*/std::nullopt, /*options=*/{}, fs_,
                                                 /*executor=*/nullptr, pool_));
    ASSERT_OK_AND_ASSIGN(auto index_readers,
                         global_index_scan->CreateReaders("f0", /*row_range_index=*/std::nullopt));
    ASSERT_EQ(index_readers.size(), 1u);
    auto index_reader = index_readers[0];

    {
        // VisitEqual: "Alice" -> row 0
        ASSERT_OK_AND_ASSIGN(auto result,
                             index_reader->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{0}");
    }
    {
        // VisitEqual: "Bob" -> rows 1,2
        ASSERT_OK_AND_ASSIGN(auto result,
                             index_reader->VisitEqual(Literal(FieldType::STRING, "Bob", 3)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{1,2}");
    }
    {
        // VisitEqual: non-existent key -> empty
        ASSERT_OK_AND_ASSIGN(auto result,
                             index_reader->VisitEqual(Literal(FieldType::STRING, "Zara", 4)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{}");
    }
    {
        // VisitNotEqual: "Bob" -> all non-null except Bob rows -> {0,3,4,5,6}
        ASSERT_OK_AND_ASSIGN(auto result,
                             index_reader->VisitNotEqual(Literal(FieldType::STRING, "Bob", 3)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{0,3,4,5,6}");
    }
    {
        // VisitIsNull -> row 7 (null key)
        ASSERT_OK_AND_ASSIGN(auto result, index_reader->VisitIsNull());
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{7}");
    }
    {
        // VisitIsNotNull -> rows 0-6
        ASSERT_OK_AND_ASSIGN(auto result, index_reader->VisitIsNotNull());
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{0,1,2,3,4,5,6}");
    }
    {
        // VisitIn: {"Alice", "Lucy"} -> rows {0, 4}
        ASSERT_OK_AND_ASSIGN(auto result,
                             index_reader->VisitIn({Literal(FieldType::STRING, "Alice", 5),
                                                    Literal(FieldType::STRING, "Lucy", 4)}));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{0,4}");
    }
    {
        // VisitNotIn: {"Alice", "Lucy"} -> all non-null except {0,4} -> {1,2,3,5,6}
        ASSERT_OK_AND_ASSIGN(auto result,
                             index_reader->VisitNotIn({Literal(FieldType::STRING, "Alice", 5),
                                                       Literal(FieldType::STRING, "Lucy", 4)}));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{1,2,3,5,6}");
    }
    {
        // VisitLessThan: "Emily" -> keys < "Emily" -> Alice(0), Bob(1,2) -> {0,1,2}
        ASSERT_OK_AND_ASSIGN(auto result,
                             index_reader->VisitLessThan(Literal(FieldType::STRING, "Emily", 5)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{0,1,2}");
    }
    {
        // VisitLessOrEqual: "Emily" -> keys <= "Emily" -> Alice(0), Bob(1,2), Emily(3) ->
        // {0,1,2,3}
        ASSERT_OK_AND_ASSIGN(
            auto result, index_reader->VisitLessOrEqual(Literal(FieldType::STRING, "Emily", 5)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{0,1,2,3}");
    }
    {
        // VisitGreaterThan: "Emily" -> keys > "Emily" -> Lucy(4), Tony(5,6) -> {4,5,6}
        ASSERT_OK_AND_ASSIGN(
            auto result, index_reader->VisitGreaterThan(Literal(FieldType::STRING, "Emily", 5)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{4,5,6}");
    }
    {
        // VisitGreaterOrEqual: "Emily" -> keys >= "Emily" -> Emily(3), Lucy(4), Tony(5,6) ->
        // {3,4,5,6}
        ASSERT_OK_AND_ASSIGN(
            auto result, index_reader->VisitGreaterOrEqual(Literal(FieldType::STRING, "Emily", 5)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{3,4,5,6}");
    }

    auto scan_impl = std::dynamic_pointer_cast<GlobalIndexScanImpl>(global_index_scan);
    ASSERT_TRUE(scan_impl);
    {
        // Equal predicate via evaluator
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Tony", 4));
        ASSERT_OK_AND_ASSIGN(auto result, scan_impl->Scan(predicate));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{5,6}");
    }
    {
        // AND predicate: f0 == "Bob" AND f1 == 20
        // f0 == "Bob" -> {1,2}, but f1 index does not exist -> AND yields {1,2}
        // (fields without index return nullptr, AND with nullptr keeps the other side)
        auto f0_predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Bob", 3));
        auto f1_predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({f0_predicate, f1_predicate}));
        ASSERT_OK_AND_ASSIGN(auto result, scan_impl->Scan(predicate));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{1,2}");
    }
    {
        // row_range_index filtering: range [0,2] should only load that range
        ASSERT_OK_AND_ASSIGN(RowRangeIndex row_range_index, RowRangeIndex::Create({Range(0, 2)}));
        ASSERT_OK_AND_ASSIGN(auto range_readers,
                             global_index_scan->CreateReaders("f0", row_range_index));
        ASSERT_EQ(range_readers.size(), 1u);
        ASSERT_OK_AND_ASSIGN(auto result,
                             range_readers[0]->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{0}");
    }
    {
        // Invalid row_range_index: no intersection -> empty readers
        ASSERT_OK_AND_ASSIGN(RowRangeIndex row_range_index,
                             RowRangeIndex::Create({Range(100, 200)}));
        ASSERT_OK_AND_ASSIGN(auto range_readers,
                             global_index_scan->CreateReaders("f0", row_range_index));
        ASSERT_EQ(range_readers.size(), 0u);
    }

    // Test full pipeline: scan with predicate -> read data
    {
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Bob", 3));
        auto scan_impl = std::dynamic_pointer_cast<GlobalIndexScanImpl>(global_index_scan);
        ASSERT_OK_AND_ASSIGN(auto index_result, scan_impl->Scan(predicate));
        ASSERT_TRUE(index_result);
        ASSERT_EQ(index_result->ToString(), "{1,2}");

        auto result_fields = fields_;
        result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Bob", 10, 1, 12.1],
[0, "Bob", 20, 0, 16.1]
    ])")
                .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, /*predicate=*/nullptr,
                                                               /*options=*/{}, index_result));
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, /*predicate=*/nullptr, plan));
    }
}

TEST_P(GlobalIndexTest, TestBTreeWriteCommitScanReadIndexWithPartition) {
    // BTree index with partitioned table. Each partition's data is sorted by f0 independently.
    auto schema = arrow::schema(fields_);
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format_},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};
    CreateTable(/*partition_keys=*/{"f1"}, schema, options);

    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    std::vector<std::string> write_cols = schema->field_names();

    // Write partition f1=10. Data sorted by f0: Alice < Bob < Bob < Emily < Tony
    auto src_array1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Bob", 10, 0, 13.1],
["Emily", 10, 0, 14.1],
["Tony", 10, 1, 15.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         WriteArray(table_path, {{"f1", "10"}}, write_cols, src_array1));
    ASSERT_OK(Commit(table_path, commit_msgs1));
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{{{"f1", "10"}}}, "f0", "btree",
                         /*options=*/{}, Range(0, 4)));

    // Write partition f1=20. Data sorted by f0: Alice < Lucy < Tony
    auto src_array2 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 20, null, 16.1],
["Lucy", 20, 1, 17.1],
["Tony", 20, 0, 18.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2,
                         WriteArray(table_path, {{"f1", "20"}}, write_cols, src_array2));
    ASSERT_OK(Commit(table_path, commit_msgs2));
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{{{"f1", "20"}}}, "f0", "btree",
                         /*options=*/{}, Range(5, 7)));

    // Scan all partitions
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexScan> global_index_scan,
                             GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                     /*partitions=*/std::nullopt, /*options=*/{},
                                                     fs_, /*executor=*/nullptr, pool_));
        ASSERT_OK_AND_ASSIGN(auto index_readers,
                             global_index_scan->CreateReaders("f0", std::nullopt));
        // One reader per partition range -> 2 ranges -> UnionGlobalIndexReader wraps them
        ASSERT_EQ(index_readers.size(), 1u);

        // "Alice" exists in both partitions: local ids {0} in range [0,4] -> global 0,
        // and local ids {0} in range [5,7] -> global 5
        ASSERT_OK_AND_ASSIGN(auto result,
                             index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{0,5}");

        // "Bob" only in partition f1=10: local ids {1,2} -> global {1,2}
        ASSERT_OK_AND_ASSIGN(auto result2,
                             index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Bob", 3)));
        ASSERT_TRUE(result2);
        ASSERT_EQ(result2->ToString(), "{1,2}");

        // "Lucy" only in partition f1=20: local ids {1} -> global {6}
        ASSERT_OK_AND_ASSIGN(auto result3,
                             index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Lucy", 4)));
        ASSERT_TRUE(result3);
        ASSERT_EQ(result3->ToString(), "{6}");
    }

    // Scan with partition filter: only f1=10
    {
        std::vector<std::map<std::string, std::string>> partitions = {{{"f1", "10"}}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<GlobalIndexScan> global_index_scan,
            GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt, partitions,
                                    /*options=*/{}, fs_, /*executor=*/nullptr, pool_));
        ASSERT_OK_AND_ASSIGN(auto index_readers,
                             global_index_scan->CreateReaders("f0", std::nullopt));
        ASSERT_EQ(index_readers.size(), 1u);

        // "Alice" in f1=10 only -> global {0}
        ASSERT_OK_AND_ASSIGN(auto result,
                             index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{0}");

        // "Lucy" not in f1=10 -> empty
        ASSERT_OK_AND_ASSIGN(auto result2,
                             index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Lucy", 4)));
        ASSERT_TRUE(result2);
        ASSERT_EQ(result2->ToString(), "{}");
    }

    // Scan with row_range_index filtering: only range [5,7] (partition f1=20)
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexScan> global_index_scan,
                             GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                     /*partitions=*/std::nullopt, /*options=*/{},
                                                     fs_, /*executor=*/nullptr, pool_));
        ASSERT_OK_AND_ASSIGN(RowRangeIndex row_range_index, RowRangeIndex::Create({Range(5, 7)}));
        ASSERT_OK_AND_ASSIGN(auto index_readers,
                             global_index_scan->CreateReaders("f0", row_range_index));
        ASSERT_EQ(index_readers.size(), 1u);

        // "Tony" in range [5,7]: local id {2} in range [5,7] -> global {7}
        ASSERT_OK_AND_ASSIGN(auto result,
                             index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Tony", 4)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{7}");
    }

    // Full pipeline with evaluator: Scan(predicate) -> read data
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexScan> global_index_scan,
                             GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                     /*partitions=*/std::nullopt, /*options=*/{},
                                                     fs_, /*executor=*/nullptr, pool_));
        auto scanner_impl = std::dynamic_pointer_cast<GlobalIndexScanImpl>(global_index_scan);
        ASSERT_TRUE(scanner_impl);

        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Tony", 4));
        ASSERT_OK_AND_ASSIGN(auto index_result, scanner_impl->Scan(predicate));
        ASSERT_TRUE(index_result);
        ASSERT_EQ(index_result->ToString(), "{4,7}");

        auto result_fields = fields_;
        result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Tony", 10, 1, 15.1],
[0, "Tony", 20, 0, 18.1]
    ])")
                .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, /*predicate=*/nullptr,
                                                               /*options=*/{}, index_result));
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, /*predicate=*/nullptr, plan));
    }
}

TEST_P(GlobalIndexTest, TestBTreeWithPartitionAndCustomExecutor) {
    // Test that UnionGlobalIndexReader uses a custom 8-thread executor to read
    // btree indexes from two partitions in parallel.
    auto schema = arrow::schema(fields_);
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format_},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};
    CreateTable(/*partition_keys=*/{"f1"}, schema, options);

    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    std::vector<std::string> write_cols = schema->field_names();

    // Write partition f1=10 (5 rows, sorted by f0)
    auto src_array1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Bob", 10, 0, 13.1],
["Emily", 10, 0, 14.1],
["Tony", 10, 1, 15.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         WriteArray(table_path, {{"f1", "10"}}, write_cols, src_array1));
    ASSERT_OK(Commit(table_path, commit_msgs1));
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{{{"f1", "10"}}}, "f0", "btree",
                         /*options=*/{}, Range(0, 4)));

    // Write partition f1=20 (3 rows, sorted by f0)
    auto src_array2 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 20, null, 16.1],
["Lucy", 20, 1, 17.1],
["Tony", 20, 0, 18.1]
    ])")
                          .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2,
                         WriteArray(table_path, {{"f1", "20"}}, write_cols, src_array2));
    ASSERT_OK(Commit(table_path, commit_msgs2));
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{{{"f1", "20"}}}, "f0", "btree",
                         /*options=*/{}, Range(5, 7)));

    // Create a GlobalIndexScan with an explicit 8-thread executor
    std::shared_ptr<Executor> executor = CreateDefaultExecutor(/*thread_count=*/8);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GlobalIndexScan> global_index_scan,
        GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                /*partitions=*/std::nullopt, /*options=*/{}, fs_, executor, pool_));

    // CreateReaders should return 1 UnionGlobalIndexReader (2 sub-readers for 2 ranges)
    ASSERT_OK_AND_ASSIGN(auto index_readers, global_index_scan->CreateReaders("f0", std::nullopt));
    ASSERT_EQ(index_readers.size(), 1u);

    auto union_reader = std::dynamic_pointer_cast<UnionGlobalIndexReader>(index_readers[0]);
    ASSERT_TRUE(union_reader);
    ASSERT_EQ(union_reader->executor_, executor);

    // "Alice" in both partitions: global ids {0, 5}
    ASSERT_OK_AND_ASSIGN(auto result,
                         index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
    ASSERT_TRUE(result);
    ASSERT_EQ(result->ToString(), "{0,5}");

    // "Bob" only in f1=10: global ids {1, 2}
    ASSERT_OK_AND_ASSIGN(auto result2,
                         index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Bob", 3)));
    ASSERT_TRUE(result2);
    ASSERT_EQ(result2->ToString(), "{1,2}");

    // "Lucy" only in f1=20: global id {6}
    ASSERT_OK_AND_ASSIGN(auto result3,
                         index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Lucy", 4)));
    ASSERT_TRUE(result3);
    ASSERT_EQ(result3->ToString(), "{6}");

    // Full pipeline: evaluator with the 8-thread executor
    auto scanner_impl = std::dynamic_pointer_cast<GlobalIndexScanImpl>(global_index_scan);
    ASSERT_TRUE(scanner_impl);

    auto predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                Literal(FieldType::STRING, "Tony", 4));
    ASSERT_OK_AND_ASSIGN(auto index_result, scanner_impl->Scan(predicate));
    ASSERT_TRUE(index_result);
    ASSERT_EQ(index_result->ToString(), "{4,7}");

    auto result_fields = fields_;
    result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Tony", 10, 1, 15.1],
[0, "Tony", 20, 0, 18.1]
    ])")
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, /*predicate=*/nullptr,
                                                           /*options=*/{}, index_result));
    ASSERT_OK(ReadData(table_path, write_cols, expected_array, /*predicate=*/nullptr, plan));
}

TEST_P(GlobalIndexTest, TestBTreeAndBitmapCoexist) {
    // Test btree-global and bitmap index coexisting on the same field (f0).
    // The evaluator should AND their results, producing the intersection.
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);
    std::vector<std::string> write_cols = schema->field_names();

    // Data sorted by f0 for btree: Alice < Bob < Bob < Emily < Lucy < Tony < Tony
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
["Alice", 10, 1, 11.1],
["Bob", 10, 1, 12.1],
["Bob", 25, 1, 16.1],
["Emily", 15, 0, 13.1],
["Lucy", 20, 1, 15.1],
["Tony", 20, 0, 14.1],
["Tony", 30, 0, 17.1],
[null, 30, null, 18.1]
    ])")
                         .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // Build both indexes on f0
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f0", "btree",
                         /*options=*/{}, Range(0, 7)));
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f0", "bitmap",
                         /*options=*/{}, Range(0, 7)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexScan> global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                 /*partitions=*/std::nullopt, /*options=*/{}, fs_,
                                                 /*executor=*/nullptr, pool_));

    // Two index types on f0 -> 2 readers
    ASSERT_OK_AND_ASSIGN(auto index_readers, global_index_scan->CreateReaders("f0", std::nullopt));
    ASSERT_EQ(index_readers.size(), 2u);

    // Each reader individually should return the same result for Equal("Bob")
    for (const auto& index_reader : index_readers) {
        ASSERT_OK_AND_ASSIGN(auto result,
                             index_reader->VisitEqual(Literal(FieldType::STRING, "Bob", 3)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{1,2}");
    }

    // Via evaluator: the two indexes' results get AND, still {1,2}
    auto scanner_impl = std::dynamic_pointer_cast<GlobalIndexScanImpl>(global_index_scan);
    ASSERT_TRUE(scanner_impl);
    ASSERT_OK_AND_ASSIGN(auto evaluator, scanner_impl->GetOrCreateIndexEvaluator());
    {
        // Equal predicate
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Bob", 3));
        ASSERT_OK_AND_ASSIGN(auto result, evaluator->Evaluate(predicate));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{1,2}");
    }
    {
        // NotEqual predicate: both indexes agree on non-null, non-"Bob" rows -> {0,3,4,5,6}
        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                       Literal(FieldType::STRING, "Bob", 3));
        ASSERT_OK_AND_ASSIGN(auto result, evaluator->Evaluate(predicate));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{0,3,4,5,6}");
    }
    {
        // IsNull: both agree on row 7
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING);
        ASSERT_OK_AND_ASSIGN(auto result, evaluator->Evaluate(predicate));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{7}");
    }

    // Full pipeline: f0 == "Alice" -> read data
    {
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Alice", 5));
        ASSERT_OK_AND_ASSIGN(auto index_result, scanner_impl->Scan(predicate));
        ASSERT_TRUE(index_result);
        ASSERT_EQ(index_result->ToString(), "{0}");

        auto result_fields = fields_;
        result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Alice", 10, 1, 11.1]
    ])")
                .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, /*predicate=*/nullptr,
                                                               /*options=*/{}, index_result));
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, /*predicate=*/nullptr, plan));
    }
    // Full pipeline with AND across btree(f0) and bitmap(f0):
    // btree supports LessOrEqual, bitmap returns nullptr for LessOrEqual
    // So AND(LessOrEqual, Equal) -> only the field(s) that both can evaluate get AND
    {
        // f0 == "Bob" AND f1 == 10 (f1 has no index -> nullptr -> keeps btree+bitmap result)
        auto f0_pred =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Bob", 3));
        auto f1_pred = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                               FieldType::INT, Literal(10));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({f0_pred, f1_pred}));
        ASSERT_OK_AND_ASSIGN(auto index_result, scanner_impl->Scan(predicate));
        ASSERT_TRUE(index_result);
        ASSERT_EQ(index_result->ToString(), "{1,2}");

        auto result_fields = fields_;
        result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Bob", 10, 1, 12.1],
[0, "Bob", 25, 1, 16.1]
    ])")
                .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, /*predicate=*/nullptr,
                                                               /*options=*/{}, index_result));
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, /*predicate=*/nullptr, plan));
    }
}

TEST_P(GlobalIndexTest, TestBTreeScanWithPartitionWithMultiMeta) {
    if (file_format_ == "lance" || file_format_ == "avro") {
        return;
    }
    std::string table_path =
        paimon::test::GetDataDir() + "/" + file_format_ +
        "/append_with_btree_with_partition.db/append_with_btree_with_partition";

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexScan> global_index_scan,
                         GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                                 /*partitions=*/std::nullopt, /*options=*/{}, fs_,
                                                 /*executor=*/nullptr, pool_));

    auto count_rows = [](const std::shared_ptr<GlobalIndexResult>& result) -> int64_t {
        EXPECT_TRUE(result);
        EXPECT_OK_AND_ASSIGN(std::vector<Range> ranges, result->ToRanges());
        int64_t total = 0;
        for (const auto& range : ranges) {
            total += range.Count();
        }
        return total;
    };

    auto get_reader = [&](const std::string& column) -> std::shared_ptr<GlobalIndexReader> {
        EXPECT_OK_AND_ASSIGN(auto readers, global_index_scan->CreateReaders(column, std::nullopt));
        EXPECT_EQ(readers.size(), 1u);
        return readers[0];
    };

    // ---- col_boolean ----
    {
        auto reader = get_reader("col_boolean");
        ASSERT_TRUE(reader);
        ASSERT_OK_AND_ASSIGN(auto eq_true, reader->VisitEqual(Literal(true)));
        ASSERT_EQ(count_rows(eq_true), 20);
        ASSERT_OK_AND_ASSIGN(auto eq_false, reader->VisitEqual(Literal(false)));
        ASSERT_EQ(count_rows(eq_false), 20);
    }

    // ---- col_int ----
    {
        auto reader = get_reader("col_int");
        ASSERT_TRUE(reader);
        ASSERT_OK_AND_ASSIGN(auto eq_15, reader->VisitEqual(Literal(15)));
        ASSERT_EQ(count_rows(eq_15), 2);
        ASSERT_OK_AND_ASSIGN(auto eq_missing, reader->VisitEqual(Literal(100)));
        ASSERT_EQ(count_rows(eq_missing), 0);
        // GreaterThan(30): i*3 > 30 -> i in [11, 19], 9 indices per partition -> 18 rows.
        ASSERT_OK_AND_ASSIGN(auto gt_30, reader->VisitGreaterThan(Literal(30)));
        ASSERT_EQ(count_rows(gt_30), 18);
        // GreaterThan(57): nothing greater than the max value.
        ASSERT_OK_AND_ASSIGN(auto gt_max, reader->VisitGreaterThan(Literal(57)));
        ASSERT_EQ(count_rows(gt_max), 0);
    }

    // ---- col_date (values are 18000 + i for i in [0,19]) ----
    {
        auto reader = get_reader("col_date");
        ASSERT_TRUE(reader);
        // 18005 is present at i=5 in both partitions.
        ASSERT_OK_AND_ASSIGN(auto eq_present, reader->VisitEqual(Literal(FieldType::DATE, 18005)));
        ASSERT_EQ(count_rows(eq_present), 2);
        ASSERT_OK_AND_ASSIGN(auto eq_missing, reader->VisitEqual(Literal(FieldType::DATE, 17999)));
        ASSERT_EQ(count_rows(eq_missing), 0);
        // GreaterThan(18010): i in [11, 19] -> 9 per partition -> 18 rows.
        ASSERT_OK_AND_ASSIGN(auto gt_mid,
                             reader->VisitGreaterThan(Literal(FieldType::DATE, 18010)));
        ASSERT_EQ(count_rows(gt_mid), 18);
    }

    // ---- col_double (values are i * 2.2 for i in [0,19]) ----
    {
        auto reader = get_reader("col_double");
        ASSERT_TRUE(reader);
        // i=5 -> 11.0
        ASSERT_OK_AND_ASSIGN(auto eq_present, reader->VisitEqual(Literal(11.0)));
        ASSERT_EQ(count_rows(eq_present), 2);
        ASSERT_OK_AND_ASSIGN(auto eq_missing, reader->VisitEqual(Literal(123.456)));
        ASSERT_EQ(count_rows(eq_missing), 0);
        // GreaterThan(10 * 2.2 = 22.0): i in [11, 19] -> 9 per partition -> 18 rows.
        ASSERT_OK_AND_ASSIGN(auto gt_mid, reader->VisitGreaterThan(Literal(10 * 2.2)));
        ASSERT_EQ(count_rows(gt_mid), 18);
    }

    // ---- col_timestamp (Timestamp from epoch millis = 1700000000000 + i*1000) ----
    {
        auto reader = get_reader("col_timestamp");
        ASSERT_TRUE(reader);
        // i=5 -> 1700000005000 ms.
        ASSERT_OK_AND_ASSIGN(
            auto eq_present,
            reader->VisitEqual(Literal(Timestamp::FromEpochMillis(1700000000000L + 5 * 1000L))));
        ASSERT_EQ(count_rows(eq_present), 2);
        ASSERT_OK_AND_ASSIGN(auto eq_missing,
                             reader->VisitEqual(Literal(Timestamp::FromEpochMillis(1L))));
        ASSERT_EQ(count_rows(eq_missing), 0);
        // GreaterThan(i=10 boundary): i in [11, 19] -> 18 rows globally.
        ASSERT_OK_AND_ASSIGN(auto gt_mid,
                             reader->VisitGreaterThan(
                                 Literal(Timestamp::FromEpochMillis(1700000000000L + 10 * 1000L))));
        ASSERT_EQ(count_rows(gt_mid), 18);
    }

    // ---- col_timestamp_ltz (same physical values as col_timestamp) ----
    {
        auto reader = get_reader("col_timestamp_ltz");
        ASSERT_TRUE(reader);
        ASSERT_OK_AND_ASSIGN(
            auto eq_present,
            reader->VisitEqual(Literal(Timestamp::FromEpochMillis(1700000000000L + 7 * 1000L))));
        ASSERT_EQ(count_rows(eq_present), 2);
        ASSERT_OK_AND_ASSIGN(auto gt_mid,
                             reader->VisitGreaterThan(
                                 Literal(Timestamp::FromEpochMillis(1700000000000L + 10 * 1000L))));
        ASSERT_EQ(count_rows(gt_mid), 18);
    }

    // ---- col_decimal (unscaled = i * 123456, precision=18, scale=6) ----
    {
        auto reader = get_reader("col_decimal");
        ASSERT_TRUE(reader);
        // i=5 -> unscaled 617280
        ASSERT_OK_AND_ASSIGN(
            auto eq_present,
            reader->VisitEqual(Literal(Decimal::FromUnscaledLong(5 * 123456L, 18, 6))));
        ASSERT_EQ(count_rows(eq_present), 2);
        ASSERT_OK_AND_ASSIGN(auto eq_missing,
                             reader->VisitEqual(Literal(Decimal::FromUnscaledLong(1L, 18, 6))));
        ASSERT_EQ(count_rows(eq_missing), 0);
        // GreaterThan(i=10): i in [11, 19] -> 18 rows globally.
        ASSERT_OK_AND_ASSIGN(
            auto gt_mid,
            reader->VisitGreaterThan(Literal(Decimal::FromUnscaledLong(10 * 123456L, 18, 6))));
        ASSERT_EQ(count_rows(gt_mid), 18);
    }

    // ---- col_string (values are "str_00000" .. "str_00019") ----
    {
        auto reader = get_reader("col_string");
        ASSERT_TRUE(reader);
        std::string present_value = "str_00005";
        ASSERT_OK_AND_ASSIGN(auto eq_present,
                             reader->VisitEqual(Literal(FieldType::STRING, present_value.data(),
                                                        present_value.size())));
        ASSERT_EQ(count_rows(eq_present), 2);
        std::string missing_value = "str_99999";
        ASSERT_OK_AND_ASSIGN(auto eq_missing,
                             reader->VisitEqual(Literal(FieldType::STRING, missing_value.data(),
                                                        missing_value.size())));
        ASSERT_EQ(count_rows(eq_missing), 0);
        // GreaterThan("str_00010"): lexicographically greater values are i in [11, 19].
        std::string mid_value = "str_00010";
        ASSERT_OK_AND_ASSIGN(auto gt_mid,
                             reader->VisitGreaterThan(
                                 Literal(FieldType::STRING, mid_value.data(), mid_value.size())));
        ASSERT_EQ(count_rows(gt_mid), 18);
    }
}

#ifdef PAIMON_ENABLE_LUMINA
TEST_P(GlobalIndexTest, TestBTreeWithLumina) {
    // Test btree on f0 (string) and lumina on f1 (vector) coexisting on different fields.
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::list(arrow::float32())),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    std::map<std::string, std::string> lumina_options = {{"lumina.index.dimension", "4"},
                                                         {"lumina.index.type", "bruteforce"},
                                                         {"lumina.distance.metric", "l2"},
                                                         {"lumina.encoding.type", "rawf32"},
                                                         {"lumina.search.parallel_number", "10"}};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format_},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};
    CreateTable(/*partition_keys=*/{}, schema, options);

    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    std::vector<std::string> write_cols = schema->field_names();

    // Data sorted by f0 for btree: Alice < Alice < Bob < Bob < Emily < Lucy < Paul < Tony
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["Alice", [0.0, 0.0, 0.0, 0.0], 10, 11.1],
["Alice", [11.0, 11.0, 11.0, 11.0], 20, 18.1],
["Bob", [0.0, 1.0, 0.0, 1.0], 10, 12.1],
["Bob", [10.0, 11.0, 10.0, 11.0], 20, 16.1],
["Emily", [1.0, 0.0, 1.0, 0.0], 10, 13.1],
["Lucy", [10.0, 10.0, 10.0, 10.0], 20, 15.1],
["Paul", [10.0, 10.0, 10.0, 10.0], 20, 19.1],
["Tony", [1.0, 1.0, 1.0, 1.0], 10, 14.1]
    ])")
                         .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols, src_array));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // Build btree index on f0
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f0", "btree",
                         /*options=*/{}, Range(0, 7)));
    // Build lumina index on f1
    ASSERT_OK(WriteIndex(table_path, /*partition_filters=*/{}, "f1", "lumina",
                         /*options=*/lumina_options, Range(0, 7)));

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GlobalIndexScan> global_index_scan,
        GlobalIndexScan::Create(table_path, /*snapshot_id=*/std::nullopt,
                                /*partitions=*/std::nullopt,
                                /*options=*/lumina_options, fs_, /*executor=*/nullptr, pool_));

    // Query f0 via btree
    {
        ASSERT_OK_AND_ASSIGN(auto index_readers,
                             global_index_scan->CreateReaders("f0", std::nullopt));
        ASSERT_EQ(index_readers.size(), 1u);

        ASSERT_OK_AND_ASSIGN(auto result,
                             index_readers[0]->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->ToString(), "{0,1}");

        ASSERT_OK_AND_ASSIGN(
            auto result2, index_readers[0]->VisitLessThan(Literal(FieldType::STRING, "Emily", 5)));
        ASSERT_TRUE(result2);
        ASSERT_EQ(result2->ToString(), "{0,1,2,3}");
    }

    // Query f1 via lumina (vector search)
    {
        ASSERT_OK_AND_ASSIGN(auto index_readers,
                             global_index_scan->CreateReaders("f1", std::nullopt));
        ASSERT_EQ(index_readers.size(), 1u);
        std::vector<float> query = {11.0f, 11.0f, 11.0f, 11.0f};
        auto vector_search = std::make_shared<VectorSearch>(
            "f1", /*limit=*/1, query, /*filter=*/nullptr,
            /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/lumina_options);
        ASSERT_OK_AND_ASSIGN(auto scored_result,
                             index_readers[0]->VisitVectorSearch(vector_search));
        ASSERT_TRUE(scored_result);
        ASSERT_EQ(scored_result->ToString(), "row ids: {1}, scores: {0.00}");
    }

    // Evaluator: btree on f0 = "Bob"
    {
        auto scanner_impl = std::dynamic_pointer_cast<GlobalIndexScanImpl>(global_index_scan);
        ASSERT_TRUE(scanner_impl);
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "Bob", 3));
        ASSERT_OK_AND_ASSIGN(auto index_result, scanner_impl->Scan(predicate));
        ASSERT_TRUE(index_result);
        ASSERT_EQ(index_result->ToString(), "{2,3}");

        // Read data for Bob
        auto result_fields = fields;
        result_fields.insert(result_fields.begin(), SpecialFields::ValueKind().ArrowField());
        auto expected_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), R"([
[0, "Bob", [0.0, 1.0, 0.0, 1.0], 10, 12.1],
[0, "Bob", [10.0, 11.0, 10.0, 11.0], 20, 16.1]
    ])")
                .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto plan, ScanGlobalIndexAndData(table_path, /*predicate=*/nullptr,
                                                               /*options=*/{}, index_result));
        ASSERT_OK(ReadData(table_path, write_cols, expected_array, /*predicate=*/nullptr, plan));
    }

    // Combined: btree f0 filter + lumina vector search with pre-filter
    // Use btree result as pre_filter for lumina search
    {
        ASSERT_OK_AND_ASSIGN(auto btree_readers,
                             global_index_scan->CreateReaders("f0", std::nullopt));
        ASSERT_EQ(btree_readers.size(), 1u);
        // Get rows where f0 == "Alice" -> {0, 1}
        ASSERT_OK_AND_ASSIGN(auto btree_result,
                             btree_readers[0]->VisitEqual(Literal(FieldType::STRING, "Alice", 5)));
        ASSERT_TRUE(btree_result);
        ASSERT_EQ(btree_result->ToString(), "{0,1}");

        // Now vector search on f1 with pre_filter limiting to Alice's rows {0, 1}
        ASSERT_OK_AND_ASSIGN(auto lumina_readers,
                             global_index_scan->CreateReaders("f1", std::nullopt));
        ASSERT_EQ(lumina_readers.size(), 1u);
        std::vector<float> query = {11.0f, 11.0f, 11.0f, 11.0f};
        auto filter = [](int64_t id) -> bool { return id == 0 || id == 1; };
        auto vector_search = std::make_shared<VectorSearch>(
            "f1", /*limit=*/1, query, filter,
            /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/lumina_options);
        ASSERT_OK_AND_ASSIGN(auto scored_result,
                             lumina_readers[0]->VisitVectorSearch(vector_search));
        ASSERT_EQ(scored_result->ToString(), "row ids: {1}, scores: {0.00}");
    }
}
#endif

std::vector<ParamType> GetTestValuesForGlobalIndexTest() {
    std::vector<ParamType> values;
    values.emplace_back("parquet", false);
    values.emplace_back("parquet", true);
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back("orc", false);
    values.emplace_back("orc", true);
#endif
#ifdef PAIMON_ENABLE_LANCE
    values.emplace_back("lance", false);
    values.emplace_back("lance", true);
#endif
#ifdef PAIMON_ENABLE_AVRO
    values.emplace_back("avro", false);
    values.emplace_back("avro", true);
#endif
    return values;
}

INSTANTIATE_TEST_SUITE_P(FileFormat, GlobalIndexTest,
                         ::testing::ValuesIn(GetTestValuesForGlobalIndexTest()));

}  // namespace paimon::test
