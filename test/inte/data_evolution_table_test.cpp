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
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/indexed_split.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
// This is a sdk end-to-end test for data evolution
class DataEvolutionTableTest : public ::testing::Test,
                               public ::testing::WithParamInterface<std::string> {
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create("local");
        int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
        std::srand(seed);
    }
    void TearDown() override {
        dir_.reset();
    }

    void CreateTable(const std::vector<std::string>& partition_keys,
                     const std::map<std::string, std::string>& options) const {
        auto schema = arrow::schema(fields_);
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
        return CreateTable(partition_keys, options);
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

    void SetFirstRowId(int64_t reset_first_row_id,
                       std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        for (auto& commit_msg : commit_msgs) {
            auto commit_msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(commit_msg);
            ASSERT_TRUE(commit_msg_impl);
            for (auto& file : commit_msg_impl->data_increment_.new_files_) {
                file->AssignFirstRowId(reset_first_row_id);
            }
        }
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

    Status ScanAndRead(const std::string& table_path, const std::vector<std::string>& read_schema,
                       const std::shared_ptr<arrow::StructArray>& expected_array,
                       const std::shared_ptr<Predicate>& predicate = nullptr,
                       const std::vector<Range>& row_ranges = {},
                       bool check_scan_plan_when_empty_result = true) const {
        // scan
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.SetPredicate(predicate);
        if (!row_ranges.empty()) {
            auto global_index_result = BitmapGlobalIndexResult::FromRanges(row_ranges);
            scan_context_builder.SetGlobalIndexResult(global_index_result);
        }
        PAIMON_ASSIGN_OR_RAISE(auto scan_context, scan_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_scan, TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(auto result_plan, table_scan->CreatePlan());
        if (!expected_array && check_scan_plan_when_empty_result) {
            if (!result_plan->Splits().empty()) {
                return Status::Invalid("check_scan_plan_when_empty_result but plan is not empty");
            }
        }

        // read
        auto splits = result_plan->Splits();
        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.SetReadSchema(read_schema).SetPredicate(predicate);
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
        // add row kind array for expected array
        auto row_kind_scalar =
            std::make_shared<arrow::Int8Scalar>(RowKind::Insert()->ToByteValue());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto row_kind_array,
            arrow::MakeArrayFromScalar(*row_kind_scalar, expected_array->length()));

        arrow::ArrayVector expected_with_row_kind_fields = expected_array->fields();
        std::vector<std::string> expected_with_row_kind_field_names =
            arrow::schema(expected_array->type()->fields())->field_names();
        expected_with_row_kind_fields.insert(expected_with_row_kind_fields.begin(), row_kind_array);
        expected_with_row_kind_field_names.insert(expected_with_row_kind_field_names.begin(),
                                                  "_VALUE_KIND");

        // check read result
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto expected_with_row_kind_array,
            arrow::StructArray::Make(expected_with_row_kind_fields,
                                     expected_with_row_kind_field_names));
        auto expected_chunk_array =
            std::make_shared<arrow::ChunkedArray>(expected_with_row_kind_array);
        if (!expected_chunk_array->Equals(read_result)) {
            std::cout << "result=" << read_result->ToString() << std::endl
                      << "expected=" << expected_chunk_array->ToString() << std::endl;
            return Status::Invalid("expected array and result array not equal");
        }
        return Status::OK();
    }

    void CheckScanResult(const std::string& table_path, const std::shared_ptr<Predicate>& predicate,
                         const std::vector<Range>& row_ranges,
                         const std::vector<std::optional<int64_t>>& expected_first_row_ids,
                         const std::vector<int64_t>& expected_row_counts) {
        ASSERT_EQ(expected_first_row_ids.size(), expected_row_counts.size());
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.SetPredicate(predicate);
        if (!row_ranges.empty()) {
            auto global_index_result = BitmapGlobalIndexResult::FromRanges(row_ranges);
            scan_context_builder.SetGlobalIndexResult(global_index_result);
        }
        ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
        const auto& result_splits = result_plan->Splits();
        if (expected_first_row_ids.empty()) {
            ASSERT_EQ(result_splits.size(), 0);
            return;
        }
        ASSERT_EQ(result_splits.size(), 1);
        std::shared_ptr<DataSplitImpl> data_split;
        if (auto indexed_split = std::dynamic_pointer_cast<IndexedSplit>(result_splits[0])) {
            data_split = std::dynamic_pointer_cast<DataSplitImpl>(indexed_split->GetDataSplit());
        } else {
            data_split = std::dynamic_pointer_cast<DataSplitImpl>(result_splits[0]);
        }
        ASSERT_TRUE(data_split);
        std::vector<std::optional<int64_t>> result_first_row_ids;
        std::vector<int64_t> result_row_counts;
        for (const auto& meta : data_split->DataFiles()) {
            result_first_row_ids.push_back(meta->first_row_id);
            result_row_counts.push_back(meta->row_count);
        }
        ASSERT_EQ(result_first_row_ids, expected_first_row_ids);
        ASSERT_EQ(result_row_counts, expected_row_counts);
    }

    std::shared_ptr<arrow::StructArray> PrepareBulkData(
        int32_t write_batch_size, std::function<std::string(int32_t)> data_generator,
        const arrow::FieldVector& fields) const {
        std::string data_str = "[";
        for (int32_t i = 0; i < write_batch_size; i++) {
            data_str.append("[");
            auto row_str = data_generator(i);
            data_str.append(row_str);
            data_str.append("],");
        }
        data_str.pop_back();
        data_str.append("]");
        return std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), data_str)
                .ValueOrDie());
    }

 private:
    std::unique_ptr<UniqueTestDirectory> dir_;
    arrow::FieldVector fields_ = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::utf8()),
    };
};

TEST_P(DataEvolutionTableTest, TestBasic) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1, f2
    std::vector<std::string> write_cols0 = schema->field_names();
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f2
    std::vector<std::string> write_cols1 = {"f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(commit_msgs, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "c"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    if (GetParam() != "lance") {
        // read with row tracking
        auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({fields_[1], fields_[0], SpecialFields::SequenceNumber().field_,
                                SpecialFields::RowId().field_, fields_[2]}),
                R"([
        ["a", 1, 2, 0, "c"]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f1", "f0", "_SEQUENCE_NUMBER", "_ROW_ID", "f2"},
                              expected_row_tracking_array));

        // read score but not indexed split
        ASSERT_NOK_WITH_MSG(
            ScanAndRead(table_path, {"f0", "f1", "_INDEX_SCORE"}, expected_row_tracking_array,
                        /*predicate=*/nullptr,
                        /*row_ranges=*/{}),
            "Invalid read schema, read _INDEX_SCORE while split cannot cast to IndexedSplit");
    }
}

TEST_P(DataEvolutionTableTest, TestMultipleAppends) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1, f2
    std::vector<std::string> write_cols0 = schema->field_names();
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f0, f1
    std::vector<std::string> write_cols1 = {"f0", "f1"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [1, "a"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(10, commit_msgs1);

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols2, src_array2));
    SetFirstRowId(10, commit_msgs2);

    std::vector<std::shared_ptr<CommitMessage>> total_msgs;
    total_msgs.insert(total_msgs.end(), commit_msgs1.begin(), commit_msgs1.end());
    total_msgs.insert(total_msgs.end(), commit_msgs2.begin(), commit_msgs2.end());
    ASSERT_OK(Commit(table_path, total_msgs));

    // write field: f0, f1
    std::vector<std::string> write_cols3 = {"f0", "f1"};
    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [2, "c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, WriteArray(table_path, write_cols3, src_array3));
    SetFirstRowId(11, commit_msgs3);
    ASSERT_OK(Commit(table_path, commit_msgs3));

    // write field: f2
    std::vector<std::string> write_cols4 = {"f2"};
    auto src_array4 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["d"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs4, WriteArray(table_path, write_cols4, src_array4));
    SetFirstRowId(11, commit_msgs4);
    ASSERT_OK(Commit(table_path, commit_msgs4));

    {
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [2, "c", "d"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));
    }
    {
        std::vector<Range> row_ranges = {Range(0l, 0l), Range(11l, 11l)};
        // test with row ids
        CheckScanResult(table_path, /*predicate=*/nullptr,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 11, 11}, /*expected_row_counts=*/{10, 1, 1});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"],
        [2, "c", "d"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
    if (GetParam() != "lance") {
        // read with row tracking
        auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({
                                                          fields_[0],
                                                          fields_[1],
                                                          fields_[2],
                                                          SpecialFields::RowId().field_,
                                                          SpecialFields::SequenceNumber().field_,
                                                      }),
                                                      R"([
        [1, "a", "b", 0, 1],
        [1, "a", "b", 1, 1],
        [1, "a", "b", 2, 1],
        [1, "a", "b", 3, 1],
        [1, "a", "b", 4, 1],
        [1, "a", "b", 5, 1],
        [1, "a", "b", 6, 1],
        [1, "a", "b", 7, 1],
        [1, "a", "b", 8, 1],
        [1, "a", "b", 9, 1],
        [1, "a", "b", 10, 2],
        [2, "c", "d", 11, 4]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                              expected_row_tracking_array));
    }
}

TEST_P(DataEvolutionTableTest, TestOnlySomeColumns) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0
    std::vector<std::string> write_cols0 = {"f0"};
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0]}), R"([
        [1]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f1
    std::vector<std::string> write_cols1 = {"f1"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[1]}), R"([
        ["a"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(0, commit_msgs1);
    ASSERT_OK(Commit(table_path, commit_msgs1));

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols2, src_array2));
    SetFirstRowId(0, commit_msgs2);
    ASSERT_OK(Commit(table_path, commit_msgs2));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    if (GetParam() != "lance") {
        // read with row tracking
        auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({
                                                          fields_[0],
                                                          fields_[1],
                                                          fields_[2],
                                                          SpecialFields::RowId().field_,
                                                          SpecialFields::SequenceNumber().field_,
                                                      }),
                                                      R"([
        [1, "a", "b", 0, 3]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                              expected_row_tracking_array));
    }
}

TEST_P(DataEvolutionTableTest, TestNullValues) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1
    std::vector<std::string> write_cols1 = {"f0", "f1"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [1, null]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(0, commit_msgs1);

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols2, src_array2));
    SetFirstRowId(0, commit_msgs2);

    std::vector<std::shared_ptr<CommitMessage>> total_msgs;
    total_msgs.insert(total_msgs.end(), commit_msgs1.begin(), commit_msgs1.end());
    total_msgs.insert(total_msgs.end(), commit_msgs2.begin(), commit_msgs2.end());
    ASSERT_OK(Commit(table_path, total_msgs));

    // Commit 2: Overwrite with non-null
    // write field: f2
    std::vector<std::string> write_cols3 = {"f2"};
    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, WriteArray(table_path, write_cols3, src_array3));
    SetFirstRowId(0, commit_msgs3);
    ASSERT_OK(Commit(table_path, commit_msgs3));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, null, "c"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    if (GetParam() != "lance") {
        // read with row tracking
        auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({
                                                          fields_[0],
                                                          fields_[1],
                                                          fields_[2],
                                                          SpecialFields::RowId().field_,
                                                          SpecialFields::SequenceNumber().field_,
                                                      }),
                                                      R"([
        [1, null, "c", 0, 2]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                              expected_row_tracking_array));
    }
}

TEST_P(DataEvolutionTableTest, TestMultipleAppendsDifferentFirstRowIds) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // First commit, firstRowId = 0
    // write field: f0, f1
    std::vector<std::string> write_cols1 = {"f0", "f1"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [1, "a"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(0, commit_msgs1);

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols2, src_array2));
    SetFirstRowId(0, commit_msgs2);

    std::vector<std::shared_ptr<CommitMessage>> total_msgs;
    total_msgs.insert(total_msgs.end(), commit_msgs1.begin(), commit_msgs1.end());
    total_msgs.insert(total_msgs.end(), commit_msgs2.begin(), commit_msgs2.end());
    ASSERT_OK(Commit(table_path, total_msgs));

    // Second commit, firstRowId = 1
    // write field: f0, f1
    std::vector<std::string> write_cols3 = {"f0", "f1"};
    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [2, "c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, WriteArray(table_path, write_cols3, src_array3));
    SetFirstRowId(1, commit_msgs3);
    ASSERT_OK(Commit(table_path, commit_msgs3));

    // Third commit
    // write field: f2
    std::vector<std::string> write_cols4 = {"f2"};
    auto src_array4 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["d"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs4, WriteArray(table_path, write_cols4, src_array4));
    SetFirstRowId(1, commit_msgs4);
    ASSERT_OK(Commit(table_path, commit_msgs4));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"],
        [2, "c", "d"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    if (GetParam() != "lance") {
        // read with row tracking
        auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({
                                                          fields_[0],
                                                          fields_[1],
                                                          fields_[2],
                                                          SpecialFields::RowId().field_,
                                                          SpecialFields::SequenceNumber().field_,
                                                      }),
                                                      R"([
        [1, "a", "b", 0, 1],
        [2, "c", "d", 1, 3]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                              expected_row_tracking_array));
    }
}

TEST_P(DataEvolutionTableTest, TestMoreData) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1
    std::vector<std::string> write_cols1 = {"f0", "f1"};
    // row0: 0, a0; row1: 1, a1 ...
    auto src_array1 = PrepareBulkData(
        10000, [](int32_t i) { return std::to_string(i) + ", \"a" + std::to_string(i) + "\""; },
        {fields_[0], fields_[1]});
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(0, commit_msgs1);

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    // row0: b0; row1: b1 ...
    auto src_array2 = PrepareBulkData(
        10000, [](int32_t i) { return "\"b" + std::to_string(i) + "\""; }, {fields_[2]});
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols2, src_array2));
    SetFirstRowId(0, commit_msgs2);

    std::vector<std::shared_ptr<CommitMessage>> total_msgs;
    total_msgs.insert(total_msgs.end(), commit_msgs1.begin(), commit_msgs1.end());
    total_msgs.insert(total_msgs.end(), commit_msgs2.begin(), commit_msgs2.end());
    ASSERT_OK(Commit(table_path, total_msgs));

    // write field: f2
    std::vector<std::string> write_cols3 = {"f2"};
    // row0: c0; row1: c1 ...
    auto src_array3 = PrepareBulkData(
        10000, [](int32_t i) { return "\"c" + std::to_string(i) + "\""; }, {fields_[2]});
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, WriteArray(table_path, write_cols3, src_array3));
    ASSERT_OK(Commit(table_path, commit_msgs3));

    // row0: 0, a0, c0; row1: 1, a1, c1 ...
    auto expected_array = PrepareBulkData(10000,
                                          [](int32_t i) {
                                              return std::to_string(i) + ", \"a" +
                                                     std::to_string(i) + "\", \"c" +
                                                     std::to_string(i) + "\"";
                                          },
                                          {fields_[0], fields_[1], fields_[2]});
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));
}

TEST_P(DataEvolutionTableTest, TestOnlyRowTrackingEnabled) {
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, GetParam()},
        {Options::FILE_SYSTEM, "local"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "false"},
    };
    CreateTable(/*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1, f2
    std::vector<std::string> write_cols0 = schema->field_names();
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"],
        [2, "c", "d"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    if (GetParam() != "lance") {
        // read with row tracking
        auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({fields_[1], fields_[0], SpecialFields::SequenceNumber().field_,
                                SpecialFields::RowId().field_, fields_[2]}),
                R"([
        ["a", 1, 1, 0, "b"],
        ["c", 2, 1, 1, "d"]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f1", "f0", "_SEQUENCE_NUMBER", "_ROW_ID", "f2"},
                              expected_row_tracking_array));
    }
}

TEST_P(DataEvolutionTableTest, TestExternalPath) {
    // create external path dir
    auto external_dir = UniqueTestDirectory::Create("local");
    ASSERT_TRUE(external_dir);
    std::string external_test_dir = external_dir->Str();

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, GetParam()},
        {Options::FILE_SYSTEM, "local"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::DATA_FILE_EXTERNAL_PATHS, "FILE://" + external_test_dir},
    };
    CreateTable(/*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1
    std::vector<std::string> write_cols0 = {"f0", "f1"};
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [1, "a"],
        [2, "c"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs0, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs0));

    // write field: f0, f2
    std::vector<std::string> write_cols1 = {"f0", "f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[2]}), R"([
        [10, "b"],
        [20, "d"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs1);
    ASSERT_OK(Commit(table_path, commit_msgs1));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [10, "a", "b"],
        [20, "c", "d"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    if (GetParam() != "lance") {
        // read with row tracking
        auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({fields_[1], fields_[0], fields_[2], SpecialFields::RowId().field_,
                                SpecialFields::SequenceNumber().field_}),
                R"([
        ["a", 10, "b", 0, 2],
        ["c", 20, "d", 1, 2]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f1", "f0", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                              expected_row_tracking_array));
    }
}

TEST_P(DataEvolutionTableTest, TestWithPartitionSimple) {
    std::vector<std::string> partition_keys = {"f1"};
    CreateTable(partition_keys);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1, f2 for f1=2024
    std::map<std::string, std::string> partition0 = {{"f1", "2024"}};
    std::vector<std::string> write_cols0 = schema->field_names();
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "2024", "b1"],
        [2, "2024", "b2"],
        [3, "2024", "b3"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, partition0, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f2 for f1=2024
    std::vector<std::string> write_cols1 = {"f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["c1"],
        ["c2"],
        ["c3"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(commit_msgs, WriteArray(table_path, partition0, write_cols1, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write fields: f1, f2 for f1=2025
    std::map<std::string, std::string> partition2 = {{"f1", "2025"}};
    std::vector<std::string> write_cols2 = {"f1", "f2"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[1], fields_[2]}), R"([
        ["2025", "d1"],
        ["2025", "d2"],
        ["2025", "d3"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(commit_msgs, WriteArray(table_path, partition2, write_cols2, src_array2));
    SetFirstRowId(/*reset_first_row_id=*/3, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));

    // test read all fields
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "2024", "c1"],
        [2, "2024", "c2"],
        [3, "2024", "c3"],
        [null, "2025", "d1"],
        [null, "2025", "d2"],
        [null, "2025", "d3"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    if (GetParam() != "lance") {
        // test only read partition fields
        auto expected_array_only_partition = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[1]}), R"([
        ["2024"],
        ["2024"],
        ["2024"],
        ["2025"],
        ["2025"],
        ["2025"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, {"f1"}, expected_array_only_partition));

        // read with row tracking
        auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_,
                                SpecialFields::SequenceNumber().field_}),
                R"([
        [1, "2024", "c1", 0, 2],
        [2, "2024", "c2", 1, 2],
        [3, "2024", "c3", 2, 2],
        [null, "2025", "d1", 3, 3],
        [null, "2025", "d2", 4, 3],
        [null, "2025", "d3", 5, 3]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                              expected_row_tracking_array));

        // read only read partition fields and row tracking
        auto expected_partition_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({fields_[1], SpecialFields::RowId().field_,
                                SpecialFields::SequenceNumber().field_}),
                R"([
        ["2024", 0, 2],
        ["2024", 1, 2],
        ["2024", 2, 2],
        ["2025", 3, 3],
        ["2025", 4, 3],
        ["2025", 5, 3]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f1", "_ROW_ID", "_SEQUENCE_NUMBER"},
                              expected_partition_row_tracking_array));
    }
}

TEST_P(DataEvolutionTableTest, TestWithPartitionWithoutPartitionFieldsInFile) {
    std::vector<std::string> partition_keys = {"f1"};
    CreateTable(partition_keys);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f2 for f1=2024
    std::map<std::string, std::string> partition0 = {{"f1", "2024"}};
    std::vector<std::string> write_cols0 = {"f0", "f2"};
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[2]}), R"([
        [1, "b1"],
        [2, "b2"],
        [3, "b3"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, partition0, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f2 for f1=2024
    std::vector<std::string> write_cols1 = {"f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["c1"],
        ["c2"],
        ["c3"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(commit_msgs, WriteArray(table_path, partition0, write_cols1, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));

    {
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "2024", "c1"],
        [2, "2024", "c2"],
        [3, "2024", "c3"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));
    }
    {
        // test with row ids
        std::vector<Range> row_ranges = {Range(1l, 1l)};
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 0}, /*expected_row_counts=*/{3, 3});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [2, "2024", "c2"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }

    if (GetParam() != "lance") {
        // read with row tracking
        auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_,
                                SpecialFields::SequenceNumber().field_}),
                R"([
        [1, "2024", "c1", 0, 2],
        [2, "2024", "c2", 1, 2],
        [3, "2024", "c3", 2, 2]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                              expected_row_tracking_array));
    }
}

TEST_P(DataEvolutionTableTest, TestPartitionWithPredicate) {
    auto file_format = GetParam();
    if (file_format == "lance" || file_format == "avro") {
        return;
    }
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},         {Options::FILE_FORMAT, GetParam()},
        {Options::FILE_SYSTEM, "local"},           {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"}, {"parquet.write.max-row-group-length", "1"}};

    CreateTable(partition_keys, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1 for partition f1 = "2024"
    std::vector<std::string> write_cols0 = {"f0", "f1"};
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [11, "2024"],
        [12, "2024"],
        [13, "2024"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs0,
                         WriteArray(table_path, {{"f1", "2024"}}, write_cols0, src_array0));

    // write field: f2 for partition f1 = "2024"
    std::vector<std::string> write_cols1 = {"f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["a"],
        ["b"],
        ["c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         WriteArray(table_path, {{"f1", "2024"}}, write_cols1, src_array1));
    std::vector<std::shared_ptr<CommitMessage>> total_msgs;
    total_msgs.insert(total_msgs.end(), commit_msgs0.begin(), commit_msgs0.end());
    total_msgs.insert(total_msgs.end(), commit_msgs1.begin(), commit_msgs1.end());
    SetFirstRowId(0, total_msgs);
    ASSERT_OK(Commit(table_path, total_msgs));

    // write field: f0, f1 for partition f1 = "2025"
    std::vector<std::string> write_cols3 = {"f0", "f1"};
    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [21, "2025"],
        [22, "2025"],
        [23, "2025"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs3,
                         WriteArray(table_path, {{"f1", "2025"}}, write_cols3, src_array3));
    SetFirstRowId(3, commit_msgs3);
    ASSERT_OK(Commit(table_path, commit_msgs3));
    {
        // only set data field predicate, predicate only takes effective in file skip
        // read will not push down
        auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                             Literal(11));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [11, "2024", "a"],
        [12, "2024", "b"],
        [13, "2024", "c"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array, equal));
    }
    {
        // only set partition predicate
        auto equal =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                    Literal(FieldType::STRING, "2024", 4));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [11, "2024", "a"],
        [12, "2024", "b"],
        [13, "2024", "c"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array, equal));
    }
    {
        // set partition predicate and data field predicate
        auto equal =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                    Literal(FieldType::STRING, "2024", 4));
        auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                                          FieldType::INT, Literal(100));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({equal, greater_than}));
        ASSERT_OK(
            ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr, predicate));
    }
    {
        // read with row tracking
        auto equal =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                    Literal(FieldType::STRING, "2024", 4));

        auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_,
                                SpecialFields::SequenceNumber().field_}),
                R"([
        [11, "2024", "a", 0, 1],
        [12, "2024", "b", 1, 1],
        [13, "2024", "c", 2, 1]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                              expected_row_tracking_array, equal));
    }
    {
        // test with row ids
        std::vector<Range> row_ranges = {Range(1l, 1l)};
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 0}, /*expected_row_counts=*/{3, 3});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [12, "2024", "b"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
    {
        std::vector<Range> row_ranges = {Range(1l, 1l)};
        // test with row ids and partition predicate
        auto equal =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                    Literal(FieldType::STRING, "2025", 4));
        CheckScanResult(table_path, /*predicate=*/equal, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{}, /*expected_row_counts=*/{});
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr,
                              /*predicate=*/equal,
                              /*row_ranges=*/row_ranges));
    }
    {
        std::vector<Range> row_ranges = {Range(1l, 1l)};
        // test with row ids and non-partition predicate
        auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                             Literal(11));
        CheckScanResult(table_path, /*predicate=*/equal, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 0}, /*expected_row_counts=*/{3, 3});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [12, "2024", "b"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/equal,
                              /*row_ranges=*/row_ranges));
    }
    {
        std::vector<Range> row_ranges = {Range(1l, 1l)};
        // test with row ids and data predicate
        auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                             Literal(50));
        CheckScanResult(table_path, /*predicate=*/equal, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{}, /*expected_row_counts=*/{});
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr,
                              /*predicate=*/equal,
                              /*row_ranges=*/row_ranges));
    }
}

TEST_P(DataEvolutionTableTest, TestAlterTable) {
    auto file_format = GetParam();
    if (file_format == "lance" || file_format == "avro") {
        return;
    }
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/append_table_alter_table_with_cast_with_data_evolution.db/"
                             "append_table_alter_table_with_cast_with_data_evolution";
    std::vector<DataField> read_fields = {
        DataField(6, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(0, arrow::field("key0", arrow::int32())),
        DataField(1, arrow::field("key1", arrow::int32())),
        DataField(2, arrow::field("f3", arrow::int32())),
        DataField(3, arrow::field("f1", arrow::utf8())),
        DataField(4, arrow::field("f2", arrow::decimal128(6, 3))),
        DataField(5, arrow::field("f0", arrow::boolean())),
        DataField(8, arrow::field("f6", arrow::int32())),
        SpecialFields::RowId(),
        SpecialFields::SequenceNumber()};

    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    {
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
["1970-01-05T00:00", 0, 1, 100, "2024-11-26 06:38:56.001000001", "0.020", true, null, 0, 1],
["1969-11-18T00:00", 0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120", true, null, 1, 1],
["1971-03-21T00:00", 0, 1, 120, "2024-11-26 06:38:56.021000021", "22.220", false, null, 2, 1],
["1957-11-01T00:00", 0, 1, 130, "2024-11-26 06:38:56.031000031", "333.320", false, null, 3, 1],
["2091-09-07T00:00", 0, 1, 140, "2024-11-26 06:38:56.041000041", "444.420", true, null, 4, 1],
["2024-11-26T06:38:56.054000154", 0, 1, 150, "2024-11-26 15:28:31", "55.002", true, 56, 5, 3],
["2024-11-26T06:38:56.064000164", 0, 1, 160, "2024-11-26 15:28:41", "666.012", false, 66, 6, 3],
["2024-11-26T06:38:56.074000174", 0, 1, 170, "2024-11-26 15:28:51", "-77.022", true, 76, 7, 3],
["2024-11-26T06:38:56.084000184", 0, 1, 180, "2024-11-26 15:29:01", "8.032", true, -86, 8, 3],
["2024-11-26T06:38:56.094000194", 0, 1, 190, "I'm strange", "-999.420", false, 96, 9, 3]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array));
    }
    {
        // only files with schema-1 will be skipped, while files with schema-0 will be reserved
        // as type change
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                       FieldType::INT, Literal(200));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
["1970-01-05T00:00", 0, 1, 100, "2024-11-26 06:38:56.001000001", "0.020", true, null, 0, 1],
["1969-11-18T00:00", 0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120", true, null, 1, 1],
["1971-03-21T00:00", 0, 1, 120, "2024-11-26 06:38:56.021000021", "22.220", false, null, 2, 1],
["1957-11-01T00:00", 0, 1, 130, "2024-11-26 06:38:56.031000031", "333.320", false, null, 3, 1],
["2091-09-07T00:00", 0, 1, 140, "2024-11-26 06:38:56.041000041", "444.420", true, null, 4, 1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        // files with schema-0 will be reserved as f6 does not exist in schema-0 (null count =
        // null), files with schema-1 will also be reserved
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/7, /*field_name=*/"f6", FieldType::INT);
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
["1970-01-05T00:00", 0, 1, 100, "2024-11-26 06:38:56.001000001", "0.020", true, null, 0, 1],
["1969-11-18T00:00", 0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120", true, null, 1, 1],
["1971-03-21T00:00", 0, 1, 120, "2024-11-26 06:38:56.021000021", "22.220", false, null, 2, 1],
["1957-11-01T00:00", 0, 1, 130, "2024-11-26 06:38:56.031000031", "333.320", false, null, 3, 1],
["2091-09-07T00:00", 0, 1, 140, "2024-11-26 06:38:56.041000041", "444.420", true, null, 4, 1],
["2024-11-26T06:38:56.054000154", 0, 1, 150, "2024-11-26 15:28:31", "55.002", true, 56, 5, 3],
["2024-11-26T06:38:56.064000164", 0, 1, 160, "2024-11-26 15:28:41", "666.012", false, 66, 6, 3],
["2024-11-26T06:38:56.074000174", 0, 1, 170, "2024-11-26 15:28:51", "-77.022", true, 76, 7, 3],
["2024-11-26T06:38:56.084000184", 0, 1, 180, "2024-11-26 15:29:01", "8.032", true, -86, 8, 3],
["2024-11-26T06:38:56.094000194", 0, 1, 190, "I'm strange", "-999.420", false, 96, 9, 3]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        std::vector<Range> row_ranges = {Range(1l, 1l)};
        // test with row ids
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 0}, /*expected_row_counts=*/{5, 5});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
["1969-11-18T00:00", 0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120", true, null, 1, 1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
}

TEST_P(DataEvolutionTableTest, TestReadCompactFiles) {
    auto file_format = GetParam();
    if (file_format == "lance" || file_format == "avro") {
        return;
    }
    std::string table_path =
        paimon::test::GetDataDir() + file_format +
        "/append_table_row_tracking_with_compact.db/append_table_row_tracking_with_compact";
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64())),
                                          SpecialFields::RowId(),
                                          SpecialFields::SequenceNumber()};

    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 12, 2.1, 0, 1],
        ["Alice", 3, 13, 3.1, 1, 1],
        ["Bob", 4, 14, 4.1, 2, 2],
        ["David", 5, 15, 5.1, 3, 2]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                          expected_array));
}

TEST_P(DataEvolutionTableTest, TestReadTableWithDenseStats) {
    auto file_format = GetParam();
    if (file_format == "lance" || file_format == "avro") {
        return;
    }
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/data_evolution_with_dense_stats.db/data_evolution_with_dense_stats";
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64())),
                                          SpecialFields::RowId(),
                                          SpecialFields::SequenceNumber()};

    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 102, 2.1, 0, 2],
        ["Alice", 4, 104, 3.1, 1, 2]
    ])")
            .ValueOrDie());
    {
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array));
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(102));
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(12));
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              /*expected_array=*/nullptr, predicate));
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::INT, Literal(6));
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              /*expected_array=*/nullptr, predicate));
    }
    {
        // f3 does not have stats, therefore data will not be filtered
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                       FieldType::DOUBLE, Literal(5.1));
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        // test row id with predicate
        std::vector<Range> row_ranges = {Range(0l, 0l)};
        auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                 FieldType::INT, Literal(5));
        CheckScanResult(table_path, /*predicate=*/predicate, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{}, /*expected_row_counts=*/{});
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              /*expected_array=*/nullptr, predicate,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test row id with predicate
        std::vector<Range> row_ranges = {Range(0l, 0l)};
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                       FieldType::DOUBLE, Literal(5.1));
        CheckScanResult(table_path, /*predicate=*/predicate, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 0}, /*expected_row_counts=*/{2, 2});
        auto expected_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 102, 2.1, 0, 2]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array0, predicate,
                              /*row_ranges=*/row_ranges));
    }
}

TEST_P(DataEvolutionTableTest, TestScanAndReadWithIndex) {
    auto file_format = GetParam();
    if (file_format == "lance" || file_format == "avro") {
        return;
    }
    // only f2 has index
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/data_evolution_with_index.db/data_evolution_with_index";
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};

    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);
    {
        // first 4 records are two file with the same first row id with data evolution
        // last 2 rows only in one file
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 102, 2.1],
        ["Alice", 4, 104, 3.1],
        ["Bob", 6, 106, 4.1],
        ["David", 8, 108, 5.1],
        [null, null, 202, 6.1],
        [null, null, 204, 7.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array));
    }
    {
        // first 4 records read with data evolution, ignore index
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(102));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 102, 2.1],
        ["Alice", 4, 104, 3.1],
        ["Bob", 6, 106, 4.1],
        ["David", 8, 108, 5.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        // f2 has bitmap index, but data evolution scan and read ignore index
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(103));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 102, 2.1],
        ["Alice", 4, 104, 3.1],
        ["Bob", 6, 106, 4.1],
        ["David", 8, 108, 5.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        // f2 has bitmap index, data evolution scan will ignore index => not empty plan
        // data evolution split read will also ignore index => not empty read batch
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(203));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        [null, null, 202, 6.1],
        [null, null, 204, 7.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate,
                              /*row_ranges=*/{},
                              /*check_scan_plan_when_empty_result=*/true));
    }
    {
        // f2 has bitmap index, data evolution split read will ignore index
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(202));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        [null, null, 202, 6.1],
        [null, null, 204, 7.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING);
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        [null, null, 202, 6.1],
        [null, null, 204, 7.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        // test row id with predicate
        std::vector<Range> row_ranges = {Range(0l, 2l)};
        // row id = {0, 1, 2}, while data evolution split read will ignore index
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(106));
        CheckScanResult(table_path, /*predicate=*/predicate, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 0}, /*expected_row_counts=*/{4, 4});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 102, 2.1],
        ["Alice", 4, 104, 3.1],
        ["Bob", 6, 106, 4.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test row id with predicate
        std::vector<Range> row_ranges = {Range(4l, 5l)};
        // row id = {4, 5}, data evolution split read will ignore bitmap index
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(204));
        CheckScanResult(table_path, /*predicate=*/predicate, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{4}, /*expected_row_counts=*/{2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        [null, null, 202, 6.1],
        [null, null, 204, 7.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate,
                              /*row_ranges=*/row_ranges));
    }
}

TEST_P(DataEvolutionTableTest, TestPredicate) {
    if (GetParam() == "lance" || GetParam() == "avro") {
        // lance and avro do not have stats
        return;
    }
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1, f2
    std::vector<std::string> write_cols0 = schema->field_names();
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f2
    std::vector<std::string> write_cols1 = {"f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(commit_msgs, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));
    {
        // test no predicate
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "c"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));
    }
    {
        // test predicate with f2
        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                       Literal(FieldType::STRING, "b", 1));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "c"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array, predicate));
    }
    {
        // test predicate with f1
        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                       Literal(FieldType::STRING, "a", 1));
        ASSERT_OK(
            ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr, predicate));
    }
    {
        // test predicate with f2
        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                       Literal(FieldType::STRING, "c", 1));
        ASSERT_OK(
            ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr, predicate));
    }
}

TEST_P(DataEvolutionTableTest, TestIOException) {
    if (GetParam() == "lance") {
        return;
    }
    std::string table_path;
    // write and commit with I/O exception
    bool write_run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 2000; i += paimon::test::RandomNumber(20, 30)) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        dir_ = UniqueTestDirectory::Create("local");
        CreateTable();
        table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
        auto schema = arrow::schema(fields_);

        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        // write field: f0, f1, f2
        std::vector<std::string> write_cols0 = schema->field_names();
        auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [10, "a", "b"],
        [20, "aa", "bb"],
        [23, "aaa", "bbb"]
    ])")
                .ValueOrDie());
        auto commit_msgs0_result = WriteArray(table_path, write_cols0, src_array0);
        CHECK_HOOK_STATUS(commit_msgs0_result.status(), i);
        CHECK_HOOK_STATUS(Commit(table_path, commit_msgs0_result.value()), i);

        // write field: f2, f0
        std::vector<std::string> write_cols1 = {"f2", "f0"};
        auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2], fields_[0]}), R"([
        ["c", 100],
        ["cc", 200],
        ["ccc", 300]
    ])")
                .ValueOrDie());
        auto commit_msgs1_result = WriteArray(table_path, write_cols1, src_array1);
        CHECK_HOOK_STATUS(commit_msgs1_result.status(), i);
        SetFirstRowId(/*reset_first_row_id=*/0,
                      const_cast<std::vector<std::shared_ptr<paimon::CommitMessage>>&>(
                          commit_msgs1_result.value()));
        CHECK_HOOK_STATUS(Commit(table_path, commit_msgs1_result.value()), i);
        write_run_complete = true;
        break;
    }
    ASSERT_TRUE(write_run_complete);

    // scan and read with I/O exception
    bool read_run_complete = false;
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[1], fields_[0], SpecialFields::SequenceNumber().field_,
                            SpecialFields::RowId().field_, fields_[2]}),
            R"([
        ["a", 100, 2, 0, "c"],
        ["aa", 200, 2, 1, "cc"],
        ["aaa", 300, 2, 2, "ccc"]
    ])")
            .ValueOrDie());

    for (size_t i = 0; i < 2000; i++) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        CHECK_HOOK_STATUS(ScanAndRead(table_path, {"f1", "f0", "_SEQUENCE_NUMBER", "_ROW_ID", "f2"},
                                      expected_array),
                          i);
        read_run_complete = true;
        break;
    }
    ASSERT_TRUE(read_run_complete);
}

TEST_P(DataEvolutionTableTest, TestWithRowIds) {
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, GetParam()},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};
    CreateTable(/*partition_keys=*/{}, options);

    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // turn 0: write field: f0, f1
    std::vector<std::string> write_cols = {"f0", "f1"};
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [0, "a"],
        [1, "b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs0, WriteArray(table_path, write_cols, src_array0));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs0);
    ASSERT_OK(Commit(table_path, commit_msgs0));

    // turn 1: write field: f0, f1
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [2, "c"],
        [3, "d"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/2, commit_msgs1);
    ASSERT_OK(Commit(table_path, commit_msgs1));

    // turn 2: write field: f0, f1
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [4, "e"],
        [5, "f"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols, src_array2));
    SetFirstRowId(/*reset_first_row_id=*/4, commit_msgs2);
    ASSERT_OK(Commit(table_path, commit_msgs2));

    {
        // test without row ids
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/{},
                        /*expected_first_row_ids=*/{0, 2, 4}, /*expected_row_counts=*/{2, 2, 2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [0, "a", null],
        [1, "b", null],
        [2, "c", null],
        [3, "d", null],
        [4, "e", null],
        [5, "f", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));
    }
    {
        // test row ids in first file
        std::vector<Range> row_ranges = {Range(0l, 1l)};
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0}, /*expected_row_counts=*/{2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [0, "a", null],
        [1, "b", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr, /*row_ranges=*/row_ranges));
    }
    {
        // test row ids in last file
        std::vector<Range> row_ranges = {Range(4l, 4l)};
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{4}, /*expected_row_counts=*/{2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [4, "e", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr, /*row_ranges=*/row_ranges));
    }
    {
        // test row ids in multiple files
        std::vector<Range> row_ranges = {Range(1l, 1l), Range(4l, 4l)};
        CheckScanResult(table_path, /*predicate=*/nullptr,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 4}, /*expected_row_counts=*/{2, 2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "b", null],
        [4, "e", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test all row ids
        std::vector<Range> row_ranges = {Range(0l, 5l)};
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 2, 4}, /*expected_row_counts=*/{2, 2, 2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [0, "a", null],
        [1, "b", null],
        [2, "c", null],
        [3, "d", null],
        [4, "e", null],
        [5, "f", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr, /*row_ranges=*/row_ranges));
    }
    {
        // test unordered row ids
        std::vector<Range> row_ranges = {Range(5l, 5l), Range(3l, 3l)};
        CheckScanResult(table_path, /*predicate=*/nullptr,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{2, 4}, /*expected_row_counts=*/{2, 2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [3, "d", null],
        [5, "f", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test row ids which partially exist
        std::vector<Range> row_ranges = {Range(100l, 100l), Range(5l, 5l), Range(3l, 3l)};
        CheckScanResult(table_path, /*predicate=*/nullptr,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{2, 4}, /*expected_row_counts=*/{2, 2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [3, "d", null],
        [5, "f", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test row ids which do not exist
        std::vector<Range> row_ranges = {Range(100l, 100l), Range(200l, 200l)};
        CheckScanResult(table_path, /*predicate=*/nullptr,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{}, /*expected_row_counts=*/{});
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
    if (GetParam() == "lance" || GetParam() == "avro") {
        // as lance and avro do not support stats
        return;
    }
    {
        // test row id with predicate
        // row ids {0, 1, 5} and predicate f0 = 2
        // scan will filter out all files
        std::vector<Range> row_ranges = {Range(0l, 0l), Range(1l, 1l), Range(5l, 5l)};
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::INT, Literal(2));
        CheckScanResult(table_path, /*predicate=*/predicate,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{}, /*expected_row_counts=*/{});
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr,
                              predicate,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test row id with predicate
        // row ids {0, 1, 5} and predicate f0 = 5
        // scan will filter out file0 and file1
        std::vector<Range> row_ranges = {Range(0l, 0l), Range(1l, 1l), Range(5l, 5l)};
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::INT, Literal(5));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [5, "f", null]
    ])")
                .ValueOrDie());
        CheckScanResult(table_path, /*predicate=*/predicate,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{4}, /*expected_row_counts=*/{2});
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array, predicate,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test with row tracking fields
        std::vector<Range> row_ranges = {Range(0l, 0l), Range(1l, 1l), Range(5l, 5l)};
        CheckScanResult(table_path, /*predicate=*/nullptr,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 4}, /*expected_row_counts=*/{2, 2});

        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({fields_[1], fields_[0], SpecialFields::SequenceNumber().field_,
                                SpecialFields::RowId().field_, fields_[2]}),
                R"([
        ["a", 0, 1, 0, null],
        ["b", 1, 1, 1, null],
        ["f", 5, 3, 5, null]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f1", "f0", "_SEQUENCE_NUMBER", "_ROW_ID", "f2"},
                              expected_array, /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
}

std::vector<std::string> GetTestValuesForDataEvolutionTableTest() {
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

INSTANTIATE_TEST_SUITE_P(FileFormat, DataEvolutionTableTest,
                         ::testing::ValuesIn(GetTestValuesForDataEvolutionTableTest()));

}  // namespace paimon::test
