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

#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_utils.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/data/blob.h"
#include "paimon/defs.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/data_split.h"
#include "paimon/table/source/startup_mode.h"
#include "paimon/table/source/table_read.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"
namespace paimon {
class DataSplit;
class RecordBatch;
}  // namespace paimon

namespace paimon::test {
class BlobTableInteTest : public testing::Test, public ::testing::WithParamInterface<std::string> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create("local");
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
        const std::vector<std::shared_ptr<arrow::Array>>& write_arrays) const {
        // write
        WriteContextBuilder write_builder(table_path, "commit_user_1");
        write_builder.WithWriteSchema(write_cols);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto file_store_write,
                               FileStoreWrite::Create(std::move(write_context)));

        for (const auto& write_array : write_arrays) {
            ArrowArray c_array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*write_array, &c_array));
            auto record_batch = std::make_unique<RecordBatch>(
                partition, /*bucket=*/0,
                /*row_kinds=*/std::vector<RecordBatch::RowKind>(), &c_array);
            PAIMON_RETURN_NOT_OK(file_store_write->Write(std::move(record_batch)));
        }
        PAIMON_ASSIGN_OR_RAISE(auto commit_msgs,
                               file_store_write->PrepareCommit(
                                   /*wait_compaction=*/false, /*commit_identifier=*/0));
        PAIMON_RETURN_NOT_OK(file_store_write->Close());
        return commit_msgs;
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
                       const std::vector<Range>& row_ranges = {}) const {
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
        if (!expected_array) {
            EXPECT_TRUE(result_plan->Splits().empty());
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
            EXPECT_FALSE(read_result);
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
        EXPECT_TRUE(expected_chunk_array->Equals(read_result))
            << "result:" << read_result->ToString() << std::endl
            << "expected:" << expected_chunk_array->ToString();
        return Status::OK();
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
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> dir_;
    arrow::FieldVector fields_ = {arrow::field("f0", arrow::int32()), BlobUtils::ToArrowField("f1"),
                                  arrow::field("f2", arrow::utf8())};
};

std::vector<std::string> GetTestValuesForBlobTableInteTest() {
    std::vector<std::string> values = {"parquet"};
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

INSTANTIATE_TEST_SUITE_P(FileFormat, BlobTableInteTest,
                         ::testing::ValuesIn(GetTestValuesForBlobTableInteTest()));

TEST_P(BlobTableInteTest, TestAppendTableWriteWithBlobAsDescriptorTrue) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32()),
                                 BlobUtils::ToArrowField("blob", true)};
    auto schema = arrow::schema(fields);

    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},       {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "700"},      {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"}, {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_AS_DESCRIPTOR, "true"},   {Options::FILE_SYSTEM, "local"}};

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;

    auto generate_blob_array = [&](const std::vector<PAIMON_UNIQUE_PTR<Bytes>>& blob_descriptors)
        -> std::shared_ptr<arrow::Array> {
        arrow::StructBuilder struct_builder(
            arrow::struct_(fields), arrow::default_memory_pool(),
            {std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int32Builder>(),
             std::make_shared<arrow::LargeBinaryBuilder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        auto int_builder = static_cast<arrow::Int32Builder*>(struct_builder.field_builder(1));
        auto binary_builder =
            static_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(2));
        for (size_t i = 0; i < blob_descriptors.size(); ++i) {
            EXPECT_TRUE(struct_builder.Append().ok());
            EXPECT_TRUE(string_builder->Append("str_" + std::to_string(i)).ok());
            if (i % 3 == 0) {
                // test null
                EXPECT_TRUE(int_builder->AppendNull().ok());
            } else {
                EXPECT_TRUE(int_builder->Append(i).ok());
            }
            EXPECT_TRUE(
                binary_builder->Append(blob_descriptors[i]->data(), blob_descriptors[i]->size())
                    .ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    };

    // prepare data
    std::vector<PAIMON_UNIQUE_PTR<Bytes>> expected_blob_descriptors;
    std::string file1 = paimon::test::GetDataDir() + "/avro/data/avro_with_null";
    ASSERT_OK_AND_ASSIGN(auto blob1, Blob::FromPath(file1));
    expected_blob_descriptors.emplace_back(blob1->ToDescriptor(pool_));

    std::string file2 = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(auto blob2, Blob::FromPath(file2, /*offset=*/0, /*length=*/91));
    expected_blob_descriptors.emplace_back(blob2->ToDescriptor(pool_));
    ASSERT_OK_AND_ASSIGN(auto blob3, Blob::FromPath(file2, /*offset=*/92, /*length=*/85));
    expected_blob_descriptors.emplace_back(blob3->ToDescriptor(pool_));
    ASSERT_OK_AND_ASSIGN(auto blob4, Blob::FromPath(file2, /*offset=*/300, /*length=*/3000));
    expected_blob_descriptors.emplace_back(blob4->ToDescriptor(pool_));

    auto array = generate_blob_array(expected_blob_descriptors);
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto schema_with_row_kind = arrow::schema(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
        [0, "str_0", null],
        [0, "str_1", 1],
        [0, "str_2", 2],
        [0, "str_3", null]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResultForBlobTable(
                                           schema_with_row_kind, data_splits, expected_data,
                                           expected_blob_descriptors));
    ASSERT_TRUE(success);
}

TEST_P(BlobTableInteTest, TestAppendTableWriteWithBlobAsDescriptorFalse) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32()),
                                 BlobUtils::ToArrowField("blob", true)};
    auto schema = arrow::schema(fields);

    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},       {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "700"},      {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"}, {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_AS_DESCRIPTOR, "false"},  {Options::FILE_SYSTEM, "local"}};

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;

    std::string data = R"([
        ["str_0", null, "apple"],
        ["str_1", 1,    "banana"],
        ["str_2", 2,    "cat"],
        ["str_3", null, "dog"]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));

    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    std::string expected_data = R"([
        [0, "str_0", null, "apple"],
        [0, "str_1", 1,    "banana"],
        [0, "str_2", 2,    "cat"],
        [0, "str_3", null, "dog"]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(BlobTableInteTest, TestBasic) {
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

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, {}, write_cols0, {src_array0}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f1, f2
    std::vector<std::string> write_cols1 = {"f1", "f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[1], fields_[2]}), R"([
        ["new_blob", "c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(commit_msgs, WriteArray(table_path, {}, write_cols1, {src_array1}));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "new_blob", "c"]
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
        ["new_blob", 1, 2, 0, "c"]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f1", "f0", "_SEQUENCE_NUMBER", "_ROW_ID", "f2"},
                              expected_row_tracking_array));
    }
}

TEST_P(BlobTableInteTest, TestMultipleAppends) {
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
    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, {}, write_cols0, {src_array0}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f0, f1
    std::vector<std::string> write_cols1 = {"f0", "f1"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [1, "a"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, {}, write_cols1, {src_array1}));
    SetFirstRowId(10, commit_msgs1);

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, {}, write_cols2, {src_array2}));
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
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, WriteArray(table_path, {}, write_cols3, {src_array3}));
    SetFirstRowId(11, commit_msgs3);
    ASSERT_OK(Commit(table_path, commit_msgs3));

    // write field: f2
    std::vector<std::string> write_cols4 = {"f2"};
    auto src_array4 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["d"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs4, WriteArray(table_path, {}, write_cols4, {src_array4}));
    SetFirstRowId(11, commit_msgs4);
    ASSERT_OK(Commit(table_path, commit_msgs4));

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

TEST_P(BlobTableInteTest, TestOnlySomeColumns) {
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
    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, {}, write_cols0, {src_array0}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f1
    std::vector<std::string> write_cols1 = {"f1"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[1]}), R"([
        ["a"]
    ])")
            .ValueOrDie());
    ASSERT_NOK_WITH_MSG(WriteArray(table_path, {}, write_cols1, {src_array1}),
                        "Can't infer struct array length with 0 child arrays");
}

TEST_P(BlobTableInteTest, TestNullValues) {
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
    ASSERT_NOK_WITH_MSG(WriteArray(table_path, {}, write_cols1, {src_array1}),
                        "BlobFormatWriter only support non-null blob.");
}

TEST_P(BlobTableInteTest, TestMultipleAppendsDifferentFirstRowIds) {
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
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, {}, write_cols1, {src_array1}));
    SetFirstRowId(0, commit_msgs1);

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, {}, write_cols2, {src_array2}));
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
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, WriteArray(table_path, {}, write_cols3, {src_array3}));
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
    ASSERT_OK_AND_ASSIGN(auto commit_msgs4, WriteArray(table_path, {}, write_cols4, {src_array4}));
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

TEST_P(BlobTableInteTest, TestMoreDataWithDataEvolution) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1
    std::vector<std::string> write_cols1 = {"f0", "f1"};
    // row0: 0, a0; row1: 1, a1 ...
    auto src_array1 = PrepareBulkData(
        10000, [](int32_t i) { return std::to_string(i) + ", \"a" + std::to_string(i) + "\""; },
        {fields_[0], fields_[1]});
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, {}, write_cols1, {src_array1}));
    SetFirstRowId(0, commit_msgs1);

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    // row0: b0; row1: b1 ...
    auto src_array2 = PrepareBulkData(
        10000, [](int32_t i) { return "\"b" + std::to_string(i) + "\""; }, {fields_[2]});
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, {}, write_cols2, {src_array2}));
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
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, WriteArray(table_path, {}, write_cols3, {src_array3}));
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

TEST_P(BlobTableInteTest, TestBlobWriteMultiRound) {
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},        {Options::FILE_FORMAT, GetParam()},
        {Options::FILE_SYSTEM, "local"},          {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::BLOB_TARGET_FILE_SIZE, "1000"}, {Options::TARGET_FILE_SIZE, "100"},
        {Options::DATA_EVOLUTION_ENABLED, "true"}};
    CreateTable(/*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1, f2
    std::vector<std::string> write_cols = schema->field_names();
    int32_t offset = 0;
    auto array_1 = PrepareBulkData(5000,
                                   [offset](int32_t i) {
                                       return std::to_string(offset + i) + ", \"a" +
                                              std::to_string(offset + i) + "\", \"c" +
                                              std::to_string(offset + i) + "\"";
                                   },
                                   {fields_[0], fields_[1], fields_[2]});

    offset = 5000;
    auto array_2 = PrepareBulkData(5000,
                                   [offset](int32_t i) {
                                       return std::to_string(offset + i) + ", \"a" +
                                              std::to_string(offset + i) + "\", \"c" +
                                              std::to_string(offset + i) + "\"";
                                   },
                                   {fields_[0], fields_[1], fields_[2]});

    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, {}, write_cols, {array_1, array_2}));
    ASSERT_OK(Commit(table_path, commit_msgs));
    auto concat_array = arrow::Concatenate({array_1, array_2}).ValueOrDie();
    auto expect_array = std::dynamic_pointer_cast<arrow::StructArray>(concat_array);
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expect_array));
}

TEST_P(BlobTableInteTest, TestExternalPath) {
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
        {Options::DATA_FILE_EXTERNAL_PATHS, "FILE://" + external_test_dir}};
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

    ASSERT_OK_AND_ASSIGN(auto commit_msgs0, WriteArray(table_path, {}, write_cols0, {src_array0}));
    ASSERT_OK(Commit(table_path, commit_msgs0));

    // write field: f0, f2
    std::vector<std::string> write_cols1 = {"f0", "f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[2]}), R"([
        [10, "b"],
        [20, "d"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, {}, write_cols1, {src_array1}));
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

TEST_P(BlobTableInteTest, TestPartitionWithPredicate) {
    auto file_format = GetParam();
    if (file_format == "lance") {
        return;
    }
    std::vector<std::string> partition_keys = {"f0"};
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},         {Options::FILE_FORMAT, GetParam()},
        {Options::FILE_SYSTEM, "local"},           {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"}, {"parquet.write.max-row-group-length", "1"}};

    CreateTable(partition_keys, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1 for partition f0 = "11"
    std::vector<std::string> write_cols0 = {"f0", "f1"};
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [11, "2024"],
        [11, "2025"],
        [11, "2026"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs0,
                         WriteArray(table_path, {{"f0", "11"}}, write_cols0, {src_array0}));

    // write field: f2 for partition f0 = "11"
    std::vector<std::string> write_cols1 = {"f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["a"],
        ["b"],
        ["c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         WriteArray(table_path, {{"f0", "11"}}, write_cols1, {src_array1}));
    std::vector<std::shared_ptr<CommitMessage>> total_msgs;
    total_msgs.insert(total_msgs.end(), commit_msgs0.begin(), commit_msgs0.end());
    total_msgs.insert(total_msgs.end(), commit_msgs1.begin(), commit_msgs1.end());
    SetFirstRowId(0, total_msgs);
    ASSERT_OK(Commit(table_path, total_msgs));

    // write field: f0, f1 for partition f0 = "22"
    std::vector<std::string> write_cols3 = {"f0", "f1"};
    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [22, "2027"],
        [22, "2028"],
        [22, "2029"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs3,
                         WriteArray(table_path, {{"f0", "22"}}, write_cols3, {src_array3}));
    SetFirstRowId(3, commit_msgs3);
    ASSERT_OK(Commit(table_path, commit_msgs3));
    {
        // only set data field predicate, file with field all null will be returned
        auto equal = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                             FieldType::STRING, Literal(FieldType::STRING, "a", 1));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [11, "2024", "a"],
        [11, "2025", "b"],
        [11, "2026", "c"],
        [22, "2027", null],
        [22, "2028", null],
        [22, "2029", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array, equal));
    }
    {
        // only set partition predicate
        auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                             Literal(11));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [11, "2024", "a"],
        [11, "2025", "b"],
        [11, "2026", "c"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array, equal));
    }
    {
        // set partition predicate and data field predicate, blob type not support predicate
        auto equal =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                    Literal(FieldType::BLOB, "2024", 4));
        auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                                          FieldType::INT, Literal(100));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({equal, greater_than}));
        ASSERT_NOK_WITH_MSG(
            ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr, predicate),
            "Invalid type large_binary for predicate");
    }
    {
        // read with row tracking
        auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                             Literal(11));

        auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_,
                                SpecialFields::SequenceNumber().field_}),
                R"([
        [11, "2024", "a", 0, 1],
        [11, "2025", "b", 1, 1],
        [11, "2026", "c", 2, 1]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                              expected_row_tracking_array, equal));
    }
}

TEST_P(BlobTableInteTest, TestPredicate) {
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

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, {}, write_cols0, {src_array0}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f2
    std::vector<std::string> write_cols1 = {"f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(commit_msgs, WriteArray(table_path, {}, write_cols1, {src_array1}));
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
        ASSERT_NOK_WITH_MSG(
            ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr, predicate),
            "Invalid type large_binary for predicate");
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

TEST_P(BlobTableInteTest, TestIOException) {
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
        auto commit_msgs0_result = WriteArray(table_path, {}, write_cols0, {src_array0});
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
        auto commit_msgs1_result = WriteArray(table_path, {}, write_cols1, {src_array1});
        CHECK_HOOK_STATUS(commit_msgs1_result.status(), i);
        SetFirstRowId(/*reset_first_row_id=*/0,
                      const_cast<std::vector<std::shared_ptr<paimon::CommitMessage>>&>(
                          commit_msgs1_result.value()));
        CHECK_HOOK_STATUS(Commit(table_path, commit_msgs1_result.value()), i);
        write_run_complete = true;
        break;
    }
    ASSERT_TRUE(write_run_complete);

    // scan and read with I / O exception
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

TEST_P(BlobTableInteTest, TestReadTableWithDenseStats) {
    auto file_format = GetParam();
    if (file_format == "lance" || file_format == "avro") {
        return;
    }
    std::string table_path =
        paimon::test::GetDataDir() + file_format +
        "/blob_data_evolution_with_dense_stats.db/blob_data_evolution_with_dense_stats/";
    std::vector<DataField> read_fields = {DataField(0, BlobUtils::ToArrowField("f0")),
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
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::BLOB,
                                    Literal(FieldType::BLOB, "Alice", 5));
        ASSERT_NOK_WITH_MSG(
            ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                        expected_array, predicate),
            "Invalid type large_binary for predicate");
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
}

TEST_P(BlobTableInteTest, TestDataEvolutionAndAlterTable) {
    auto file_format = GetParam();
    if (file_format == "lance" || file_format == "avro") {
        return;
    }
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/blob_append_table_alter_table_with_cast_with_data_evolution.db/"
                             "blob_append_table_alter_table_with_cast_with_data_evolution/";
    std::vector<DataField> read_fields = {
        DataField(6, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(0, arrow::field("key0", arrow::int32())),
        DataField(1, arrow::field("key1", arrow::int32())),
        DataField(2, arrow::field("f3", arrow::int32())),
        DataField(3, arrow::field("f1", arrow::utf8())),
        DataField(4, arrow::field("f2", arrow::decimal128(6, 3))),
        DataField(5, arrow::field("f0", arrow::boolean())),
        DataField(8, BlobUtils::ToArrowField("f5")),
        DataField(9, arrow::field("f6", arrow::int32())),
        SpecialFields::RowId(),
        SpecialFields::SequenceNumber()};

    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    {
        // only read blob column
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({BlobUtils::ToArrowField("f5")}), R"([
            ["Lily"],
            ["Alice"],
            ["Bob"],
            ["Cindy"],
            ["Dave"],
            ["Apple"],
            ["Banana"],
            ["Cherry"],
            ["Durian"],
            ["Elderberry"]
        ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, {"f5"}, expected_array));
    }
    {
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
["1970-01-05T00:00", 0, 1, 100, "2024-11-26 06:38:56.001000001", "0.020", true, "Lily", null, 0, 1],
["1969-11-18T00:00", 0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120", true, "Alice", null, 1, 1],
["1971-03-21T00:00", 0, 1, 120, "2024-11-26 06:38:56.021000021", "22.220", false, "Bob", null, 2, 1],
["1957-11-01T00:00", 0, 1, 130, "2024-11-26 06:38:56.031000031", "333.320", false, "Cindy", null, 3, 1],
["2091-09-07T00:00", 0, 1, 140, "2024-11-26 06:38:56.041000041", "444.420", true, "Dave", null, 4, 1],
["2024-11-26T06:38:56.054000154", 0, 1, 150, "2024-11-26 15:28:31", "55.002", true, "Apple", 56, 5, 3],
["2024-11-26T06:38:56.064000164", 0, 1, 160, "2024-11-26 15:28:41", "666.012", false, "Banana", 66, 6, 3],
["2024-11-26T06:38:56.074000174", 0, 1, 170, "2024-11-26 15:28:51", "-77.022", true, "Cherry", 76, 7, 3],
["2024-11-26T06:38:56.084000184", 0, 1, 180, "2024-11-26 15:29:01", "8.032", true, "Durian", -86, 8, 3],
["2024-11-26T06:38:56.094000194", 0, 1, 190, "I'm strange", "-999.420", false, "Elderberry", 96, 9, 3]
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
["1970-01-05T00:00", 0, 1, 100, "2024-11-26 06:38:56.001000001", "0.020", true, "Lily", null, 0, 1],
["1969-11-18T00:00", 0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120", true, "Alice", null, 1, 1],
["1971-03-21T00:00", 0, 1, 120, "2024-11-26 06:38:56.021000021", "22.220", false, "Bob", null, 2, 1],
["1957-11-01T00:00", 0, 1, 130, "2024-11-26 06:38:56.031000031", "333.320", false, "Cindy", null, 3, 1],
["2091-09-07T00:00", 0, 1, 140, "2024-11-26 06:38:56.041000041", "444.420", true, "Dave", null, 4, 1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        // files with schema-0 will be reserved as f6 does not exist in schema-0 (null count =
        // null), files with schema-1 will also be reserved
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/8, /*field_name=*/"f6", FieldType::INT);
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
["1970-01-05T00:00", 0, 1, 100, "2024-11-26 06:38:56.001000001", "0.020", true, "Lily", null, 0, 1],
["1969-11-18T00:00", 0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120", true, "Alice", null, 1, 1],
["1971-03-21T00:00", 0, 1, 120, "2024-11-26 06:38:56.021000021", "22.220", false, "Bob", null, 2, 1],
["1957-11-01T00:00", 0, 1, 130, "2024-11-26 06:38:56.031000031", "333.320", false, "Cindy", null, 3, 1],
["2091-09-07T00:00", 0, 1, 140, "2024-11-26 06:38:56.041000041", "444.420", true, "Dave", null, 4, 1],
["2024-11-26T06:38:56.054000154", 0, 1, 150, "2024-11-26 15:28:31", "55.002", true, "Apple", 56, 5, 3],
["2024-11-26T06:38:56.064000164", 0, 1, 160, "2024-11-26 15:28:41", "666.012", false, "Banana", 66, 6, 3],
["2024-11-26T06:38:56.074000174", 0, 1, 170, "2024-11-26 15:28:51", "-77.022", true, "Cherry", 76, 7, 3],
["2024-11-26T06:38:56.084000184", 0, 1, 180, "2024-11-26 15:29:01", "8.032", true, "Durian", -86, 8, 3],
["2024-11-26T06:38:56.094000194", 0, 1, 190, "I'm strange", "-999.420", false, "Elderberry", 96, 9, 3]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
}

TEST_P(BlobTableInteTest, TestWithRowIdsSimple) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    std::vector<std::string> write_cols = {"f0", "f1", "f2"};
    // row0: 0, "a0", "b0"; row1: 1, "a1", "b1" ...
    auto src_array = PrepareBulkData(
        1000,
        [](int32_t i) {
            return std::to_string(i) + ", \"a" + std::to_string(i) + "\", \"b" + std::to_string(i) +
                   "\"";
        },
        fields_);
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, /*partition=*/{}, write_cols, {src_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [10, "a10", "b10"],
        [999, "a999", "b999"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                          /*predicate=*/nullptr,
                          /*row_ranges=*/{Range(999l, 999l), Range(10l, 10l)}));
}

TEST_P(BlobTableInteTest, TestWithRowIdsForMultipleBlobFiles) {
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format},
                                                  {Options::TARGET_FILE_SIZE, "1000"},
                                                  {Options::BLOB_TARGET_FILE_SIZE, "80"},
                                                  {Options::BUCKET, "-1"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"},
                                                  {Options::BLOB_AS_DESCRIPTOR, "false"},
                                                  {Options::FILE_SYSTEM, "local"}};
    CreateTable(/*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // target blob size is 80, therefore, each 4 rows in blob will be in one file
    std::vector<std::string> write_cols = {"f0", "f1", "f2"};
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [0, "aaa0", "b0"],
        [1, "aaa1", "b1"],
        [2, "aaa2", "b2"],
        [3, "aaa3", "b3"],
        [4, "aaa4", "b4"],
        [5, "aaa5", "b5"],
        [6, "aaa6", "b6"],
        [7, "aaa7", "b7"],
        [8, "aaa8", "b8"],
        [9, "aaa9", "b9"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, /*partition=*/{}, write_cols, {src_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    {
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "aaa1", "b1"],
        [8, "aaa8", "b8"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/{Range(1l, 1l), Range(8l, 8l)}));
    }
    {
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "aaa1", "b1"],
        [5, "aaa5", "b5"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/{Range(1l, 1l), Range(5l, 5l)}));
    }
    {
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [5, "aaa5", "b5"],
        [6, "aaa6", "b6"],
        [8, "aaa8", "b8"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/{Range(5l, 6l), Range(8l, 8l)}));
    }
    {
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [5, "aaa5", "b5"],
        [7, "aaa7", "b7"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/{Range(5l, 5l), Range(7l, 7l)}));
    }
    {
        // f0 0-9 in one file, predicate does not skip any data file
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::INT, Literal(5));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "aaa1", "b1"],
        [5, "aaa5", "b5"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array, predicate,
                              /*row_ranges=*/{Range(1l, 1l), Range(5l, 5l)}));
    }
    {
        // test ont read blob field
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[1]}), R"([
        ["aaa1"],
        ["aaa8"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, {"f1"}, expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/{Range(1l, 1l), Range(8l, 8l)}));
    }
}

}  // namespace paimon::test
