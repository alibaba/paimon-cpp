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
#include <numeric>
#include <set>
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

struct ReadResult {
    std::unique_ptr<BatchReader> batch_reader;
    std::shared_ptr<arrow::ChunkedArray> chunked_array;
};

class BlobTableInteTest : public testing::Test, public ::testing::WithParamInterface<std::string> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create("local");
        blob_dir_ = UniqueTestDirectory::Create("local");
    }

    void TearDown() override {
        dir_.reset();
    }

    void CreateTable(const std::vector<std::string>& partition_keys,
                     const std::map<std::string, std::string>& options) const {
        CreateTable(fields_, partition_keys, options);
    }

    void CreateTable(const arrow::FieldVector& fields,
                     const std::vector<std::string>& partition_keys,
                     const std::map<std::string, std::string>& options) const {
        auto schema = arrow::schema(fields);
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

    /// Scan table and return the plan (without reading data).
    Result<std::shared_ptr<Plan>> ScanTable(const std::string& table_path,
                                            const std::shared_ptr<Predicate>& predicate = nullptr,
                                            const std::vector<Range>& row_ranges = {}) const {
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.SetPredicate(predicate);
        if (!row_ranges.empty()) {
            auto global_index_result = BitmapGlobalIndexResult::FromRanges(row_ranges);
            scan_context_builder.SetGlobalIndexResult(global_index_result);
        }
        PAIMON_ASSIGN_OR_RAISE(auto scan_context, scan_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_scan, TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(auto result_plan, table_scan->CreatePlan());
        return result_plan;
    }

    /// Read from table using a pre-scanned plan, returning the ChunkedArray and batch_reader.
    /// The batch_reader must outlive the returned ChunkedArray (array memory depends on reader).
    Result<ReadResult> ReadTable(const std::string& table_path,
                                 const std::vector<std::string>& read_schema,
                                 const std::shared_ptr<Plan>& plan,
                                 const std::shared_ptr<Predicate>& predicate = nullptr,
                                 const std::map<std::string, std::string>& options = {}) const {
        auto splits = plan->Splits();
        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.SetReadSchema(read_schema).SetPredicate(predicate);
        if (!options.empty()) {
            read_context_builder.SetOptions(options);
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context,
                               read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(auto batch_reader, table_read->CreateReader(splits));
        PAIMON_ASSIGN_OR_RAISE(auto read_result,
                               ReadResultCollector::CollectResult(batch_reader.get()));
        return ReadResult{std::move(batch_reader), std::move(read_result)};
    }

    /// Convenience: scan + read in one call.
    Result<ReadResult> ScanAndReadResult(const std::string& table_path,
                                         const std::vector<std::string>& read_schema,
                                         const std::shared_ptr<Predicate>& predicate = nullptr,
                                         const std::vector<Range>& row_ranges = {}) const {
        PAIMON_ASSIGN_OR_RAISE(auto result_plan, ScanTable(table_path, predicate, row_ranges));
        return ReadTable(table_path, read_schema, result_plan, predicate);
    }

    /// Prepend a _VALUE_KIND (Insert) column to a StructArray.
    static Result<std::shared_ptr<arrow::StructArray>> PrependRowKindColumn(
        const std::shared_ptr<arrow::StructArray>& array) {
        auto row_kind_scalar =
            std::make_shared<arrow::Int8Scalar>(RowKind::Insert()->ToByteValue());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto row_kind_array, arrow::MakeArrayFromScalar(*row_kind_scalar, array->length()));
        arrow::ArrayVector fields_with_row_kind = array->fields();
        std::vector<std::string> names_with_row_kind =
            arrow::schema(array->type()->fields())->field_names();
        fields_with_row_kind.insert(fields_with_row_kind.begin(), row_kind_array);
        names_with_row_kind.insert(names_with_row_kind.begin(), "_VALUE_KIND");
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto result, arrow::StructArray::Make(fields_with_row_kind, names_with_row_kind));
        return std::dynamic_pointer_cast<arrow::StructArray>(result);
    }

    Status ScanAndRead(const std::string& table_path, const std::vector<std::string>& read_schema,
                       const std::shared_ptr<arrow::StructArray>& expected_array,
                       const std::shared_ptr<Predicate>& predicate = nullptr,
                       const std::vector<Range>& row_ranges = {}) const {
        PAIMON_ASSIGN_OR_RAISE(auto scan_read,
                               ScanAndReadResult(table_path, read_schema, predicate, row_ranges));

        if (!expected_array) {
            EXPECT_FALSE(scan_read.chunked_array);
            return Status::OK();
        }
        PAIMON_ASSIGN_OR_RAISE(auto expected_with_row_kind, PrependRowKindColumn(expected_array));
        auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_with_row_kind);
        EXPECT_TRUE(expected_chunk_array->Equals(scan_read.chunked_array))
            << "result:" << scan_read.chunked_array->ToString() << std::endl
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
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), data_str)
                .ValueOrDie());
    }

    /// Convert a StructArray with raw blob bytes into a StructArray with serialized
    /// BlobDescriptor bytes. Each raw blob value is written to a temporary file, and
    /// the corresponding cell is replaced with the serialized BlobDescriptor pointing
    /// to that file.
    /// Common framework for transforming blob fields in a StructArray.
    /// Non-blob fields are kept as-is; blob fields are processed row-by-row via `transform_row`.
    /// `transform_row` receives (binary_value_view) and returns the transformed bytes via builder.
    using BlobRowTransform =
        std::function<Status(const std::string_view& value, arrow::LargeBinaryBuilder* builder)>;

    Result<std::shared_ptr<arrow::StructArray>> TransformBlobFields(
        const std::shared_ptr<arrow::StructArray>& input_array,
        const std::set<std::string>& blob_fields, BlobRowTransform transform_row) const {
        auto fields = input_array->type()->fields();
        arrow::ArrayVector child_arrays;

        for (const auto& field : fields) {
            auto col = input_array->GetFieldByName(field->name());
            if (blob_fields.count(field->name()) == 0) {
                child_arrays.push_back(col);
                continue;
            }
            const auto& binary_array =
                arrow::internal::checked_cast<const arrow::LargeBinaryArray&>(*col);
            arrow::LargeBinaryBuilder builder;
            for (int64_t i = 0; i < binary_array.length(); ++i) {
                if (binary_array.IsNull(i)) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
                    continue;
                }
                PAIMON_RETURN_NOT_OK(transform_row(binary_array.GetView(i), &builder));
            }
            std::shared_ptr<arrow::Array> result_col;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&result_col));
            child_arrays.push_back(result_col);
        }

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(auto result,
                                          arrow::StructArray::Make(child_arrays, fields));
        return result;
    }

    Result<std::shared_ptr<arrow::StructArray>> ConvertRawBlobToDescriptor(
        const std::shared_ptr<arrow::StructArray>& raw_array,
        const std::set<std::string>& blob_fields) {
        auto fs = std::make_shared<LocalFileSystem>();
        return TransformBlobFields(
            raw_array, blob_fields,
            [&](const std::string_view& raw_value, arrow::LargeBinaryBuilder* builder) -> Status {
                std::string file_path =
                    blob_dir_->Str() + "/blob_" + std::to_string(blob_file_counter_++) + ".bin";
                PAIMON_ASSIGN_OR_RAISE(auto out, fs->Create(file_path, /*overwrite=*/true));
                PAIMON_ASSIGN_OR_RAISE(
                    auto written,
                    out->Write(raw_value.data(), static_cast<uint32_t>(raw_value.size())));
                PAIMON_RETURN_NOT_OK(out->Flush());
                PAIMON_RETURN_NOT_OK(out->Close());
                if (static_cast<size_t>(written) != raw_value.size()) {
                    return Status::Invalid("Short write: expected {}, wrote {}", raw_value.size(),
                                           written);
                }
                PAIMON_ASSIGN_OR_RAISE(auto blob, Blob::FromPath(file_path));
                auto descriptor = blob->ToDescriptor(pool_);
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    builder->Append(descriptor->data(), descriptor->size()));
                return Status::OK();
            });
    }

    /// Convert a StructArray with serialized BlobDescriptor bytes back to a StructArray
    /// with raw blob bytes. Only blob fields are resolved; other columns (including
    /// _VALUE_KIND) are kept as-is.
    Result<std::shared_ptr<arrow::StructArray>> ConvertDescriptorToRawBlob(
        const std::shared_ptr<arrow::StructArray>& desc_array,
        const std::set<std::string>& blob_fields) const {
        auto fs = std::make_shared<LocalFileSystem>();
        return TransformBlobFields(
            desc_array, blob_fields,
            [&](const std::string_view& descriptor_bytes,
                arrow::LargeBinaryBuilder* builder) -> Status {
                PAIMON_ASSIGN_OR_RAISE(auto blob, Blob::FromDescriptor(descriptor_bytes.data(),
                                                                       descriptor_bytes.size()));
                PAIMON_ASSIGN_OR_RAISE(auto data, blob->ToData(fs, pool_));
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(data->data(), data->size()));
                return Status::OK();
            });
    }

    /// Verify DataFileMeta properties from a scan plan.
    /// Each vector element corresponds to one expected DataFileMeta (ordered by file index).
    static void VerifyDataFileMetas(
        const std::shared_ptr<Plan>& plan, size_t expected_file_count,
        const std::vector<int64_t>& expected_row_counts,
        const std::vector<int64_t>& expected_min_seqs,
        const std::vector<int64_t>& expected_max_seqs,
        const std::vector<int64_t>& expected_first_row_ids,
        const std::vector<std::optional<std::vector<std::string>>>& expected_write_cols) {
        std::vector<std::shared_ptr<DataFileMeta>> all_files;
        for (const auto& split : plan->Splits()) {
            auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
            ASSERT_TRUE(data_split);
            for (const auto& file : data_split->DataFiles()) {
                all_files.push_back(file);
            }
        }
        ASSERT_EQ(all_files.size(), expected_file_count);
        ASSERT_EQ(expected_row_counts.size(), expected_file_count);
        ASSERT_EQ(expected_min_seqs.size(), expected_file_count);
        ASSERT_EQ(expected_max_seqs.size(), expected_file_count);
        ASSERT_EQ(expected_first_row_ids.size(), expected_file_count);
        ASSERT_EQ(expected_write_cols.size(), expected_file_count);
        for (size_t i = 0; i < all_files.size(); ++i) {
            const auto& file = all_files[i];
            EXPECT_EQ(file->row_count, expected_row_counts[i]);
            EXPECT_EQ(file->min_sequence_number, expected_min_seqs[i]);
            EXPECT_EQ(file->max_sequence_number, expected_max_seqs[i]);
            ASSERT_TRUE(file->first_row_id.has_value());
            EXPECT_EQ(file->first_row_id.value(), expected_first_row_ids[i]);
            EXPECT_EQ(file->write_cols, expected_write_cols[i]);
        }
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::unique_ptr<UniqueTestDirectory> blob_dir_;
    int blob_file_counter_ = 0;
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
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32()),
                                 BlobUtils::ToArrowField("blob", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},       {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},      {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"}, {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_AS_DESCRIPTOR, "true"},   {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // prepare data: input uses plain raw blob bytes for readability
    std::string raw_json = R"([
        ["str_0", null, "hello_blob_0"],
        ["str_1", 1,    "blob_data_1"],
        ["str_2", 2,    "blob_data_2"],
        ["str_3", null, "blob_data_3"]
    ])";
    auto raw_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), raw_json).ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto desc_array, ConvertRawBlobToDescriptor(raw_array, {"blob"}));

    // write descriptor array
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, {}, schema->field_names(), {desc_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // read result contains descriptors pointing to paimon internal blob files
    // resolve descriptors back to raw bytes, then prepend _VALUE_KIND and compare
    ASSERT_OK_AND_ASSIGN(auto result, ScanAndReadResult(table_path, schema->field_names()));
    ASSERT_TRUE(result.chunked_array);
    auto read_concat = arrow::Concatenate(result.chunked_array->chunks()).ValueOrDie();
    auto read_struct = std::dynamic_pointer_cast<arrow::StructArray>(read_concat);
    ASSERT_OK_AND_ASSIGN(auto resolved, ConvertDescriptorToRawBlob(read_struct, {"blob"}));
    ASSERT_OK_AND_ASSIGN(auto expected_with_rk, PrependRowKindColumn(raw_array));
    ASSERT_TRUE(resolved->Equals(expected_with_rk));
}

TEST_P(BlobTableInteTest, TestAppendTableWriteWithBlobAsDescriptorFalse) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32()),
                                 BlobUtils::ToArrowField("blob", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},       {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},      {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"}, {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_AS_DESCRIPTOR, "false"},  {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    std::string data_json = R"([
        ["str_0", null, "apple"],
        ["str_1", 1,    "banana"],
        ["str_2", 2,    "cat"],
        ["str_3", null, "dog"]
    ])";
    auto write_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), data_json).ValueOrDie());

    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, {}, schema->field_names(), {write_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // BLOB_AS_DESCRIPTOR=false: blob data is stored inline, read result should match input
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), write_array));
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
                        "SeparateBlobArray expects at least one main field, but got none.");
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

TEST_P(BlobTableInteTest, TestAppendTableWriteWithMultipleBlobFields) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        BlobUtils::ToArrowField("blob1", true), BlobUtils::ToArrowField("blob2", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},       {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},      {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"}, {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    std::string data_json = R"([
        ["str_0", null, "apple",  "red"],
        ["str_1", 1,    "banana", "yellow"],
        ["str_2", 2,    "cat",    "black"]
    ])";
    auto write_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), data_json).ValueOrDie());

    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, {}, schema->field_names(), {write_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), write_array));
}

TEST_P(BlobTableInteTest, TestAppendWriteWithNullBlob) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::int32()),
                                 BlobUtils::ToArrowField("blob", true)};

    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, GetParam()},
                                                  {Options::BUCKET, "-1"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // Write: row 0 non-null blob, row 1 null blob, row 2 non-null blob
    std::string data_json = R"([
        [1, "hello"],
        [2, null],
        [3, "world"]
    ])";
    auto write_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), data_json).ValueOrDie());

    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, {}, schema->field_names(), {write_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), write_array));
}

TEST_P(BlobTableInteTest, TestReadTableWithMultiBlobFields) {
    auto file_format = GetParam();
    if (file_format == "lance" || file_format == "avro") {
        return;
    }
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/append_table_with_multi_blob.db/append_table_with_multi_blob";
    std::vector<DataField> read_fields = {
        DataField(5, BlobUtils::ToArrowField("f5")),
        DataField(1, arrow::field("f1", arrow::int32())),
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f3", arrow::float64())),
        DataField(4, arrow::field("f4", arrow::utf8())),
        SpecialFields::RowId(),
        DataField(6, BlobUtils::ToArrowField("f6")),
    };

    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    auto make_json_row = [&](int32_t i) -> std::string {
        std::string f5_json;
        if (i <= 1) {
            f5_json = "null";
        } else {
            f5_json = "\"" + std::string(1024, static_cast<char>('A' + i)) + "\"";
        }

        std::string f6_json;
        if (i == 0 || i == 2) {
            f6_json = "null";
        } else {
            f6_json = "\"" + std::string(2048, static_cast<char>('a' + i)) + "\"";
        }
        // f1=i, f2=i*10, f3=i+0.5, f4="desc_i", row_id=i
        return "[" + f5_json + ", " + std::to_string(i) + ", " + std::to_string(i * 10) + ", " +
               std::to_string(i + 0.5) + ", \"desc_" + std::to_string(i) + "\", " +
               std::to_string(i) + ", " + f6_json + "]";
    };

    auto build_expected =
        [&](const std::vector<int32_t>& row_indices) -> std::shared_ptr<arrow::StructArray> {
        std::string json_str = "[";
        for (size_t idx = 0; idx < row_indices.size(); idx++) {
            if (idx > 0) {
                json_str += ",";
            }
            json_str += make_json_row(row_indices[idx]);
        }
        json_str += "]";
        return std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, json_str).ValueOrDie());
    };

    // Full scan: all 10 rows
    {
        std::vector<int32_t> all_rows(10);
        std::iota(all_rows.begin(), all_rows.end(), 0);
        auto expected_array = build_expected(all_rows);
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array));
    }

    // Test with row_ranges: select row 0 (both null), row 2 (f5 non-null, f6 null),
    // row 5 (both non-null) to verify null handling under row range filtering.
    {
        auto expected_rr = build_expected({0, 2, 5});
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_rr, /*predicate=*/nullptr,
                              /*row_ranges=*/{Range(0l, 0l), Range(2l, 2l), Range(5l, 5l)}));
    }
}

TEST_P(BlobTableInteTest, TestBlobDescriptorFieldWithoutExternalStorage) {
    if (GetParam() == "lance") {
        return;
    }
    // Two blob fields configured via BLOB_DESCRIPTOR_FIELD, no external storage.
    arrow::FieldVector fields = {arrow::field("f0", arrow::int32()),
                                 BlobUtils::ToArrowField("b0", true),
                                 BlobUtils::ToArrowField("b1", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},         {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},        {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"},   {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_DESCRIPTOR_FIELD, "b0,b1"}, {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // Input uses plain raw bytes for readability
    std::string raw_json = R"([
        [1, "image_data_0", "video_data_0"],
        [2, "image_data_1", "video_data_1"],
        [3, "image_data_2", "video_data_2"]
    ])";
    auto raw_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), raw_json).ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto desc_array, ConvertRawBlobToDescriptor(raw_array, {"b0", "b1"}));

    // write descriptor array
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, {}, schema->field_names(), {desc_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // Scan and verify DataFileMeta: no external storage -> write_cols should be nullopt
    ASSERT_OK_AND_ASSIGN(auto plan, ScanTable(table_path));
    VerifyDataFileMetas(plan, /*expected_file_count=*/1, /*expected_row_counts=*/{3},
                        /*expected_min_seqs=*/{1}, /*expected_max_seqs=*/{1},
                        /*expected_first_row_ids=*/{0},
                        /*expected_write_cols=*/{std::nullopt});

    // Read and resolve descriptors back to raw bytes
    std::map<std::string, std::string> read_options = {};
    ASSERT_OK_AND_ASSIGN(auto result, ReadTable(table_path, schema->field_names(), plan,
                                                /*predicate=*/nullptr, read_options));
    ASSERT_TRUE(result.chunked_array);
    auto read_concat = arrow::Concatenate(result.chunked_array->chunks()).ValueOrDie();
    auto read_struct = std::dynamic_pointer_cast<arrow::StructArray>(read_concat);
    ASSERT_OK_AND_ASSIGN(auto resolved, ConvertDescriptorToRawBlob(read_struct, {"b0", "b1"}));
    ASSERT_OK_AND_ASSIGN(auto expected_with_rk, PrependRowKindColumn(raw_array));
    ASSERT_TRUE(resolved->Equals(expected_with_rk));

    // Descriptor bytes should be unchanged (inline, not repacked)
    ASSERT_TRUE(read_struct->GetFieldByName("b0")->Equals(desc_array->GetFieldByName("b0")));
    ASSERT_TRUE(read_struct->GetFieldByName("b1")->Equals(desc_array->GetFieldByName("b1")));
}

TEST_P(BlobTableInteTest, TestBlobDescriptorFieldWithExternalStorage) {
    if (GetParam() == "lance") {
        return;
    }
    // Two blob fields configured via BLOB_DESCRIPTOR_FIELD + BLOB_EXTERNAL_STORAGE_FIELD
    // with BLOB_EXTERNAL_STORAGE_PATH pointing to blob_dir_.
    arrow::FieldVector fields = {arrow::field("f0", arrow::int32()),
                                 BlobUtils::ToArrowField("b0", true),
                                 BlobUtils::ToArrowField("b1", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},
        {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_DESCRIPTOR_FIELD, "b0,b1"},
        {Options::BLOB_EXTERNAL_STORAGE_FIELD, "b0,b1"},
        {Options::BLOB_EXTERNAL_STORAGE_PATH, blob_dir_->Str()},
        {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // Input uses plain raw bytes for readability
    std::string raw_json = R"([
        [1, "image_data_0", "video_data_0"],
        [2, "image_data_1", "video_data_1"],
        [3, "image_data_2", "video_data_2"]
    ])";
    auto raw_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), raw_json).ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto desc_array, ConvertRawBlobToDescriptor(raw_array, {"b0", "b1"}));

    // write descriptor array
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, {}, schema->field_names(), {desc_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // Scan and verify DataFileMeta: with external storage -> write_cols should be explicit
    ASSERT_OK_AND_ASSIGN(auto plan, ScanTable(table_path));
    VerifyDataFileMetas(plan, /*expected_file_count=*/1, /*expected_row_counts=*/{3},
                        /*expected_min_seqs=*/{1}, /*expected_max_seqs=*/{1},
                        /*expected_first_row_ids=*/{0},
                        /*expected_write_cols=*/{std::vector<std::string>{"f0", "b0", "b1"}});

    // Read and resolve descriptors back to raw bytes
    std::map<std::string, std::string> read_options = {{Options::BLOB_AS_DESCRIPTOR, "true"}};
    ASSERT_OK_AND_ASSIGN(auto result, ReadTable(table_path, schema->field_names(), plan,
                                                /*predicate=*/nullptr, read_options));
    ASSERT_TRUE(result.chunked_array);
    auto read_concat = arrow::Concatenate(result.chunked_array->chunks()).ValueOrDie();
    auto read_struct = std::dynamic_pointer_cast<arrow::StructArray>(read_concat);
    ASSERT_OK_AND_ASSIGN(auto resolved, ConvertDescriptorToRawBlob(read_struct, {"b0", "b1"}));
    ASSERT_OK_AND_ASSIGN(auto expected_with_rk, PrependRowKindColumn(raw_array));
    ASSERT_TRUE(resolved->Equals(expected_with_rk));

    // Descriptor bytes should differ (repacked by external storage)
    ASSERT_FALSE(read_struct->GetFieldByName("b0")->Equals(desc_array->GetFieldByName("b0")));
    ASSERT_FALSE(read_struct->GetFieldByName("b1")->Equals(desc_array->GetFieldByName("b1")));
}

TEST_P(BlobTableInteTest, TestBlobDescriptorFieldPartialExternalStorage) {
    if (GetParam() == "lance") {
        return;
    }
    // 4 blob fields: b0,b1 have external storage, b2,b3 are descriptor-only (no external storage).
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()), BlobUtils::ToArrowField("b0", true),
        BlobUtils::ToArrowField("b1", true), BlobUtils::ToArrowField("b2", true),
        BlobUtils::ToArrowField("b3", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},
        {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_DESCRIPTOR_FIELD, "b0,b1,b2,b3"},
        {Options::BLOB_EXTERNAL_STORAGE_FIELD, "b0,b1"},
        {Options::BLOB_EXTERNAL_STORAGE_PATH, blob_dir_->Str()},
        {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // Input uses plain raw bytes for readability; some blob fields are null
    std::string raw_json = R"([
        [1, "img_0", null,    "doc_0", "log_0"],
        [2, null,    "vid_1", null,    "log_1"],
        [3, "img_2", "vid_2", "doc_2", null   ]
    ])";
    auto raw_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), raw_json).ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto desc_array,
                         ConvertRawBlobToDescriptor(raw_array, {"b0", "b1", "b2", "b3"}));

    // write descriptor array
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, {}, schema->field_names(), {desc_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // Scan and verify DataFileMeta: external storage on b0,b1 -> write_cols should be explicit
    ASSERT_OK_AND_ASSIGN(auto plan, ScanTable(table_path));
    VerifyDataFileMetas(
        plan, /*expected_file_count=*/1, /*expected_row_counts=*/{3},
        /*expected_min_seqs=*/{1}, /*expected_max_seqs=*/{1},
        /*expected_first_row_ids=*/{0},
        /*expected_write_cols=*/{std::vector<std::string>{"f0", "b0", "b1", "b2", "b3"}});

    // Read and resolve all descriptors back to raw bytes
    std::map<std::string, std::string> read_options = {{Options::BLOB_AS_DESCRIPTOR, "true"}};
    ASSERT_OK_AND_ASSIGN(auto result, ReadTable(table_path, schema->field_names(), plan,
                                                /*predicate=*/nullptr, read_options));
    ASSERT_TRUE(result.chunked_array);
    auto read_concat = arrow::Concatenate(result.chunked_array->chunks()).ValueOrDie();
    auto read_struct = std::dynamic_pointer_cast<arrow::StructArray>(read_concat);
    ASSERT_OK_AND_ASSIGN(auto resolved,
                         ConvertDescriptorToRawBlob(read_struct, {"b0", "b1", "b2", "b3"}));
    ASSERT_OK_AND_ASSIGN(auto expected_with_rk, PrependRowKindColumn(raw_array));
    ASSERT_TRUE(resolved->Equals(expected_with_rk));

    // b0,b1 repacked by external storage, should differ
    ASSERT_FALSE(read_struct->GetFieldByName("b0")->Equals(desc_array->GetFieldByName("b0")));
    ASSERT_FALSE(read_struct->GetFieldByName("b1")->Equals(desc_array->GetFieldByName("b1")));
    // b2,b3 inline descriptor, should match
    ASSERT_TRUE(read_struct->GetFieldByName("b2")->Equals(desc_array->GetFieldByName("b2")));
    ASSERT_TRUE(read_struct->GetFieldByName("b3")->Equals(desc_array->GetFieldByName("b3")));
}

TEST_P(BlobTableInteTest, TestBlobDescriptorFieldPartialInline) {
    if (GetParam() == "lance") {
        return;
    }
    // 4 blob fields: b0,b1 are descriptor (inline), b2,b3 are regular blob (written to .blob
    // files). No external storage.
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()), BlobUtils::ToArrowField("b0", true),
        BlobUtils::ToArrowField("b1", true), BlobUtils::ToArrowField("b2", true),
        BlobUtils::ToArrowField("b3", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},         {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},        {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"},   {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_DESCRIPTOR_FIELD, "b0,b1"}, {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // Input uses plain raw bytes:
    // b0: all non-null, b1: has nulls, b2: all non-null, b3: has nulls
    std::string raw_json = R"([
        [1, "img_0", null,    "raw_2_0", "raw_3_0"],
        [2, "img_1", "vid_1", "raw_2_1", null      ],
        [3, "img_2", null,    "raw_2_2", "raw_3_2" ]
    ])";
    auto raw_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), raw_json).ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto desc_array,
                         ConvertRawBlobToDescriptor(raw_array, {"b0", "b1", "b2", "b3"}));

    // write: b0,b1 as descriptor bytes; b2,b3 as raw bytes (paimon writes them to .blob files)
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, {}, schema->field_names(), {desc_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // Scan and verify DataFileMeta: b2,b3 go to .blob files, "f0", "b0", "b1" go to main files.
    ASSERT_OK_AND_ASSIGN(auto plan, ScanTable(table_path));
    VerifyDataFileMetas(plan, /*expected_file_count=*/3, /*expected_row_counts=*/{3, 3, 3},
                        /*expected_min_seqs=*/{1, 1, 1}, /*expected_max_seqs=*/{1, 1, 1},
                        /*expected_first_row_ids=*/{0, 0, 0},
                        /*expected_write_cols=*/
                        {std::vector<std::string>{"f0", "b0", "b1"}, std::vector<std::string>{"b2"},
                         std::vector<std::string>{"b3"}});

    std::map<std::string, std::string> read_options = {{Options::BLOB_AS_DESCRIPTOR, "true"}};
    ASSERT_OK_AND_ASSIGN(auto result, ReadTable(table_path, schema->field_names(), plan,
                                                /*predicate=*/nullptr, read_options));
    ASSERT_TRUE(result.chunked_array);
    auto read_concat = arrow::Concatenate(result.chunked_array->chunks()).ValueOrDie();
    auto read_struct = std::dynamic_pointer_cast<arrow::StructArray>(read_concat);

    // b0,b1 inline descriptor (not repacked), should match input
    ASSERT_TRUE(read_struct->GetFieldByName("b0")->Equals(desc_array->GetFieldByName("b0")));
    ASSERT_TRUE(read_struct->GetFieldByName("b1")->Equals(desc_array->GetFieldByName("b1")));

    // Resolve b0,b1 descriptors back to raw bytes, then compare full struct
    ASSERT_OK_AND_ASSIGN(auto resolved,
                         ConvertDescriptorToRawBlob(read_struct, {"b0", "b1", "b2", "b3"}));
    ASSERT_OK_AND_ASSIGN(auto expected_with_rk, PrependRowKindColumn(raw_array));
    ASSERT_TRUE(resolved->Equals(expected_with_rk));
}

TEST_P(BlobTableInteTest, TestBlobDescriptorFieldPartialExternalStorageRepack) {
    if (GetParam() == "lance") {
        return;
    }
    // 4 blob fields: b0,b1 are descriptor + external-storage-field WITH external-storage-path.
    // b2,b3 are regular blob (written to .blob files).
    // All blob descriptors get repacked by external storage or .blob writer.
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()), BlobUtils::ToArrowField("b0", true),
        BlobUtils::ToArrowField("b1", true), BlobUtils::ToArrowField("b2", true),
        BlobUtils::ToArrowField("b3", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},
        {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_DESCRIPTOR_FIELD, "b0,b1"},
        {Options::BLOB_EXTERNAL_STORAGE_FIELD, "b0,b1"},
        {Options::BLOB_EXTERNAL_STORAGE_PATH, blob_dir_->Str()},
        {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // b0: all non-null, b1: has nulls, b2: all non-null, b3: has nulls
    std::string raw_json = R"([
        [1, "img_0", null,    "raw_2_0", "raw_3_0"],
        [2, "img_1", "vid_1", "raw_2_1", null      ],
        [3, "img_2", null,    "raw_2_2", "raw_3_2" ]
    ])";
    auto raw_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), raw_json).ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto desc_array,
                         ConvertRawBlobToDescriptor(raw_array, {"b0", "b1", "b2", "b3"}));

    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, {}, schema->field_names(), {desc_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // b0,b1 repacked to external storage; b2,b3 go to .blob files.
    // Main file contains f0,b0,b1; .blob files for b2 and b3.
    ASSERT_OK_AND_ASSIGN(auto plan, ScanTable(table_path));
    VerifyDataFileMetas(plan, /*expected_file_count=*/3, /*expected_row_counts=*/{3, 3, 3},
                        /*expected_min_seqs=*/{1, 1, 1}, /*expected_max_seqs=*/{1, 1, 1},
                        /*expected_first_row_ids=*/{0, 0, 0},
                        /*expected_write_cols=*/
                        {std::vector<std::string>{"f0", "b0", "b1"}, std::vector<std::string>{"b2"},
                         std::vector<std::string>{"b3"}});

    std::map<std::string, std::string> read_options = {{Options::BLOB_AS_DESCRIPTOR, "true"}};
    ASSERT_OK_AND_ASSIGN(auto result, ReadTable(table_path, schema->field_names(), plan,
                                                /*predicate=*/nullptr, read_options));
    ASSERT_TRUE(result.chunked_array);
    auto read_concat = arrow::Concatenate(result.chunked_array->chunks()).ValueOrDie();
    auto read_struct = std::dynamic_pointer_cast<arrow::StructArray>(read_concat);

    // Resolve descriptors back to raw bytes and compare
    ASSERT_OK_AND_ASSIGN(auto resolved,
                         ConvertDescriptorToRawBlob(read_struct, {"b0", "b1", "b2", "b3"}));
    ASSERT_OK_AND_ASSIGN(auto expected_with_rk, PrependRowKindColumn(raw_array));
    ASSERT_TRUE(resolved->Equals(expected_with_rk));

    // All blob columns should differ from input desc_array (all repacked)
    ASSERT_FALSE(read_struct->GetFieldByName("b0")->Equals(desc_array->GetFieldByName("b0")));
    ASSERT_FALSE(read_struct->GetFieldByName("b1")->Equals(desc_array->GetFieldByName("b1")));
    ASSERT_FALSE(read_struct->GetFieldByName("b2")->Equals(desc_array->GetFieldByName("b2")));
    ASSERT_FALSE(read_struct->GetFieldByName("b3")->Equals(desc_array->GetFieldByName("b3")));
}

TEST_P(BlobTableInteTest, TestBlobDescriptorFieldPartialExternalStorageSingleField) {
    if (GetParam() == "lance") {
        return;
    }
    // 4 blob fields: b0,b1 are descriptor; only b1 has external storage.
    // b2,b3 are regular blob (written to .blob files).
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()), BlobUtils::ToArrowField("b0", true),
        BlobUtils::ToArrowField("b1", true), BlobUtils::ToArrowField("b2", true),
        BlobUtils::ToArrowField("b3", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},
        {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_DESCRIPTOR_FIELD, "b0,b1"},
        {Options::BLOB_EXTERNAL_STORAGE_FIELD, "b1"},
        {Options::BLOB_EXTERNAL_STORAGE_PATH, blob_dir_->Str()},
        {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // b0: all non-null, b1: has nulls, b2: all non-null, b3: has nulls
    std::string raw_json = R"([
        [1, "img_0", null,    "raw_2_0", "raw_3_0"],
        [2, "img_1", "vid_1", "raw_2_1", null      ],
        [3, "img_2", null,    "raw_2_2", "raw_3_2" ]
    ])";
    auto raw_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), raw_json).ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto desc_array,
                         ConvertRawBlobToDescriptor(raw_array, {"b0", "b1", "b2", "b3"}));

    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, {}, schema->field_names(), {desc_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // b1 repacked to external storage; b2,b3 go to .blob files; b0 stays inline in main file.
    // Main file contains f0,b0,b1; .blob files for b2 and b3.
    ASSERT_OK_AND_ASSIGN(auto plan, ScanTable(table_path));
    VerifyDataFileMetas(plan, /*expected_file_count=*/3, /*expected_row_counts=*/{3, 3, 3},
                        /*expected_min_seqs=*/{1, 1, 1}, /*expected_max_seqs=*/{1, 1, 1},
                        /*expected_first_row_ids=*/{0, 0, 0},
                        /*expected_write_cols=*/
                        {std::vector<std::string>{"f0", "b0", "b1"}, std::vector<std::string>{"b2"},
                         std::vector<std::string>{"b3"}});

    std::map<std::string, std::string> read_options = {{Options::BLOB_AS_DESCRIPTOR, "true"}};
    ASSERT_OK_AND_ASSIGN(auto result, ReadTable(table_path, schema->field_names(), plan,
                                                /*predicate=*/nullptr, read_options));
    ASSERT_TRUE(result.chunked_array);
    auto read_concat = arrow::Concatenate(result.chunked_array->chunks()).ValueOrDie();
    auto read_struct = std::dynamic_pointer_cast<arrow::StructArray>(read_concat);

    // Resolve all descriptors back to raw bytes and compare
    ASSERT_OK_AND_ASSIGN(auto resolved,
                         ConvertDescriptorToRawBlob(read_struct, {"b0", "b1", "b2", "b3"}));
    ASSERT_OK_AND_ASSIGN(auto expected_with_rk, PrependRowKindColumn(raw_array));
    ASSERT_TRUE(resolved->Equals(expected_with_rk));

    // b0 is inline descriptor (not repacked), should match input
    ASSERT_TRUE(read_struct->GetFieldByName("b0")->Equals(desc_array->GetFieldByName("b0")));
    // b1 is repacked by external storage, should differ
    ASSERT_FALSE(read_struct->GetFieldByName("b1")->Equals(desc_array->GetFieldByName("b1")));
    // b2,b3 are repacked by .blob writer, should differ
    ASSERT_FALSE(read_struct->GetFieldByName("b2")->Equals(desc_array->GetFieldByName("b2")));
    ASSERT_FALSE(read_struct->GetFieldByName("b3")->Equals(desc_array->GetFieldByName("b3")));
}

TEST_P(BlobTableInteTest, TestBlobDescriptorFieldPartialExternalStorageNoAsDescriptor) {
    if (GetParam() == "lance") {
        return;
    }
    // Same as TestBlobDescriptorFieldPartialExternalStorageSingleField but without
    // BLOB_AS_DESCRIPTOR in table options. Only b0 is explicitly converted to descriptor before
    // write. b1 is written as raw bytes but still configured as descriptor field, so paimon should
    // auto-convert it to descriptor internally (write auto-detects descriptor via magic header).
    // After read with BLOB_AS_DESCRIPTOR=true, b0 and b1 are both stored as descriptor.
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()), BlobUtils::ToArrowField("b0", true),
        BlobUtils::ToArrowField("b1", true), BlobUtils::ToArrowField("b2", true),
        BlobUtils::ToArrowField("b3", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},
        {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_DESCRIPTOR_FIELD, "b0,b1"},
        {Options::BLOB_EXTERNAL_STORAGE_FIELD, "b1"},
        {Options::BLOB_EXTERNAL_STORAGE_PATH, blob_dir_->Str()},
        {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // b0: all non-null, b1: has nulls, b2: all non-null, b3: has nulls
    std::string raw_json = R"([
        [1, "img_0", null,    "raw_2_0", "raw_3_0"],
        [2, "img_1", "vid_1", "raw_2_1", null      ],
        [3, "img_2", null,    "raw_2_2", "raw_3_2" ]
    ])";
    auto raw_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), raw_json).ValueOrDie());
    // Only convert b0 to descriptor; b1,b2,b3 remain as raw bytes
    ASSERT_OK_AND_ASSIGN(auto desc_array, ConvertRawBlobToDescriptor(raw_array, {"b0"}));

    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, {}, schema->field_names(), {desc_array}));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // b1 repacked to external storage; b2,b3 go to .blob files; b0 stays inline in main file.
    ASSERT_OK_AND_ASSIGN(auto plan, ScanTable(table_path));
    VerifyDataFileMetas(plan, /*expected_file_count=*/3, /*expected_row_counts=*/{3, 3, 3},
                        /*expected_min_seqs=*/{1, 1, 1}, /*expected_max_seqs=*/{1, 1, 1},
                        /*expected_first_row_ids=*/{0, 0, 0},
                        /*expected_write_cols=*/
                        {std::vector<std::string>{"f0", "b0", "b1"}, std::vector<std::string>{"b2"},
                         std::vector<std::string>{"b3"}});

    std::map<std::string, std::string> read_options = {{Options::BLOB_AS_DESCRIPTOR, "false"}};
    ASSERT_OK_AND_ASSIGN(auto result, ReadTable(table_path, schema->field_names(), plan,
                                                /*predicate=*/nullptr, read_options));
    ASSERT_TRUE(result.chunked_array);
    auto read_concat = arrow::Concatenate(result.chunked_array->chunks()).ValueOrDie();
    auto read_struct = std::dynamic_pointer_cast<arrow::StructArray>(read_concat);

    // After read, b0 and b1 are both descriptor-stored; resolve all back to raw bytes
    ASSERT_OK_AND_ASSIGN(auto resolved, ConvertDescriptorToRawBlob(read_struct, {"b0", "b1"}));
    ASSERT_OK_AND_ASSIGN(auto expected_with_rk, PrependRowKindColumn(raw_array));
    ASSERT_TRUE(resolved->Equals(expected_with_rk));

    // b0 is inline descriptor (not repacked), should match input desc_array
    ASSERT_TRUE(read_struct->GetFieldByName("b0")->Equals(desc_array->GetFieldByName("b0")));
}

TEST_P(BlobTableInteTest, TestBlobDescriptorMultiCommitAndShuffledReadSchema) {
    if (GetParam() == "lance") {
        return;
    }
    // Similar to TestBlobDescriptorFieldPartialExternalStorageNoAsDescriptor but:
    // 1. Multiple write+commit rounds
    // 2. Read schema is shuffled: b3, b2, b1, b0, f0
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()), BlobUtils::ToArrowField("b0", true),
        BlobUtils::ToArrowField("b1", true), BlobUtils::ToArrowField("b2", true),
        BlobUtils::ToArrowField("b3", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},
        {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_DESCRIPTOR_FIELD, "b0,b1"},
        {Options::BLOB_EXTERNAL_STORAGE_FIELD, "b1"},
        {Options::BLOB_EXTERNAL_STORAGE_PATH, blob_dir_->Str()},
        {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields);

    // --- First write+commit ---
    std::string raw_json_1 = R"([
        [1, "img_0", null,    "raw_2_0", "raw_3_0"],
        [2, "img_1", "vid_1", "raw_2_1", null      ]
    ])";
    auto raw_array_1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), raw_json_1).ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto desc_array_1, ConvertRawBlobToDescriptor(raw_array_1, {"b0"}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_1,
                         WriteArray(table_path, {}, schema->field_names(), {desc_array_1}));
    ASSERT_OK(Commit(table_path, commit_msgs_1));

    // --- Second write+commit ---
    std::string raw_json_2 = R"([
        [3, "img_2", "vid_2", "raw_2_2", "raw_3_2"],
        [4, null,    "vid_3", "raw_2_3", "raw_3_3"]
    ])";
    auto raw_array_2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), raw_json_2).ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto desc_array_2, ConvertRawBlobToDescriptor(raw_array_2, {"b0"}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2,
                         WriteArray(table_path, {}, schema->field_names(), {desc_array_2}));
    ASSERT_OK(Commit(table_path, commit_msgs_2));

    // --- Third write+commit ---
    std::string raw_json_3 = R"([
        [5, "img_4", null,    "raw_2_4", null     ],
        [6, "img_5", "vid_5", null,      "raw_3_5"]
    ])";
    auto raw_array_3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), raw_json_3).ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto desc_array_3, ConvertRawBlobToDescriptor(raw_array_3, {"b0"}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_3,
                         WriteArray(table_path, {}, schema->field_names(), {desc_array_3}));
    ASSERT_OK(Commit(table_path, commit_msgs_3));

    // test read
    {
        // --- Read with shuffled schema: b3, b2, b1, b0, f0 ---
        std::vector<std::string> shuffled_read_schema = {"b3", "b2", "b1", "b0", "f0"};
        ASSERT_OK_AND_ASSIGN(auto plan, ScanTable(table_path));

        std::map<std::string, std::string> read_options = {{Options::BLOB_AS_DESCRIPTOR, "false"}};
        ASSERT_OK_AND_ASSIGN(auto result, ReadTable(table_path, shuffled_read_schema, plan,
                                                    /*predicate=*/nullptr, read_options));
        ASSERT_TRUE(result.chunked_array);
        auto read_concat = arrow::Concatenate(result.chunked_array->chunks()).ValueOrDie();
        auto read_struct = std::dynamic_pointer_cast<arrow::StructArray>(read_concat);

        // Build expected array in shuffled order from all 3 batches
        arrow::FieldVector shuffled_fields = {
            BlobUtils::ToArrowField("b3", true), BlobUtils::ToArrowField("b2", true),
            BlobUtils::ToArrowField("b1", true), BlobUtils::ToArrowField("b0", true),
            arrow::field("f0", arrow::int32())};
        std::string expected_json = R"([
        ["raw_3_0", "raw_2_0", null,    "img_0", 1],
        [null,      "raw_2_1", "vid_1", "img_1", 2],
        ["raw_3_2", "raw_2_2", "vid_2", "img_2", 3],
        ["raw_3_3", "raw_2_3", "vid_3", null,    4],
        [null,      "raw_2_4", null,    "img_4", 5],
        ["raw_3_5", null,      "vid_5", "img_5", 6]
    ])";
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(shuffled_fields),
                                                      expected_json)
                .ValueOrDie());

        // Resolve descriptors (b0, b1 are descriptor fields) back to raw bytes
        ASSERT_OK_AND_ASSIGN(auto resolved, ConvertDescriptorToRawBlob(read_struct, {"b0", "b1"}));
        ASSERT_OK_AND_ASSIGN(auto expected_with_rk, PrependRowKindColumn(expected_array));
        ASSERT_TRUE(resolved->Equals(expected_with_rk));
    }
    {
        // test scan and read with GlobalIndexResult
        std::vector<std::string> shuffled_read_schema = {"b3", "b2", "b1", "b0", "f0"};
        ASSERT_OK_AND_ASSIGN(auto plan, ScanTable(table_path, /*predicate=*/nullptr,
                                                  /*row_ranges=*/{Range(1, 3), Range(5, 5)}));
        std::map<std::string, std::string> read_options = {{Options::BLOB_AS_DESCRIPTOR, "false"}};
        ASSERT_OK_AND_ASSIGN(auto result, ReadTable(table_path, shuffled_read_schema, plan,
                                                    /*predicate=*/nullptr, read_options));
        ASSERT_TRUE(result.chunked_array);
        auto read_concat = arrow::Concatenate(result.chunked_array->chunks()).ValueOrDie();
        auto read_struct = std::dynamic_pointer_cast<arrow::StructArray>(read_concat);

        // Build expected array in shuffled order from all 3 batches
        arrow::FieldVector shuffled_fields = {
            BlobUtils::ToArrowField("b3", true), BlobUtils::ToArrowField("b2", true),
            BlobUtils::ToArrowField("b1", true), BlobUtils::ToArrowField("b0", true),
            arrow::field("f0", arrow::int32())};
        std::string expected_json = R"([
        [null,      "raw_2_1", "vid_1", "img_1", 2],
        ["raw_3_2", "raw_2_2", "vid_2", "img_2", 3],
        ["raw_3_3", "raw_2_3", "vid_3", null,    4],
        ["raw_3_5", null,      "vid_5", "img_5", 6]
    ])";
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(shuffled_fields),
                                                      expected_json)
                .ValueOrDie());

        // Resolve descriptors (b0, b1 are descriptor fields) back to raw bytes
        ASSERT_OK_AND_ASSIGN(auto resolved, ConvertDescriptorToRawBlob(read_struct, {"b0", "b1"}));
        ASSERT_OK_AND_ASSIGN(auto expected_with_rk, PrependRowKindColumn(expected_array));
        ASSERT_TRUE(resolved->Equals(expected_with_rk));
    }
}

TEST_P(BlobTableInteTest, TestDataEvolutionWithBlobDescriptorField) {
    if (GetParam() == "lance") {
        return;
    }
    // Test DataEvolution (split-column write) combined with blob descriptor fields.
    // Schema: f0(int32), b0(blob descriptor inline), b1(blob descriptor+external), b2(blob),
    // b3(blob)
    // Commit 1: file A writes (f0, b2, b3)
    // Commit 2: file B writes (f0, b0, b1) with SetFirstRowId(0)
    // -> merges with commit 1
    // Commit 3: file A writes (f0, b0, b1, b3)
    // Commit 4: file B writes (b0, b1, b3) with SetFirstRowId(3)
    // -> merges with commit 3
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()), BlobUtils::ToArrowField("b0", true),
        BlobUtils::ToArrowField("b1", true), BlobUtils::ToArrowField("b2", true),
        BlobUtils::ToArrowField("b3", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},
        {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_DESCRIPTOR_FIELD, "b0,b1"},
        {Options::BLOB_EXTERNAL_STORAGE_FIELD, "b1"},
        {Options::BLOB_EXTERNAL_STORAGE_PATH, blob_dir_->Str()},
        {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // --- Commit 1: file A (f0, b2, b3), Commit 2: file B (f0, b0, b1) SetFirstRowId(0) ---
    std::string file_a1_json = R"([
        [1, "raw_2_0", "raw_3_0"],
        [2, "raw_2_1", null     ],
        [3, null,      "raw_3_2"]
    ])";
    arrow::FieldVector file_a1_fields = {fields[0], fields[3], fields[4]};
    auto file_a1_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(file_a1_fields), file_a1_json)
            .ValueOrDie());

    std::string file_b1_json = R"([
        [1, "img_0", "vid_0"],
        [2, "img_1", null   ],
        [3, "img_2", "vid_2"]
    ])";
    arrow::FieldVector file_b1_fields = {fields[0], fields[1], fields[2]};
    auto file_b1_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(file_b1_fields), file_b1_json)
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto file_b1_desc, ConvertRawBlobToDescriptor(file_b1_array, {"b0"}));

    ASSERT_OK_AND_ASSIGN(auto commit_msgs_a1,
                         WriteArray(table_path, {}, {"f0", "b2", "b3"}, {file_a1_array}));
    ASSERT_OK(Commit(table_path, commit_msgs_a1));

    ASSERT_OK_AND_ASSIGN(auto commit_msgs_b1,
                         WriteArray(table_path, {}, {"f0", "b0", "b1"}, {file_b1_desc}));
    SetFirstRowId(0, commit_msgs_b1);
    ASSERT_OK(Commit(table_path, commit_msgs_b1));

    // --- Commit 3: file A (f0, b0, b1, b3), Commit 4: file B (b0, b1, b3) SetFirstRowId(3) ---
    // Duplicate cols b0, b1, b3: file B (commit 4, newer) takes precedence.
    std::string file_a2_json = R"([
        [4, "img_3_old", "vid_3_old", "raw_3_3_old"],
        [5, null,        "vid_4_old", "raw_3_4_old"],
        [6, "img_5_old", null,        null         ]
    ])";
    arrow::FieldVector file_a2_fields = {fields[0], fields[1], fields[2], fields[4]};
    auto file_a2_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(file_a2_fields), file_a2_json)
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto file_a2_desc, ConvertRawBlobToDescriptor(file_a2_array, {"b0"}));

    std::string file_b2_json = R"([
        ["img_3", "vid_3", "raw_3_3"],
        [null,    "vid_4", "raw_3_4"],
        ["img_5", null,    null     ]
    ])";
    arrow::FieldVector file_b2_fields = {fields[1], fields[2], fields[4]};
    auto file_b2_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(file_b2_fields), file_b2_json)
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto file_b2_desc, ConvertRawBlobToDescriptor(file_b2_array, {"b0"}));

    ASSERT_OK_AND_ASSIGN(auto commit_msgs_a2,
                         WriteArray(table_path, {}, {"f0", "b0", "b1", "b3"}, {file_a2_desc}));
    ASSERT_OK(Commit(table_path, commit_msgs_a2));

    ASSERT_OK_AND_ASSIGN(auto commit_msgs_b2,
                         WriteArray(table_path, {}, {"b0", "b1", "b3"}, {file_b2_desc}));
    SetFirstRowId(3, commit_msgs_b2);
    ASSERT_OK(Commit(table_path, commit_msgs_b2));

    // --- Read all data with full schema ---
    std::vector<std::string> read_schema = {"f0", "b0", "b1", "b2", "b3"};
    ASSERT_OK_AND_ASSIGN(auto plan, ScanTable(table_path));

    std::map<std::string, std::string> read_options = {{Options::BLOB_AS_DESCRIPTOR, "false"}};
    ASSERT_OK_AND_ASSIGN(auto result, ReadTable(table_path, read_schema, plan,
                                                /*predicate=*/nullptr, read_options));
    ASSERT_TRUE(result.chunked_array);
    auto read_concat = arrow::Concatenate(result.chunked_array->chunks()).ValueOrDie();
    auto read_struct = std::dynamic_pointer_cast<arrow::StructArray>(read_concat);
    ASSERT_EQ(read_struct->length(), 6);

    // Expected: round1 all columns present; round2 b2=null, b0/b1/b3 from file B (newer)
    std::string expected_json = R"([
        [1, "img_0", "vid_0", "raw_2_0", "raw_3_0"],
        [2, "img_1", null,    "raw_2_1", null      ],
        [3, "img_2", "vid_2", null,      "raw_3_2" ],
        [4, "img_3", "vid_3", null,      "raw_3_3" ],
        [5, null,    "vid_4", null,      "raw_3_4" ],
        [6, "img_5", null,    null,      null      ]
    ])";
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), expected_json)
            .ValueOrDie());

    // Resolve descriptors back to raw bytes
    ASSERT_OK_AND_ASSIGN(auto resolved, ConvertDescriptorToRawBlob(read_struct, {"b0", "b1"}));
    ASSERT_OK_AND_ASSIGN(auto expected_with_rk, PrependRowKindColumn(expected_array));
    ASSERT_TRUE(resolved->type()->Equals(expected_with_rk->type()));
    ASSERT_TRUE(resolved->Equals(expected_with_rk));
}

TEST_P(BlobTableInteTest, TestBlobDescriptorFieldWriteRawBytesDirectly) {
    if (GetParam() == "lance") {
        return;
    }
    // Similar to TestBlobDescriptorFieldWithoutExternalStorage but writes raw bytes directly
    // without converting to descriptor first. The writer should auto-detect that the data
    // is NOT a descriptor (no magic header) and handle it accordingly.
    arrow::FieldVector fields = {arrow::field("f0", arrow::int32()),
                                 BlobUtils::ToArrowField("b0", true),
                                 BlobUtils::ToArrowField("b1", true)};

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},         {Options::FILE_FORMAT, GetParam()},
        {Options::TARGET_FILE_SIZE, "700"},        {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"},   {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::BLOB_DESCRIPTOR_FIELD, "b0,b1"}, {Options::FILE_SYSTEM, "local"}};
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // Write raw bytes directly (no ConvertRawBlobToDescriptor)
    std::string raw_json = R"([
        [1, "image_data_0", "video_data_0"],
        [2, "image_data_1", "video_data_1"],
        [3, "image_data_2", "video_data_2"]
    ])";
    auto raw_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), raw_json).ValueOrDie());

    auto schema = arrow::schema(fields);
    ASSERT_NOK_WITH_MSG(WriteArray(table_path, {}, schema->field_names(), {raw_array}),
                        "BLOB inline field b0 configured by blob-descriptor-field or "
                        "blob-view-field require values "
                        "to be a BlobDescriptor or BlobViewStruct.");
}

}  // namespace paimon::test
