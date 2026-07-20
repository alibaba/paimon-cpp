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

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/data/variant.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/table/source/startup_mode.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/variant_test_data.h"

namespace paimon::test {

// End-to-end tests for tables with a VARIANT column: create, write, commit, scan and read.
class VariantTableInteTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        test_dir_ = dir_->Str();
        pool_ = GetDefaultPool();
        fields_ = {arrow::field("id", arrow::int32()), VariantTypeUtils::ToArrowField("v")};
        schema_ = arrow::schema(fields_);
    }

    void TearDown() override {
        dir_.reset();
    }

    std::shared_ptr<arrow::StructArray> BuildArray(const std::vector<const char*>& jsons,
                                                   int32_t id_offset = 0) {
        auto result =
            VariantTestData::BuildVariantBatch(fields_[0], fields_[1], jsons, pool_, id_offset);
        EXPECT_TRUE(result.ok()) << result.status().ToString();
        return std::move(result).value();
    }

    Result<std::unique_ptr<RecordBatch>> MakeBatch(
        const std::shared_ptr<arrow::StructArray>& array) {
        ::ArrowArray arrow_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &arrow_array));
        RecordBatchBuilder batch_builder(&arrow_array);
        return batch_builder.SetPartition({}).SetBucket(0).SetRowKinds({}).Finish();
    }

    // Reads all rows back and checks the variant column renders to `expected_jsons` (nullptr
    // means a null variant). The read result carries a leading `_VALUE_KIND` column.
    void ReadAndCheck(TestHelper* helper, const std::vector<std::shared_ptr<Split>>& splits,
                      const std::vector<int32_t>& expected_ids,
                      const std::vector<const char*>& expected_jsons) {
        ASSERT_OK_AND_ASSIGN(auto result, helper->ReadResult(splits));
        ASSERT_EQ(result->num_chunks(), 1);
        auto result_struct = std::static_pointer_cast<arrow::StructArray>(result->chunk(0));
        ASSERT_EQ(result_struct->length(), static_cast<int64_t>(expected_jsons.size()));
        auto struct_type = std::static_pointer_cast<arrow::StructType>(result_struct->type());
        int32_t id_index = struct_type->GetFieldIndex("id");
        int32_t variant_index = struct_type->GetFieldIndex("v");
        ASSERT_GE(id_index, 0);
        ASSERT_GE(variant_index, 0);
        auto id_column =
            std::static_pointer_cast<arrow::Int32Array>(result_struct->field(id_index));
        auto variant_column =
            std::static_pointer_cast<arrow::StructArray>(result_struct->field(variant_index));
        auto value_column = std::static_pointer_cast<arrow::BinaryArray>(variant_column->field(0));
        auto metadata_column =
            std::static_pointer_cast<arrow::BinaryArray>(variant_column->field(1));
        for (size_t i = 0; i < expected_jsons.size(); ++i) {
            SCOPED_TRACE("row " + std::to_string(i));
            ASSERT_EQ(id_column->Value(i), expected_ids[i]);
            if (expected_jsons[i] == nullptr) {
                ASSERT_TRUE(variant_column->IsNull(i));
                continue;
            }
            ASSERT_FALSE(variant_column->IsNull(i));
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> variant,
                                 GenericVariant::Create(value_column->GetView(i),
                                                        metadata_column->GetView(i), pool_));
            ASSERT_OK_AND_ASSIGN(std::string actual_json, variant->ToJson());
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> expected,
                                 GenericVariant::FromJson(expected_jsons[i], pool_));
            ASSERT_OK_AND_ASSIGN(std::string expected_json, expected->ToJson());
            ASSERT_EQ(actual_json, expected_json);
        }
    }

    // Builds a variant-access projection field via the public builder.
    std::shared_ptr<arrow::Field> BuildAccessField(
        const std::vector<std::pair<std::shared_ptr<arrow::DataType>, std::string>>& accesses,
        const std::string& field_name) {
        VariantAccessBuilder builder;
        for (const auto& [type, path] : accesses) {
            auto target = std::make_unique<ArrowSchema>();
            EXPECT_TRUE(arrow::ExportField(arrow::Field("t", type), target.get()).ok());
            EXPECT_OK(builder.AddField(target.get(), path, /*fail_on_error=*/false));
        }
        auto c_field = builder.Build(field_name);
        EXPECT_TRUE(c_field.ok()) << c_field.status().ToString();
        auto imported = arrow::ImportField(c_field.value().get());
        EXPECT_TRUE(imported.ok()) << imported.status().ToString();
        return imported.ValueOrDie();
    }

    // Reads `splits` back with `read_schema` projected and returns the single result chunk.
    void ReadWithSchema(TestHelper* helper, const std::vector<std::shared_ptr<Split>>& splits,
                        const std::shared_ptr<arrow::Schema>& read_schema,
                        std::shared_ptr<arrow::StructArray>* result_struct) {
        auto c_read_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*read_schema, c_read_schema.get()).ok());
        ASSERT_OK_AND_ASSIGN(auto result, helper->ReadResult(splits, std::move(c_read_schema)));
        ASSERT_EQ(result->num_chunks(), 1);
        *result_struct = std::static_pointer_cast<arrow::StructArray>(result->chunk(0));
    }

 protected:
    std::string test_dir_;
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<MemoryPool> pool_;
    arrow::FieldVector fields_;
    std::shared_ptr<arrow::Schema> schema_;
};

TEST_F(VariantTableInteTest, TestAppendTable) {
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema_, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    // The document set covers deep object/array alternation, escaped and unicode strings,
    // wide integers, decimals, exponent doubles and empty containers.
    std::vector<const char*> jsons = {
        R"({"age": 35, "city": "Hangzhou"})",
        nullptr,
        "[1, \"two\", 3.5, null, true]",
        "{\"nested\": {\"x\": [1, 2]}, \"s\": \"中文\"}",
        R"({
            "user": {
                "id": 9007199254740993,
                "name": "张三 \"quoted\" \\ / \b\f\n\r\t",
                "tags": ["a", 1, 2.5, true, null, {"deep": [[1, [2, [3, [4]]]]]}],
                "address": {
                    "city": "Hangzhou",
                    "geo": {"lat": 30.274085, "lng": 120.15507, "alt": -1.5e-3},
                    "history": [
                        {"year": 2020, "city": "Beijing"},
                        {"year": 2021, "city": "Shanghai", "note": null}
                    ]
                },
                "balance": 12345678901234567890.123456789,
                "scores": [0.1, -0.0, 1e100, -1e-100]
            },
            "empty_object": {},
            "empty_array": [],
            "flags": [true, false, null]
        })",
        R"([{"a": [{"b": {"c": [null, {"d": 1}]}}]}, [], {}, "end"])",
        R"({"unicode": "😀"})",
    };
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, MakeBatch(BuildArray(jsons)));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                                /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt,
                                         /*is_streaming=*/false));
    ReadAndCheck(helper.get(), splits, {0, 1, 2, 3, 4, 5, 6}, jsons);
}

TEST_F(VariantTableInteTest, TestPrimaryKeyTable) {
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "1"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(test_dir_, schema_, /*partition_keys=*/{},
                                                         /*primary_keys=*/{"id"}, options,
                                                         /*is_streaming_mode=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_1,
        MakeBatch(BuildArray({"{\"a\": 1}", "{\"b\": 2}", nullptr}, /*id_offset=*/0)));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_1,
                         helper->WriteAndCommit(std::move(batch_1), /*commit_identifier=*/0,
                                                /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         MakeBatch(BuildArray({"{\"b\": \"updated\"}", "[42]"}, /*id_offset=*/1)));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2,
                         helper->WriteAndCommit(std::move(batch_2), /*commit_identifier=*/1,
                                                /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    // The second batch overwrites ids 1 and 2, so the merged view holds three rows.
    ReadAndCheck(helper.get(), splits, {0, 1, 2}, {"{\"a\": 1}", R"({"b": "updated"})", "[42]"});
}

TEST_F(VariantTableInteTest, TestVariantAccessRead) {
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(test_dir_, schema_, /*partition_keys=*/{},
                                                         /*primary_keys=*/{}, options,
                                                         /*is_streaming_mode=*/false));
    std::vector<const char*> jsons = {R"({"age": 35, "city": "Chicago"})",
                                      R"({"age": 25, "other": "Hello"})", nullptr};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, MakeBatch(BuildArray(jsons)));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                                /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt,
                                         /*is_streaming=*/false));

    auto access_field =
        BuildAccessField({{arrow::int64(), "$.age"}, {arrow::utf8(), "$.other"}}, "v");
    auto read_schema = arrow::schema({fields_[0], access_field});
    std::shared_ptr<arrow::StructArray> result_struct;
    ReadWithSchema(helper.get(), splits, read_schema, &result_struct);
    ASSERT_EQ(result_struct->length(), 3);
    auto struct_type = std::static_pointer_cast<arrow::StructType>(result_struct->type());
    auto v_column = std::static_pointer_cast<arrow::StructArray>(
        result_struct->field(struct_type->GetFieldIndex("v")));
    const auto& age = static_cast<const arrow::Int64Array&>(*v_column->field(0));
    const auto& other = static_cast<const arrow::StringArray&>(*v_column->field(1));
    ASSERT_EQ(age.Value(0), 35);
    ASSERT_EQ(age.Value(1), 25);
    ASSERT_TRUE(v_column->IsNull(2));
    ASSERT_TRUE(other.IsNull(0));
    ASSERT_EQ(other.GetString(1), "Hello");
}

// The two tests below read a variant nested inside a ROW and inside an ARRAY column as a
// variant-access projection. Unlike the format-level tests they go through the whole table read
// path, where the read schema is resolved against the table schema before the read plans see it.
TEST_F(VariantTableInteTest, TestNestedRowVariantAccessRead) {
    // Table: [id, s: ROW<nv: VARIANT, t: STRING>]
    auto struct_field = arrow::field("s", arrow::struct_({VariantTypeUtils::ToArrowField("nv"),
                                                          arrow::field("t", arrow::utf8())}));
    auto table_schema = arrow::schema({fields_[0], struct_field});
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                            /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));

    std::vector<const char*> jsons = {R"({"age": 35, "city": "Chicago"})",
                                      R"({"age": 25, "other": "Hello"})", nullptr};
    auto variant_batch = BuildArray(jsons);
    arrow::StringBuilder sibling_builder;
    for (size_t i = 0; i < jsons.size(); ++i) {
        ASSERT_TRUE(sibling_builder.Append("t" + std::to_string(i)).ok());
    }
    std::shared_ptr<arrow::Array> sibling;
    ASSERT_TRUE(sibling_builder.Finish(&sibling).ok());
    auto struct_data = arrow::ArrayData::Make(
        struct_field->type(), static_cast<int64_t>(jsons.size()), {nullptr},
        {variant_batch->field(1)->data(), sibling->data()}, /*null_count=*/0);
    auto batch_data = arrow::ArrayData::Make(
        arrow::struct_({fields_[0], struct_field}), static_cast<int64_t>(jsons.size()), {nullptr},
        {variant_batch->field(0)->data(), struct_data}, /*null_count=*/0);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(std::make_shared<arrow::StructArray>(batch_data)));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                                /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt,
                                         /*is_streaming=*/false));

    auto access_field =
        BuildAccessField({{arrow::int64(), "$.age"}, {arrow::utf8(), "$.other"}}, "nv");
    auto read_schema = arrow::schema(
        {fields_[0],
         struct_field->WithType(arrow::struct_({access_field, struct_field->type()->field(1)}))});
    std::shared_ptr<arrow::StructArray> result_struct;
    ReadWithSchema(helper.get(), splits, read_schema, &result_struct);

    auto struct_type = std::static_pointer_cast<arrow::StructType>(result_struct->type());
    auto s_column = std::static_pointer_cast<arrow::StructArray>(
        result_struct->field(struct_type->GetFieldIndex("s")));
    const auto& nv = static_cast<const arrow::StructArray&>(*s_column->field(0));
    const auto& age = static_cast<const arrow::Int64Array&>(*nv.field(0));
    const auto& other = static_cast<const arrow::StringArray&>(*nv.field(1));
    const auto& kept_sibling = static_cast<const arrow::StringArray&>(*s_column->field(1));
    ASSERT_EQ(age.Value(0), 35);
    ASSERT_EQ(age.Value(1), 25);
    ASSERT_TRUE(nv.IsNull(2));
    ASSERT_TRUE(other.IsNull(0));
    ASSERT_EQ(other.GetString(1), "Hello");
    for (size_t i = 0; i < jsons.size(); ++i) {
        EXPECT_EQ(kept_sibling.GetString(static_cast<int64_t>(i)), "t" + std::to_string(i));
    }
}

TEST_F(VariantTableInteTest, TestArrayVariantAccessRead) {
    // Table: [id, arr: ARRAY<VARIANT>]
    auto list_field = arrow::field("arr", arrow::list(VariantTypeUtils::ToArrowField("element")));
    auto table_schema = arrow::schema({fields_[0], list_field});
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(test_dir_, table_schema, /*partition_keys=*/{},
                                            /*primary_keys=*/{}, options,
                                            /*is_streaming_mode=*/false));

    // Row 0 holds two elements, row 1 is empty and row 2 holds one.
    std::vector<const char*> flat = {R"({"x": 1, "y": 2})", R"({"x": 3})", R"({"x": 5})"};
    std::vector<int32_t> offsets = {0, 2, 2, 3};
    arrow::Int32Builder offset_builder;
    ASSERT_TRUE(offset_builder.AppendValues(offsets).ok());
    std::shared_ptr<arrow::Array> offset_array;
    ASSERT_TRUE(offset_builder.Finish(&offset_array).ok());
    auto elements = BuildArray(flat);
    arrow::Int32Builder id_builder;
    ASSERT_TRUE(id_builder.AppendValues({0, 1, 2}).ok());
    std::shared_ptr<arrow::Array> ids;
    ASSERT_TRUE(id_builder.Finish(&ids).ok());
    auto list_data = arrow::ArrayData::Make(list_field->type(), /*length=*/3,
                                            {nullptr, offset_array->data()->buffers[1]},
                                            {elements->field(1)->data()}, /*null_count=*/0);
    auto batch_data = arrow::ArrayData::Make(arrow::struct_({fields_[0], list_field}), /*length=*/3,
                                             {nullptr}, {ids->data(), list_data}, /*null_count=*/0);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(std::make_shared<arrow::StructArray>(batch_data)));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                                /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt,
                                         /*is_streaming=*/false));

    auto access_field = BuildAccessField({{arrow::int64(), "$.x"}}, "element");
    auto read_schema = arrow::schema({fields_[0], list_field->WithType(arrow::list(access_field))});
    std::shared_ptr<arrow::StructArray> result_struct;
    ReadWithSchema(helper.get(), splits, read_schema, &result_struct);

    auto struct_type = std::static_pointer_cast<arrow::StructType>(result_struct->type());
    const auto& list = static_cast<const arrow::ListArray&>(
        *result_struct->field(struct_type->GetFieldIndex("arr")));
    ASSERT_EQ(list.length(), 3);
    ASSERT_EQ(list.value_length(0), 2);
    ASSERT_EQ(list.value_length(1), 0);
    ASSERT_EQ(list.value_length(2), 1);
    const auto& x = static_cast<const arrow::Int64Array&>(
        *static_cast<const arrow::StructArray&>(*list.values()).field(0));
    ASSERT_EQ(x.Value(list.value_offset(0)), 1);
    ASSERT_EQ(x.Value(list.value_offset(0) + 1), 3);
    ASSERT_EQ(x.Value(list.value_offset(2)), 5);
}

TEST_F(VariantTableInteTest, TestReadWithIOException) {
    // Injects an IO error at every position of the scan+read path and verifies each failure
    // surfaces as a clean error status.
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema_, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    std::vector<const char*> jsons = {R"({"age": 35, "city": "Hangzhou"})", nullptr, "[1, 2, 3]"};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, MakeBatch(BuildArray(jsons)));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                                /*expected_commit_messages=*/std::nullopt));

    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 500; i++) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        Result<std::vector<std::shared_ptr<Split>>> splits =
            helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt,
                            /*is_streaming=*/false);
        CHECK_HOOK_STATUS(splits.status(), i);
        Result<std::shared_ptr<arrow::ChunkedArray>> read_result =
            helper->ReadResult(splits.value());
        CHECK_HOOK_STATUS(read_result.status(), i);
        run_complete = true;
        // All IO succeeded before the injected position was reached: check the data.
        io_hook->Clear();
        ReadAndCheck(helper.get(), splits.value(), {0, 1, 2}, jsons);
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_F(VariantTableInteTest, TestWriteWithIOException) {
    // Injects an IO error at every position of the create+write+commit path (on a fresh table
    // directory per attempt) and verifies each failure surfaces as a clean error status.
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "-1"},
    };
    std::vector<const char*> jsons = {R"({"age": 35, "city": "Hangzhou"})", nullptr};
    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 500; i++) {
        std::string table_dir = test_dir_ + fmt::format("/io_exception_{}", i);
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        Result<std::unique_ptr<TestHelper>> helper =
            TestHelper::Create(table_dir, schema_, /*partition_keys=*/{},
                               /*primary_keys=*/{}, options, /*is_streaming_mode=*/false);
        CHECK_HOOK_STATUS(helper.status(), i);
        Result<std::unique_ptr<RecordBatch>> batch = MakeBatch(BuildArray(jsons));
        CHECK_HOOK_STATUS(batch.status(), i);
        Result<std::vector<std::shared_ptr<CommitMessage>>> commit_msgs =
            helper.value()->WriteAndCommit(std::move(batch).value(), /*commit_identifier=*/0,
                                           /*expected_commit_messages=*/std::nullopt);
        CHECK_HOOK_STATUS(commit_msgs.status(), i);
        run_complete = true;
        // All IO succeeded before the injected position was reached: check the data.
        io_hook->Clear();
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> splits,
                             helper.value()->NewScan(StartupMode::LatestFull(),
                                                     /*snapshot_id=*/std::nullopt,
                                                     /*is_streaming=*/false));
        ReadAndCheck(helper.value().get(), splits, {0, 1}, jsons);
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_F(VariantTableInteTest, TestOrcFormatRejected) {
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "avro"},
        {Options::FILE_FORMAT, "orc"},
        {Options::BUCKET, "-1"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(test_dir_, schema_, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, MakeBatch(BuildArray({"{\"a\": 1}"})));
    auto result = helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                         /*expected_commit_messages=*/std::nullopt);
    ASSERT_FALSE(result.ok());
}

}  // namespace paimon::test
