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

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/data/variant.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/table/source/startup_mode.h"
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
        auto result = BuildVariantBatch(fields_[0], fields_[1], jsons, pool_, id_offset);
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
    std::vector<const char*> jsons = {
        R"({"age": 35, "city": "Hangzhou"})",
        nullptr,
        "[1, \"two\", 3.5, null, true]",
        "{\"nested\": {\"x\": [1, 2]}, \"s\": \"中文\"}",
    };
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, MakeBatch(BuildArray(jsons)));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0,
                                                /*expected_commit_messages=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt,
                                         /*is_streaming=*/false));
    ReadAndCheck(helper.get(), splits, {0, 1, 2, 3}, jsons);
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

    VariantAccessBuilder access_builder;
    auto age_target = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportField(arrow::Field("t", arrow::int64()), age_target.get()).ok());
    ASSERT_OK(access_builder.AddField(age_target.get(), "$.age", /*fail_on_error=*/false));
    auto other_target = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportField(arrow::Field("t", arrow::utf8()), other_target.get()).ok());
    ASSERT_OK(access_builder.AddField(other_target.get(), "$.other", /*fail_on_error=*/false));
    ASSERT_OK_AND_ASSIGN(auto c_access_field, access_builder.Build("v"));
    auto imported_access = arrow::ImportField(c_access_field.get());
    ASSERT_TRUE(imported_access.ok()) << imported_access.status().ToString();
    auto read_schema = arrow::schema({fields_[0], imported_access.ValueOrDie()});
    auto c_read_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*read_schema, c_read_schema.get()).ok());

    ASSERT_OK_AND_ASSIGN(auto result, helper->ReadResult(splits, std::move(c_read_schema)));
    ASSERT_EQ(result->num_chunks(), 1);
    auto result_struct = std::static_pointer_cast<arrow::StructArray>(result->chunk(0));
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
