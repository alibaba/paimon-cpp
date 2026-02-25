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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/format/parquet/parquet_file_batch_reader.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_format_writer.h"
#include "paimon/format/parquet/parquet_input_stream_impl.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "parquet/properties.h"

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::parquet::test {

class PredicatePushdownTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
        batch_size_ = 10;

        arrow::FieldVector fields = {
            arrow::field("f0", arrow::utf8()),  arrow::field("f1", arrow::float32()),
            arrow::field("f2", arrow::int64()), arrow::field("f3", arrow::boolean()),
            arrow::field("f4", arrow::int64()), arrow::field("f5", arrow::binary())};

        struct_array_ = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        ["apple", 4.0, 4, true, null, "add"],  ["banana", 4.0, 6, true, null, "bad"],
        ["camera", 4.0, 8, true, null, "cat"], ["data", null, 10, true, null, "dad"]
    ])")
                .ValueOrDie());
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        file_name_ = dir_->Str() + "/test.data";
        fs_ = dir_->GetFileSystem();
    }

    void TearDown() override {}

    void PrepareTestData(const std::shared_ptr<arrow::StructArray>& struct_array) {
        auto data_type = struct_array->struct_type();
        auto data_schema = arrow::schema(data_type->fields());
        auto data_arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*struct_array, data_arrow_array.get()).ok());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs_->Create(file_name_, /*overwrite=*/false));
        ::parquet::WriterProperties::Builder builder;
        builder.write_batch_size(batch_size_);
        auto writer_properties = builder.build();
        ASSERT_OK_AND_ASSIGN(
            auto format_writer,
            ParquetFormatWriter::Create(out, data_schema, writer_properties,
                                        DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));
        ASSERT_OK(format_writer->AddBatch(data_arrow_array.get()));
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK(out->Close());
    }

    void CheckResult(const std::shared_ptr<arrow::Schema>& read_schema,
                     const std::shared_ptr<Predicate>& predicate,
                     const std::shared_ptr<arrow::Array>& expected_array,
                     uint32_t predicate_node_count_limit =
                         paimon::parquet::DEFAULT_PARQUET_READ_PREDICATE_NODE_COUNT_LIMIT) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_name_));
        ASSERT_OK_AND_ASSIGN(uint64_t length, in->Length());
        auto in_stream = std::make_shared<ParquetInputStreamImpl>(in, arrow_pool_, length);

        std::map<std::string, std::string> options;
        options[paimon::parquet::PARQUET_READ_PREDICATE_NODE_COUNT_LIMIT] =
            std::to_string(predicate_node_count_limit);
        ASSERT_OK_AND_ASSIGN(auto batch_reader,
                             ParquetFileBatchReader::Create(std::move(in_stream), arrow_pool_,
                                                            options, batch_size_));
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        auto arrow_status = arrow::ExportSchema(*read_schema, c_schema.get());
        ASSERT_TRUE(arrow_status.ok());
        ASSERT_OK(batch_reader->SetReadSchema(c_schema.get(), predicate,
                                              /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(auto arrow_array,
                             paimon::test::ReadResultCollector::CollectResult(batch_reader.get()));
        if (expected_array) {
            ASSERT_TRUE(arrow_array);
            auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
            ASSERT_TRUE(expected_chunk_array->Equals(arrow_array)) << arrow_array->ToString();
        } else {
            ASSERT_FALSE(arrow_array);
        }
    }

 private:
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<MemoryPool> pool_;
    int32_t batch_size_;
    std::shared_ptr<arrow::StructArray> struct_array_;
    std::shared_ptr<FileSystem> fs_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::string file_name_;
};

TEST_F(PredicatePushdownTest, TestIntDoubleData) {
    PrepareTestData(struct_array_);
    auto data_type = struct_array_->struct_type();
    arrow::FieldVector fields = {data_type->GetFieldByName("f0"), data_type->GetFieldByName("f1"),
                                 data_type->GetFieldByName("f2"), data_type->GetFieldByName("f3"),
                                 data_type->GetFieldByName("f4")};
    auto read_schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected_array =
        arrow::StructArray::Make(
            {struct_array_->GetFieldByName("f0"), struct_array_->GetFieldByName("f1"),
             struct_array_->GetFieldByName("f2"), struct_array_->GetFieldByName("f3"),
             struct_array_->GetFieldByName("f4")},
            fields)
            .ValueOrDie();
    {
        // f1 == 4, has data
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                    Literal(static_cast<float>(4.0)));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f1 == 6, no data
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                    Literal(static_cast<float>(6.0)));
        CheckResult(read_schema, predicate, /*expected_array=*/
                    nullptr);
    }
    {
        // f1 != 4, no data
        auto predicate = PredicateBuilder::NotEqual(
            /*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
            Literal(static_cast<float>(4.0)));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        // f2 != 4, has data
        auto predicate = PredicateBuilder::NotEqual(/*field_index=*/2, /*field_name=*/"f2",
                                                    FieldType::BIGINT, Literal(4l));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f2 == 6, has data
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::BIGINT, Literal(6l));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f2 == 1, no data
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::BIGINT, Literal(1l));
        CheckResult(read_schema, predicate, /*expected_array=*/
                    nullptr);
    }
    {
        // f2 in [1,2,3], no data
        auto predicate =
            PredicateBuilder::In(/*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT,
                                 {Literal(1l), Literal(2l), Literal(3l)});
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        // f2 in [1,2,3] but has small predicate node limit, has data
        auto predicate =
            PredicateBuilder::In(/*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT,
                                 {Literal(1l), Literal(2l), Literal(3l)});
        CheckResult(read_schema, predicate, expected_array,
                    /*predicate_node_count_limit=*/1);
    }
    {
        // f2 not in [1,2,3], has data
        auto predicate =
            PredicateBuilder::NotIn(/*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT,
                                    {Literal(1l), Literal(2l), Literal(3l)});
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f2 in [2,3,4], has data
        auto predicate =
            PredicateBuilder::In(/*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT,
                                 {Literal(2l), Literal(3l), Literal(4l)});
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f2 not in [2,3,4], has data
        auto predicate =
            PredicateBuilder::NotIn(/*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT,
                                    {Literal(2l), Literal(3l), Literal(4l)});
        CheckResult(read_schema, predicate, expected_array);
    }
}

TEST_F(PredicatePushdownTest, TestBoolData) {
    PrepareTestData(struct_array_);
    auto data_type = struct_array_->struct_type();
    arrow::FieldVector fields = {data_type->GetFieldByName("f0"), data_type->GetFieldByName("f1"),
                                 data_type->GetFieldByName("f2"), data_type->GetFieldByName("f3"),
                                 data_type->GetFieldByName("f4")};
    auto read_schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected_array =
        arrow::StructArray::Make(
            {struct_array_->GetFieldByName("f0"), struct_array_->GetFieldByName("f1"),
             struct_array_->GetFieldByName("f2"), struct_array_->GetFieldByName("f3"),
             struct_array_->GetFieldByName("f4")},
            fields)
            .ValueOrDie();
    {
        // f3 is null, no data
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN);
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        // f3 is not null, has data
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN);
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f3 == true, has data
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::BOOLEAN, Literal(true));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::In(/*field_index=*/3, /*field_name=*/"f3",
                                              FieldType::BOOLEAN, {Literal(false)});
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
}

TEST_F(PredicatePushdownTest, TestStringData) {
    PrepareTestData(struct_array_);
    auto data_type = struct_array_->struct_type();
    arrow::FieldVector fields = {data_type->GetFieldByName("f0"), data_type->GetFieldByName("f1"),
                                 data_type->GetFieldByName("f2"), data_type->GetFieldByName("f3"),
                                 data_type->GetFieldByName("f4")};
    auto read_schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected_array =
        arrow::StructArray::Make(
            {struct_array_->GetFieldByName("f0"), struct_array_->GetFieldByName("f1"),
             struct_array_->GetFieldByName("f2"), struct_array_->GetFieldByName("f3"),
             struct_array_->GetFieldByName("f4")},
            fields)
            .ValueOrDie();
    {
        // f0 is null, no data
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING);
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        // f0 is not null, has data
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING);
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f0 == apple, has data
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "apple", 5));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f0 == anything, no data
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                    Literal(FieldType::STRING, "anything", 8));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        // f0 > zooooooo, no data
        auto predicate = PredicateBuilder::GreaterThan(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
            Literal(FieldType::STRING, "zooooooo", 8));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
}

TEST_F(PredicatePushdownTest, TestBinaryData) {
    PrepareTestData(struct_array_);
    auto data_type = struct_array_->struct_type();
    arrow::FieldVector fields = {data_type->GetFieldByName("f5")};
    auto read_schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected_array =
        arrow::StructArray::Make({struct_array_->GetFieldByName("f5")}, fields).ValueOrDie();
    // paimon does not pushdown binary type to parquet, will skip this predicate
    {
        // f5 < aaa, do not have data, but binary predicate will be skipped
        auto predicate =
            PredicateBuilder::LessThan(/*field_index=*/5, /*field_name=*/"f5", FieldType::BINARY,
                                       Literal(FieldType::BINARY, "aaa", 3));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f5 >= zoo, do not have data, but binary predicate will be skipped
        auto predicate = PredicateBuilder::GreaterOrEqual(
            /*field_index=*/5, /*field_name=*/"f5", FieldType::BINARY,
            Literal(FieldType::BINARY, "zoo", 3));
        CheckResult(read_schema, predicate, expected_array);
    }
}

TEST_F(PredicatePushdownTest, TestPredicatePushdownWithAllDataNull) {
    PrepareTestData(struct_array_);
    auto data_type = struct_array_->struct_type();
    arrow::FieldVector fields = {data_type->GetFieldByName("f0"), data_type->GetFieldByName("f1"),
                                 data_type->GetFieldByName("f2"), data_type->GetFieldByName("f3"),
                                 data_type->GetFieldByName("f4")};
    auto read_schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected_array =
        arrow::StructArray::Make(
            {struct_array_->GetFieldByName("f0"), struct_array_->GetFieldByName("f1"),
             struct_array_->GetFieldByName("f2"), struct_array_->GetFieldByName("f3"),
             struct_array_->GetFieldByName("f4")},
            fields)
            .ValueOrDie();
    {
        // f4 is null, has data
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/4, /*field_name=*/"f4", FieldType::BIGINT);
        CheckResult(read_schema, predicate, expected_array);
    }

    // other predicate, always return IS_NULL (no data)
    {
        // f4 in [1,2], no data
        auto predicate = PredicateBuilder::In(/*field_index=*/4, /*field_name=*/"f4",
                                              FieldType::BIGINT, {Literal(1l), Literal(2l)});
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        // f4 not in [1,2], no data
        auto predicate = PredicateBuilder::NotIn(/*field_index=*/4, /*field_name=*/"f4",
                                                 FieldType::BIGINT, {Literal(1l), Literal(2l)});
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        // f4 >= 3, no data
        auto predicate = PredicateBuilder::GreaterOrEqual(/*field_index=*/4, /*field_name=*/"f4",
                                                          FieldType::BIGINT, Literal(3l));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        // f4 <= 3, no data
        auto predicate = PredicateBuilder::LessOrEqual(/*field_index=*/4, /*field_name=*/"f4",
                                                       FieldType::BIGINT, Literal(3l));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
}

TEST_F(PredicatePushdownTest, TestCompoundPredicate) {
    PrepareTestData(struct_array_);
    auto read_schema = arrow::schema(struct_array_->struct_type()->fields());
    std::shared_ptr<arrow::Array> expected_array = struct_array_;
    {
        // f2 < 6 and f1 == 4 and f3 == true, has data
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And(
                {PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f2",
                                            FieldType::BIGINT, Literal(6l)),
                 PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                         Literal(static_cast<float>(4.0))),
                 PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                         Literal(true))}));
        ASSERT_TRUE(predicate);
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f2 < 6 and f1 == 4 and f3 is null, no data
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And(
                {PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f2",
                                            FieldType::BIGINT, Literal(6l)),
                 PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                         Literal(static_cast<float>(4.0))),
                 PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f3",
                                          FieldType::BOOLEAN)}));
        ASSERT_TRUE(predicate);
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        // f2 < 6 and f1 == 4 and f5 is null, no data
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And(
                {PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f2",
                                            FieldType::BIGINT, Literal(6l)),
                 PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                         Literal(static_cast<float>(4.0))),
                 PredicateBuilder::IsNull(/*field_index=*/5, /*field_name=*/"f5",
                                          FieldType::BINARY)}));
        ASSERT_TRUE(predicate);
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        // f2 < 6 and f1 == 4 and f5 == zoo, has data
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And(
                {PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f2",
                                            FieldType::BIGINT, Literal(6l)),
                 PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                         Literal(static_cast<float>(4.0))),
                 PredicateBuilder::Equal(/*field_index=*/5, /*field_name=*/"f5", FieldType::BINARY,
                                         Literal(FieldType::BINARY, "zoo", 3))}));
        ASSERT_TRUE(predicate);
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f2 < 6 and f1 == 5 and f5 is null, no data
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And(
                {PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f2",
                                            FieldType::BIGINT, Literal(6l)),
                 PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                         Literal(static_cast<float>(5.0))),
                 PredicateBuilder::IsNull(/*field_index=*/5, /*field_name=*/"f5",
                                          FieldType::BINARY)}));
        ASSERT_TRUE(predicate);
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        // f2 < 6 or f1 == 4, has data
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::Or(
                {PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f2",
                                            FieldType::BIGINT, Literal(6l)),
                 PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                         Literal(static_cast<float>(4.0)))}));
        ASSERT_TRUE(predicate);
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f2 < 6 or f1 == 5, has data
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::Or(
                {PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f2",
                                            FieldType::BIGINT, Literal(6l)),
                 PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                         Literal(static_cast<float>(5.0)))}));
        ASSERT_TRUE(predicate);
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f2 < 2 or f5 is null, no data
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::Or({PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f2",
                                                             FieldType::BIGINT, Literal(2l)),
                                  PredicateBuilder::IsNull(/*field_index=*/5, /*field_name=*/"f5",
                                                           FieldType::BINARY)}));
        ASSERT_TRUE(predicate);
        CheckResult(read_schema, predicate, nullptr);
    }
    {
        // f2 < 2 or f5 = zoo, ignore <or predicate> as it contains binary
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::Or(
                {PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f2",
                                            FieldType::BIGINT, Literal(2l)),
                 PredicateBuilder::Equal(/*field_index=*/5, /*field_name=*/"f5", FieldType::BINARY,
                                         Literal(FieldType::BINARY, "zoo", 3))}));
        ASSERT_TRUE(predicate);
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f2 < 2 or f1 == 4 or f3 == false, has data
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::Or(
                {PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f2",
                                            FieldType::BIGINT, Literal(2l)),
                 PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                         Literal(static_cast<float>(4.0))),
                 PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3", FieldType::BOOLEAN,
                                         Literal(false))}));
        ASSERT_TRUE(predicate);
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f2 < 2 or f1 == 5 or f3 is null, no data
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::Or(
                {PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f2",
                                            FieldType::BIGINT, Literal(2l)),
                 PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::FLOAT,
                                         Literal(static_cast<float>(5.0))),
                 PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f3",
                                          FieldType::BOOLEAN)}));
        ASSERT_TRUE(predicate);
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
}

TEST_F(PredicatePushdownTest, TestComplexType) {
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()),
        arrow::field("f3", arrow::date32()),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f5", arrow::decimal128(23, 5)),
    };
    auto read_schema = arrow::schema(fields);
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [10, 1, 1234,  "2033-05-18 03:33:20.0",         "123456789987654321.45678"],
        [10, 1, 19909, "2033-05-18 03:33:20.000001001", "12.30000"],
        [10, 1, 0,     "2008-12-28 00:00:00.000123456", "12.30000"],
        [10, 1, 100,   "2008-12-28 00:00:00.00012345",  "-123.45000"],
        [10, 1, 100,  "1899-01-01 00:59:20.001001001", "0.00000"],
        [10, 1, 20006, "2024-10-10 10:10:10.100100100", "1728551410100.10010"]
    ])")
            .ValueOrDie());
    PrepareTestData(expected_array);
    //  date
    {
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f3", FieldType::DATE);
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/2, /*field_name=*/"f3", FieldType::DATE);
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f3",
                                                 FieldType::DATE, Literal(FieldType::DATE, 4));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f3",
                                                 FieldType::DATE, Literal(FieldType::DATE, -111));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::LessThan(
            /*field_index=*/2, /*field_name=*/"f3", FieldType::DATE,
            Literal(FieldType::DATE, 20006));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(
            /*field_index=*/2, /*field_name=*/"f3", FieldType::DATE,
            Literal(FieldType::DATE, 20006));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }

    // timestamp, always be ignored
    {
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f4", FieldType::TIMESTAMP);
        CheckResult(read_schema, predicate, /*expected_array=*/
                    expected_array);
    }
    {
        auto predicate = PredicateBuilder::IsNotNull(/*field_index=*/3, /*field_name=*/"f4",
                                                     FieldType::TIMESTAMP);
        CheckResult(read_schema, predicate, /*expected_array=*/
                    expected_array);
    }
    {
        auto predicate =
            PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f4", FieldType::TIMESTAMP,
                                    Literal(Timestamp(2240521239999l, 0)));
        CheckResult(read_schema, predicate, /*expected_array=*/expected_array);
    }
    {
        auto predicate =
            PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f4", FieldType::TIMESTAMP,
                                       Literal(Timestamp(1230422400000l, 123460)));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f4",
                                                       FieldType::TIMESTAMP,
                                                       Literal(Timestamp(2000000000000l, 1001)));
        CheckResult(read_schema, predicate, /*expected_array=*/expected_array);
    }

    // decimal
    {
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL);
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL);
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::Equal(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            Literal(Decimal(23, 5, 123456)));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::Equal(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            Literal(Decimal(22, 3, -123456)));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::LessThan(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            Literal(Decimal(23, 3, DecimalUtils::StrToInt128("-123456789987654321567").value())));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::LessThan(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            Literal(Decimal(23, 3, DecimalUtils::StrToInt128("123456789987654321567").value())));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            Literal(Decimal(23, 3, DecimalUtils::StrToInt128("123456789987654321567").value())));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::In(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            {Literal(Decimal(23, 5, DecimalUtils::StrToInt128("-12345678998765432134567").value())),
             Literal(Decimal(23, 5, 1234567))});
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::NotIn(
            /*field_index=*/4, /*field_name=*/"f5", FieldType::DECIMAL,
            {Literal(Decimal(23, 5, DecimalUtils::StrToInt128("-12345678998765432134567").value())),
             Literal(Decimal(23, 5, 1234567))});
        CheckResult(read_schema, predicate, expected_array);
    }
}

TEST_F(PredicatePushdownTest, TestAllNullOrAllSameValue) {
    arrow::FieldVector fields = {arrow::field("f1", arrow::int32()),
                                 arrow::field("f2", arrow::int32())};
    auto read_schema = arrow::schema(fields);
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [null, 10],
        [null, 10],
        [null, 10]
    ])")
            .ValueOrDie());
    PrepareTestData(expected_array);
    // for f1
    {
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f1", FieldType::INT);
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/0, /*field_name=*/"f1", FieldType::INT);
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f1",
                                                 FieldType::INT, Literal(10));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(10));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f1",
                                                       FieldType::INT, Literal(10));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::GreaterOrEqual(/*field_index=*/0, /*field_name=*/"f1",
                                                          FieldType::INT, Literal(10));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(10));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::LessOrEqual(/*field_index=*/0, /*field_name=*/"f1",
                                                       FieldType::INT, Literal(10));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f1",
                                              FieldType::INT, {Literal(10), Literal(20)});
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f1",
                                                 FieldType::INT, {Literal(10), Literal(20)});
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    // for f2
    {
        auto predicate = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f2",
                                                    FieldType::INT, Literal(10));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f2",
                                                    FieldType::INT, Literal(30));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::In(/*field_index=*/1, /*field_name=*/"f2",
                                              FieldType::INT, {Literal(10), Literal(20)});
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        auto predicate = PredicateBuilder::NotIn(/*field_index=*/1, /*field_name=*/"f2",
                                                 FieldType::INT, {Literal(10), Literal(20)});
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        auto predicate = PredicateBuilder::NotIn(/*field_index=*/1, /*field_name=*/"f2",
                                                 FieldType::INT, {Literal(20), Literal(30)});
        CheckResult(read_schema, predicate, expected_array);
    }
}
}  // namespace paimon::parquet::test
