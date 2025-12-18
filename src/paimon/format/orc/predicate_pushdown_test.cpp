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
#include "orc/OrcFile.hh"
#include "paimon/defs.h"
#include "paimon/format/orc/orc_file_batch_reader.h"
#include "paimon/format/orc/orc_format_defs.h"
#include "paimon/format/orc/orc_format_writer.h"
#include "paimon/format/orc/orc_input_stream_impl.h"
#include "paimon/format/orc/orc_output_stream_impl.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::orc::test {

class PredicatePushdownTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
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
        PrepareTestData(struct_array_);
    }
    void TearDown() override {}

    void PrepareTestData(const std::shared_ptr<arrow::StructArray>& array) {
        auto data_type = array->struct_type();
        auto data_schema = arrow::schema(data_type->fields());
        auto data_arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*array, data_arrow_array.get()).ok());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs_->Create(file_name_, /*overwrite=*/true));
        ASSERT_OK_AND_ASSIGN(auto out_stream, OrcOutputStreamImpl::Create(out));
        ASSERT_OK_AND_ASSIGN(
            auto format_writer,
            OrcFormatWriter::Create(std::move(out_stream), *data_schema, /*options=*/{},
                                    /*compression=*/"zstd", batch_size_, pool_));
        ASSERT_OK(format_writer->AddBatch(data_arrow_array.get()));
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK(out->Close());
    }

    void CheckResult(const std::shared_ptr<arrow::Schema>& read_schema,
                     const std::shared_ptr<Predicate>& predicate,
                     const std::shared_ptr<arrow::Array>& expected_array, bool result_ok = true) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_name_));
        ASSERT_OK_AND_ASSIGN(auto in_stream,
                             OrcInputStreamImpl::Create(in, DEFAULT_NATURAL_READ_SIZE));
        ASSERT_OK_AND_ASSIGN(auto orc_batch_reader,
                             OrcFileBatchReader::Create(std::move(in_stream), pool_,
                                                        /*options=*/{}, batch_size_));
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        auto arrow_status = arrow::ExportSchema(*read_schema, c_schema.get());
        ASSERT_TRUE(arrow_status.ok());
        ASSERT_OK(orc_batch_reader->SetReadSchema(c_schema.get(), predicate,
                                                  /*selection_bitmap=*/std::nullopt));
        auto result = paimon::test::ReadResultCollector::CollectResult(orc_batch_reader.get());
        if (result_ok) {
            ASSERT_TRUE(result.ok());
            // check result
            auto& arrow_array = result.value();
            if (expected_array) {
                ASSERT_TRUE(arrow_array);
                auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
                ASSERT_TRUE(expected_chunk_array->Equals(arrow_array)) << arrow_array->ToString();
            } else {
                ASSERT_FALSE(arrow_array);
            }
        } else {
            ASSERT_FALSE(result.ok());
        }
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    int32_t batch_size_;
    std::shared_ptr<arrow::StructArray> struct_array_;
    std::shared_ptr<FileSystem> fs_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::string file_name_;
};

TEST_F(PredicatePushdownTest, TestIntDoubleData) {
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
        // f3 in [true, false], has data
        auto predicate = PredicateBuilder::In(/*field_index=*/3, /*field_name=*/"f3",
                                              FieldType::BOOLEAN, {Literal(false), Literal(true)});
        CheckResult(read_schema, predicate, expected_array);
    }
}

TEST_F(PredicatePushdownTest, TestStringData) {
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

    // orc string type need set option for statistics, otherwise predicate always return
    // YES_NO_NULL (has data)
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
    auto data_type = struct_array_->struct_type();
    arrow::FieldVector fields = {data_type->GetFieldByName("f5")};
    auto read_schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected_array =
        arrow::StructArray::Make({struct_array_->GetFieldByName("f5")}, fields).ValueOrDie();
    // paimon do not pushdown binary type to orc, will skip this predicate
    {
        // f5 < anything, has data
        auto predicate =
            PredicateBuilder::LessThan(/*field_index=*/5, /*field_name=*/"f5", FieldType::BINARY,
                                       Literal(FieldType::BINARY, "anything", 8));
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f5 >= anything, has data
        auto predicate = PredicateBuilder::GreaterOrEqual(
            /*field_index=*/5, /*field_name=*/"f5", FieldType::BINARY,
            Literal(FieldType::BINARY, "anything", 8));
        CheckResult(read_schema, predicate, expected_array);
    }
}

TEST_F(PredicatePushdownTest, TestPredicatePushdownWithAllDataNull) {
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
        // f4 == null, has data
        auto predicate = PredicateBuilder::Equal(/*field_index=*/4, /*field_name=*/"f4",
                                                 FieldType::BIGINT, Literal(FieldType::BIGINT));
        CheckResult(read_schema, predicate, expected_array);
    }
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

TEST_F(PredicatePushdownTest, TestPredicatePushdownWithNullLiteral) {
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
        //  f2 in [], orc create reader throw exception
        auto predicate =
            PredicateBuilder::In(/*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT, {});
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_name_));
        ASSERT_OK_AND_ASSIGN(auto in_stream,
                             OrcInputStreamImpl::Create(in, DEFAULT_NATURAL_READ_SIZE));
        ASSERT_OK_AND_ASSIGN(auto orc_batch_reader,
                             OrcFileBatchReader::Create(std::move(in_stream), pool_,
                                                        /*options=*/{}, batch_size_));
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        auto arrow_status = arrow::ExportSchema(*read_schema, c_schema.get());
        ASSERT_TRUE(arrow_status.ok());
        ASSERT_NOK_WITH_MSG(
            orc_batch_reader->SetReadSchema(c_schema.get(), predicate, /*selection_bitmap=*/
                                            std::nullopt),
            "predicate [In] need literal on field f2");
    }
    {
        // f2 < null, orc NextBatch throw exception
        auto predicate = PredicateBuilder::LessThan(
            /*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT, Literal(FieldType::BIGINT));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr,
                    /*result_ok=*/false);
    }
    {
        // f2 >= null, orc NextBatch throw exception
        auto predicate = PredicateBuilder::GreaterOrEqual(
            /*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT, Literal(FieldType::BIGINT));
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr,
                    /*result_ok=*/false);
    }
    {
        // f2 in [1,null,2], no data
        auto predicate = PredicateBuilder::In(
            /*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT,
            {Literal(1l), Literal(FieldType::BIGINT), Literal(2l)});
        CheckResult(read_schema, predicate, /*expected_array=*/nullptr);
    }
    {
        // f2 in [1,null,2,4], has data
        auto predicate = PredicateBuilder::In(
            /*field_index=*/2, /*field_name=*/"f2", FieldType::BIGINT,
            {Literal(1l), Literal(FieldType::BIGINT), Literal(2l), Literal(4l)});
        CheckResult(read_schema, predicate, expected_array);
    }
}

TEST_F(PredicatePushdownTest, TestCompoundPredicate) {
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
        // f2 < 6 and f1 == 4 and f5 is null, will ignore binary predicate, has data
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
        CheckResult(read_schema, predicate, expected_array);
    }
    {
        // f2 < 6 and f1 == 5 and f5 is null, will ignore binary predicate, no data
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
        // f2 < 2 or f5 is null, will skip this predicate, has data
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::Or({PredicateBuilder::LessThan(/*field_index=*/2, /*field_name=*/"f2",
                                                             FieldType::BIGINT, Literal(2l)),
                                  PredicateBuilder::IsNull(/*field_index=*/5, /*field_name=*/"f5",
                                                           FieldType::BINARY)}));
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

}  // namespace paimon::orc::test
