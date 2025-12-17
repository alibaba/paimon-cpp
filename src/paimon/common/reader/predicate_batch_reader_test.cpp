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

#include "paimon/common/reader/predicate_batch_reader.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::test {
class PredicateBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        fields_ = {arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int64()),
                   arrow::field("f2", arrow::boolean())};
        data_type_ = arrow::struct_(fields_);
    }

    void TearDown() override {}

    std::shared_ptr<arrow::Array> PrepareArray(int32_t length, int32_t offset = 0) {
        arrow::StructBuilder struct_builder(
            data_type_, arrow::default_memory_pool(),
            {std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int64Builder>(),
             std::make_shared<arrow::BooleanBuilder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        auto big_int_builder = static_cast<arrow::Int64Builder*>(struct_builder.field_builder(1));
        auto bool_builder = static_cast<arrow::BooleanBuilder*>(struct_builder.field_builder(2));
        for (int32_t i = 0 + offset; i < length + offset; ++i) {
            EXPECT_TRUE(struct_builder.Append().ok());
            EXPECT_TRUE(string_builder->Append("str_" + std::to_string(i)).ok());
            EXPECT_TRUE(big_int_builder->Append(i).ok());
            EXPECT_TRUE(bool_builder->Append(static_cast<bool>(i % 2)).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    }

    void CheckResult(std::unique_ptr<BatchReader>&& reader,
                     const std::shared_ptr<Predicate>& predicate,
                     const std::shared_ptr<arrow::ChunkedArray>& expected_array) const {
        ASSERT_OK_AND_ASSIGN(
            auto predicate_reader,
            PredicateBatchReader::Create(std::move(reader), predicate, GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                             ReadResultCollector::CollectResult(predicate_reader.get()));
        if (expected_array) {
            ASSERT_TRUE(result_array->Equals(expected_array));
        } else {
            ASSERT_FALSE(result_array);
        }
    }

 private:
    arrow::FieldVector fields_;
    std::shared_ptr<arrow::DataType> data_type_;
};

TEST_F(PredicateBatchReaderTest, TestSimple) {
    auto data_array = PrepareArray(100);
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(24l));
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array->Slice(0, 24));
        CheckResult(std::move(reader), predicate, expected_array);
    }
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(1l));
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array->Slice(0, 1));
        CheckResult(std::move(reader), predicate, expected_array);
    }
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(99l));
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array->Slice(0, 99));
        CheckResult(std::move(reader), predicate, expected_array);
    }
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::BIGINT, Literal(0l));
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array->Slice(1, 99));
        CheckResult(std::move(reader), predicate, expected_array);
    }
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::BIGINT, Literal(98l));
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array->Slice(99, 1));
        CheckResult(std::move(reader), predicate, expected_array);
    }
}

TEST_F(PredicateBatchReaderTest, TestVariousBatchSize) {
    auto data_array = arrow::ipc::internal::json::ArrayFromJSON(data_type_, R"([
        ["str_-1", -1, false],
        ["str_0", 0, false], ["str_1", 1, true],
        ["str_-1", -1, false],
        ["str_2", 2, false], ["str_3", 3, true],
        ["str_4", 4, false], ["str_5", 5, true], ["str_6", 6, false],
        ["str_-1", -1, false],
        ["str_7", 7, true],
        ["str_-1", -1, false]
    ])")
                          .ValueOrDie();
    auto expected_array = std::make_shared<arrow::ChunkedArray>(PrepareArray(8));
    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                   FieldType::BIGINT, Literal(-1l));
    for (auto batch_size : {5, 10, 11, 20}) {
        auto reader = std::make_unique<MockFileBatchReader>(data_array, data_type_, batch_size);
        CheckResult(std::move(reader), predicate, expected_array);
    }
}

TEST_F(PredicateBatchReaderTest, TestOneByOneCase) {
    auto data_array = PrepareArray(8);
    auto reader = std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
    auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                             FieldType::BOOLEAN, Literal(true));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(data_type_, {R"([
        ["str_1", 1, true], ["str_3", 3, true], ["str_5", 5, true], ["str_7", 7, true]
    ])"},
                                                                         &expected_array);
    CheckResult(std::move(reader), predicate, expected_array);
}

TEST_F(PredicateBatchReaderTest, TestFullAndEmptyCase) {
    auto data_array = PrepareArray(15);
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(20l));
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array);
        CheckResult(std::move(reader), predicate, expected_array);
    }
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::BIGINT, Literal(20l));
        CheckResult(std::move(reader), predicate, nullptr);
    }
}

}  // namespace paimon::test
