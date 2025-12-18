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

#include "paimon/record_batch.h"

#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(RecordBatchTest, TestSimple) {
    // prepare an arrow array with struct<col1:string,col2:int32,col3:int64,col4:bool>
    auto string_field = arrow::field("col1", arrow::utf8());
    auto int_field = arrow::field("col2", arrow::int32());
    auto long_field = arrow::field("col3", arrow::int64());
    auto bool_field = arrow::field("col4", arrow::boolean());

    auto struct_type = arrow::struct_({string_field, int_field, long_field, bool_field});
    auto schema =
        arrow::schema(arrow::FieldVector({string_field, int_field, long_field, bool_field}));

    arrow::StructBuilder struct_builder(
        struct_type, arrow::default_memory_pool(),
        {std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int32Builder>(),
         std::make_shared<arrow::Int64Builder>(), std::make_shared<arrow::BooleanBuilder>()});
    auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
    auto int_builder = static_cast<arrow::Int32Builder*>(struct_builder.field_builder(1));
    auto long_builder = static_cast<arrow::Int64Builder*>(struct_builder.field_builder(2));
    auto bool_builder = static_cast<arrow::BooleanBuilder*>(struct_builder.field_builder(3));
    for (int32_t i = 0; i < 10; ++i) {
        ASSERT_TRUE(struct_builder.Append().ok());
        ASSERT_TRUE(string_builder->Append("20240813").ok());
        ASSERT_TRUE(int_builder->Append(23).ok());
        ASSERT_TRUE(long_builder->Append(static_cast<int64_t>(1722848484308ll + i)).ok());
        ASSERT_TRUE(bool_builder->Append(static_cast<bool>(i % 2)).ok());
    }
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(struct_builder.Finish(&array).ok());
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    std::map<std::string, std::string> partition = {{"col1", "20240813"}, {"col2", "23"}};
    ASSERT_NOK(batch_builder.SetPartition(partition)
                   .SetRowKinds({RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT})
                   .Finish());
    ::ArrowArray arrow_array2;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array2).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch2,
                         batch_builder.MoveData(&arrow_array2).SetPartition(partition).Finish());

    RecordBatch batch3 = std::move(*batch2);
    ASSERT_EQ(batch3.GetPartition(), partition);

    RecordBatch batch4(std::move(batch3));
    ASSERT_EQ(batch4.GetPartition(), partition);
}

TEST(RecordBatchTest, TestAssignAndMove) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::boolean()),
                                 arrow::field("f1", arrow::int8())};
    std::map<std::string, std::string> partition = {{"f1", "1"}};
    auto old_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [true, 1]
    ])")
            .ValueOrDie());
    ::ArrowArray old_arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*old_array, &old_arrow_array).ok());
    RecordBatch old_batch(partition, /*bucket=*/0, {RecordBatch::RowKind::INSERT},
                          &old_arrow_array);

    auto new_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        [false, 1]
    ])")
            .ValueOrDie());
    ::ArrowArray new_arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*new_array, &new_arrow_array).ok());
    RecordBatch new_batch(partition, /*bucket=*/1, {RecordBatch::RowKind::INSERT},
                          &new_arrow_array);

    old_batch = std::move(new_batch);
    ASSERT_EQ(old_batch.GetBucket(), 1);
    ASSERT_FALSE(
        new_batch.GetData());  // NOLINT(bugprone-use-after-move, clang-analyzer-cplusplus.Move)
    new_batch = std::move(old_batch);
    ASSERT_EQ(new_batch.GetBucket(), 1);
    ASSERT_FALSE(
        old_batch.GetData());  // NOLINT(bugprone-use-after-move, clang-analyzer-cplusplus.Move)
}

}  // namespace paimon::test
