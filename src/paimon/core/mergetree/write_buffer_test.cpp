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

#include "paimon/core/mergetree/write_buffer.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
template <typename T>
class MergeFunctionWrapper;
}  // namespace paimon

namespace paimon::test {
struct ReaderResult {
    std::vector<int64_t> sequence_numbers;
    std::vector<int8_t> row_kind_values;
};

class WriteBufferTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        value_fields_ = {DataField(0, arrow::field("f0", arrow::utf8())),
                         DataField(1, arrow::field("f1", arrow::int32())),
                         DataField(2, arrow::field("f2", arrow::int32())),
                         DataField(3, arrow::field("f3", arrow::float64()))};
        value_type_ = DataField::ConvertDataFieldsToArrowStructType(value_fields_);
        primary_keys_ = {"f0"};
        ASSERT_OK_AND_ASSIGN(key_comparator_,
                             FieldsComparator::Create({value_fields_[0]},
                                                      /*is_ascending_order=*/true));

        auto merge_function = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        merge_function_wrapper_ =
            std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
    }

    std::unique_ptr<RecordBatch> CreateBatch(
        const std::shared_ptr<arrow::Array>& array,
        const std::vector<RecordBatch::RowKind>& row_kinds) const {
        ::ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        RecordBatchBuilder batch_builder(&c_array);
        batch_builder.SetRowKinds(row_kinds);
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.Finish());
        return batch;
    }

    Result<ReaderResult> ReadReaderResult(KeyValueRecordReader* reader) const {
        PAIMON_ASSIGN_OR_RAISE(auto iterator, reader->NextBatch());

        ReaderResult result;
        while (iterator->HasNext()) {
            PAIMON_ASSIGN_OR_RAISE(KeyValue key_value, iterator->Next());
            result.sequence_numbers.push_back(key_value.sequence_number);
            result.row_kind_values.push_back(key_value.value_kind->ToByteValue());
        }
        return result;
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::vector<DataField> value_fields_;
    std::shared_ptr<arrow::DataType> value_type_;
    std::vector<std::string> primary_keys_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
};

TEST_F(WriteBufferTest, TestFlushResetsStateAndAdvancesSequenceNumber) {
    WriteBuffer write_buffer(value_type_, primary_keys_, /*user_defined_sequence_fields=*/{},
                             key_comparator_, merge_function_wrapper_, pool_);

    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Alice", 10, 0, 13.1],
      ["Bob", 20, 1, 14.1]
    ])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> array2 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Charlie", 30, 2, 15.1]
    ])")
            .ValueOrDie();

    ASSERT_OK(write_buffer.Write(CreateBatch(array1, /*row_kinds=*/{})));
    ASSERT_OK(write_buffer.Write(CreateBatch(array2, /*row_kinds=*/{})));
    ASSERT_FALSE(write_buffer.IsEmpty());
    ASSERT_GT(write_buffer.GetMemoryUsage(), 0);

    int64_t last_sequence_number = 10;
    ASSERT_OK_AND_ASSIGN(auto readers, write_buffer.DrainToReaders(&last_sequence_number));

    ASSERT_EQ(readers.size(), 2);
    ASSERT_TRUE(write_buffer.IsEmpty());
    ASSERT_EQ(write_buffer.GetMemoryUsage(), 0);
    ASSERT_EQ(last_sequence_number, 13);

    ASSERT_OK_AND_ASSIGN(auto first_result, ReadReaderResult(readers[0].get()));
    ASSERT_EQ(first_result.sequence_numbers, (std::vector<int64_t>{10, 11}));
    ASSERT_EQ(
        first_result.row_kind_values,
        (std::vector<int8_t>{RowKind::Insert()->ToByteValue(), RowKind::Insert()->ToByteValue()}));

    ASSERT_OK_AND_ASSIGN(auto second_result, ReadReaderResult(readers[1].get()));
    ASSERT_EQ(second_result.sequence_numbers, (std::vector<int64_t>{12}));
    ASSERT_EQ(second_result.row_kind_values,
              (std::vector<int8_t>{RowKind::Insert()->ToByteValue()}));
}

TEST_F(WriteBufferTest, TestFlushPreservesRowKinds) {
    WriteBuffer write_buffer(value_type_, primary_keys_, /*user_defined_sequence_fields=*/{},
                             key_comparator_, merge_function_wrapper_, pool_);

    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Alice", 10, 0, 13.1],
      ["Bob", 20, 1, 14.1],
      ["Charlie", 30, 2, 15.1],
      ["Diana", 40, 3, 16.1]
    ])")
            .ValueOrDie();
    std::vector<RecordBatch::RowKind> row_kinds = {
        RecordBatch::RowKind::INSERT,
        RecordBatch::RowKind::UPDATE_BEFORE,
        RecordBatch::RowKind::UPDATE_AFTER,
        RecordBatch::RowKind::DELETE,
    };

    ASSERT_OK(write_buffer.Write(CreateBatch(array, row_kinds)));

    int64_t last_sequence_number = 0;
    ASSERT_OK_AND_ASSIGN(auto readers, write_buffer.DrainToReaders(&last_sequence_number));
    ASSERT_EQ(readers.size(), 1);
    ASSERT_EQ(last_sequence_number, 4);

    ASSERT_OK_AND_ASSIGN(auto reader_result, ReadReaderResult(readers[0].get()));

    ASSERT_EQ(reader_result.row_kind_values,
              (std::vector<int8_t>{
                  RowKind::Insert()->ToByteValue(), RowKind::UpdateBefore()->ToByteValue(),
                  RowKind::UpdateAfter()->ToByteValue(), RowKind::Delete()->ToByteValue()}));
    ASSERT_EQ(reader_result.sequence_numbers, (std::vector<int64_t>{0, 1, 2, 3}));
}

TEST_F(WriteBufferTest, TestEstimateMemoryUse) {
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
          ["Lucy", 20, 1, 14.1],
          ["Paul", 20, 1, null],
          ["Alice", 10, 0, 13.1]
        ])")
                .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(int64_t memory_use, WriteBuffer::EstimateMemoryUse(array));
        int64_t expected_memory_use =
            1 + (13 + 3 * 4 + 1) + (3 * 4 + 1) + (3 * 4 + 1) + (3 * 8 + 1);
        ASSERT_EQ(memory_use, expected_memory_use);
    }
    {
        arrow::FieldVector fields = {arrow::field("v0", arrow::boolean()),
                                     arrow::field("v1", arrow::int8()),
                                     arrow::field("v2", arrow::int16()),
                                     arrow::field("v3", arrow::int32()),
                                     arrow::field("v4", arrow::int64()),
                                     arrow::field("v5", arrow::float32()),
                                     arrow::field("v6", arrow::float64()),
                                     arrow::field("v7", arrow::date32()),
                                     arrow::field("v8", arrow::timestamp(arrow::TimeUnit::NANO)),
                                     arrow::field("v9", arrow::decimal128(30, 20)),
                                     arrow::field("v10", arrow::utf8()),
                                     arrow::field("v11", arrow::binary())};

        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [true, 10, 200, 65536, 123456789, 0.0, 0.0, 2000, -86399999999500, "2134.48690000000000000009", "difference", "Alice"],
        [false, -128, -32768, -2147483648, -9223372036854775808, -3.4028235E38, -1.7976931348623157E308, -719528, -9223372036854775808, "-999999999999999999.99999999999999999999", "Alice", "Two"],
        [true, 127, 32767, 2147483647, 9223372036854775807, 3.4028235E38, 1.7976931348623157E308, 2932896, 9223372036854775807, "999999999999999999.99999999999999999999", "Alice", "made"],
        [true, 0, 0, 0, 0, 1.4E-45, 4.9E-324, 0, 0, "0.00000000000000000000", "Alice", "wood"]
])")
                .ValueOrDie());
        ASSERT_OK_AND_ASSIGN(int64_t memory_use, WriteBuffer::EstimateMemoryUse(array));
        int64_t expected_memory_use = 1 + (4 + 1) + (4 + 1) + (2 * 4 + 1) + (4 * 4 + 1) +
                                      (8 * 4 + 1) + (4 * 4 + 1) + (8 * 4 + 1) + (4 * 4 + 1) +
                                      (8 * 4 + 1) + (4 * 16 + 1) + (25 + 4 * 4 + 1) +
                                      (16 + 4 * 4 + 1);
        ASSERT_EQ(memory_use, expected_memory_use);
    }
    {
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::list(arrow::int32())),
            arrow::field("f1", arrow::map(arrow::utf8(), arrow::int64())),
            arrow::field("f2", arrow::struct_({arrow::field("sub1", arrow::int64()),
                                               arrow::field("sub2", arrow::float64()),
                                               arrow::field("sub3", arrow::boolean())})),
        };
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [[1, 2, 3],    [["apple", 3], ["banana", 4]],          [10, 10.1, false]],
        [[4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], [20, 20.1, true]],
        [[6],          [["elephant", 7], ["fox", 8]],          [null, 30.1, true]]
    ])")
                .ValueOrDie());
        ASSERT_OK_AND_ASSIGN(int64_t memory_use, WriteBuffer::EstimateMemoryUse(array));
        int64_t list_mem = 1 + (4 * 6 + 1);
        int64_t map_mem = 1 + (33 + 4 * 7 + 1) + (8 * 7 + 1);
        int64_t struct_mem = 1 + (8 * 3 + 1) + (8 * 3 + 1) + (1 * 3 + 1);
        int64_t expected_memory_use = 1 + list_mem + map_mem + struct_mem;
        ASSERT_EQ(memory_use, expected_memory_use);
    }
}

}  // namespace paimon::test
