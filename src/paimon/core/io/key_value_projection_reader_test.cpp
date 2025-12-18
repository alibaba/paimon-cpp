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

#include "paimon/core/io/key_value_projection_reader.h"

#include <cassert>
#include <ostream>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_nested.h"
#include "arrow/c/abi.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/util/checked_cast.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/io/async_key_value_projection_reader.h"
#include "paimon/core/io/concat_key_value_record_reader.h"
#include "paimon/core/io/key_value_data_file_record_reader.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/mergetree/compact/sort_merge_reader_with_min_heap.h"
#include "paimon/core/utils/fields_comparator.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class KeyValueProjectionReaderTest : public testing::Test,
                                     public ::testing::WithParamInterface<bool> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    std::unique_ptr<BatchReader> GenerateProjectionReader(
        const std::shared_ptr<arrow::Array>& src_array,
        const std::shared_ptr<arrow::Schema>& target_schema,
        const std::vector<int32_t>& target_to_src_mapping, int32_t key_arity,
        const std::shared_ptr<arrow::Schema>& value_schema, int32_t batch_size,
        bool multi_thread_row_to_batch) const {
        auto src_type = src_array->type();
        std::vector<DataField> key_fields;
        std::vector<int32_t> key_sort_fields;
        for (int32_t i = 0; i < key_arity; i++) {
            key_fields.emplace_back(/*id=*/0, src_type->field(i));
            key_sort_fields.push_back(i);
        }
        EXPECT_OK_AND_ASSIGN(
            std::shared_ptr<FieldsComparator> user_key_comparator,
            FieldsComparator::Create(key_fields, key_sort_fields,
                                     /*is_ascending_order=*/true, /*use_view=*/true));

        auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        auto merge_function_wrapper =
            std::make_shared<ReducerMergeFunctionWrapper>(std::move(mfunc));
        auto file_batch_reader = std::make_unique<MockFileBatchReader>(src_array, src_array->type(),
                                                                       /*batch_size=*/batch_size);
        auto record_reader = std::make_unique<KeyValueDataFileRecordReader>(
            std::move(file_batch_reader), key_arity, value_schema, /*level=*/0, pool_);
        std::vector<std::unique_ptr<KeyValueRecordReader>> readers;
        readers.push_back(std::move(record_reader));
        std::vector<std::unique_ptr<KeyValueRecordReader>> concat_readers;
        concat_readers.push_back(std::make_unique<ConcatKeyValueRecordReader>(std::move(readers)));

        auto sort_merge_reader = std::make_unique<SortMergeReaderWithMinHeap>(
            std::move(concat_readers), user_key_comparator,
            /*user_defined_seq_comparator=*/nullptr, merge_function_wrapper);
        if (!multi_thread_row_to_batch) {
            EXPECT_OK_AND_ASSIGN(auto projection_reader, KeyValueProjectionReader::Create(
                                                             std::move(sort_merge_reader),
                                                             target_schema, target_to_src_mapping,
                                                             /*batch_size=*/batch_size, pool_));
            return std::move(projection_reader);
        } else {
            return std::make_unique<AsyncKeyValueProjectionReader>(
                std::move(sort_merge_reader), target_schema, target_to_src_mapping, batch_size,
                /*projection_thread_num=*/3, pool_);
        }
    }

    void CheckResult(const std::shared_ptr<arrow::Array>& src_array,
                     const std::shared_ptr<arrow::StructType>& target_type,
                     const std::vector<int32_t>& target_to_src_mapping, int32_t key_arity,
                     const std::shared_ptr<arrow::Schema>& value_schema,
                     const std::shared_ptr<arrow::ChunkedArray>& expected_array,
                     const std::shared_ptr<arrow::Schema>& expected_sort_schema,
                     int32_t expected_reserve_count) const {
        bool multi_thread_row_to_batch = GetParam();
        auto target_schema = arrow::schema(target_type->fields());
        for (const auto& batch_size : {1, 2, 3, 4, 100}) {
            auto projection_reader =
                GenerateProjectionReader(src_array, target_schema, target_to_src_mapping, key_arity,
                                         value_schema, batch_size, multi_thread_row_to_batch);
            ASSERT_OK_AND_ASSIGN(auto result, paimon::test::ReadResultCollector::CollectResult(
                                                  projection_reader.get()));
            // after read eof, always return nullptr
            ASSERT_OK_AND_ASSIGN(auto eof_result, projection_reader->NextBatch());
            ASSERT_FALSE(eof_result.first);

            if (!multi_thread_row_to_batch) {
                ASSERT_TRUE(result);
                ASSERT_TRUE(expected_array->Equals(result));
            } else {
                ASSERT_TRUE(result);
                ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> sorted_result,
                                     ReadResultCollector::SortArray(result, expected_sort_schema));
                ASSERT_OK_AND_ASSIGN(
                    std::shared_ptr<arrow::ChunkedArray> sorted_expected,
                    ReadResultCollector::SortArray(expected_array, expected_sort_schema));
                ASSERT_TRUE(sorted_result->Equals(*sorted_expected))
                    << sorted_result->ToString() << std::endl
                    << sorted_expected->ToString();
            }
            projection_reader->Close();
            // test reserve and accumulate
            if (!multi_thread_row_to_batch) {
                auto* typed_projection_reader =
                    dynamic_cast<KeyValueProjectionReader*>(projection_reader.get());
                ASSERT_TRUE(typed_projection_reader);
                auto& consumer = typed_projection_reader->key_value_consumer_;
                ASSERT_EQ(consumer->reserved_sizes_.size(), expected_reserve_count);
                for (const auto& reserved_size : consumer->reserved_sizes_) {
                    ASSERT_GT(reserved_size, 0);
                }
            }
        }
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_P(KeyValueProjectionReaderTest, TestBulkData) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int16()),
                                 arrow::field("v1", arrow::float32()),
                                 arrow::field("v2", arrow::utf8())};
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_({fields});

    auto arrow_pool = GetArrowPool(pool_);
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    ASSERT_TRUE(arrow::MakeBuilder(arrow_pool.get(), src_type, &array_builder).ok());

    auto struct_builder =
        arrow::internal::checked_pointer_cast<arrow::StructBuilder>(std::move(array_builder));
    auto seq_builder = static_cast<arrow::Int64Builder*>(struct_builder->field_builder(0));
    auto kind_builder = static_cast<arrow::Int8Builder*>(struct_builder->field_builder(1));
    auto int_builder = static_cast<arrow::Int32Builder*>(struct_builder->field_builder(2));
    auto short_builder = static_cast<arrow::Int16Builder*>(struct_builder->field_builder(3));
    auto float_builder = static_cast<arrow::FloatBuilder*>(struct_builder->field_builder(4));
    auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder->field_builder(5));
    for (int32_t i = 0; i < 2000; ++i) {
        ASSERT_TRUE(struct_builder->Append().ok());
        ASSERT_TRUE(int_builder->Append(i).ok());
        ASSERT_TRUE(seq_builder->Append(0).ok());
        ASSERT_TRUE(kind_builder->Append(0).ok());

        if (i % 2 == 0) {
            // test null value
            ASSERT_TRUE(short_builder->AppendNull().ok());
            ASSERT_TRUE(float_builder->AppendNull().ok());
            ASSERT_TRUE(string_builder->AppendNull().ok());
        } else {
            ASSERT_TRUE(short_builder->Append(i * 2).ok());
            ASSERT_TRUE(float_builder->Append(i + 0.1).ok());
            ASSERT_TRUE(string_builder->Append("str_" + std::to_string(i)).ok());
        }
    }
    std::shared_ptr<arrow::Array> src_array;
    ASSERT_TRUE(struct_builder->Finish(&src_array).ok());
    auto typed_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(src_array);

    auto target_type = std::dynamic_pointer_cast<arrow::StructType>(
        arrow::struct_({fields[2], fields[3], fields[4], fields[5]}));
    ASSERT_TRUE(target_type);
    std::vector<int32_t> target_to_src_mapping = {0, 1, 2, 3};

    auto expected_array = arrow::StructArray::Make({typed_array->field(2), typed_array->field(3),
                                                    typed_array->field(4), typed_array->field(5)},
                                                   value_schema->fields())
                              .ValueOrDie();
    auto expected = arrow::ChunkedArray::Make({expected_array}).ValueOrDie();

    std::shared_ptr<arrow::Schema> expected_sort_schema =
        arrow::schema(arrow::FieldVector({fields[2]}));
    CheckResult(src_array, target_type, target_to_src_mapping, /*key_arity=*/1, value_schema,
                expected, expected_sort_schema, /*expected_reserve_count=*/6);
}

TEST_P(KeyValueProjectionReaderTest, TestSimple) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int8()),
                                 arrow::field("k1", arrow::int16()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int64()),
                                 arrow::field("v2", arrow::float32()),
                                 arrow::field("v3", arrow::float64())};
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema(
        arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6], fields[7]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_({fields});
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 10, 20, null, 30.01],
        [0, 0, 1, 3, 11, 21, 31.0, 31.01],
        [1, 0, 2, 2, 12, 22, 32.0, null],
        [2, 0, 2, 3, 13, 23, 33.0, 32.01]
    ])")
            .ValueOrDie());

    auto target_type = std::dynamic_pointer_cast<arrow::StructType>(
        arrow::struct_({fields[7], fields[6], fields[4], fields[2]}));
    ASSERT_TRUE(target_type);
    std::vector<int32_t> target_to_src_mapping = {5, 4, 2, 0};
    std::shared_ptr<arrow::ChunkedArray> expected;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(target_type, {R"([
        [30.01, null, 10, 1],
        [31.01, 31.0, 11, 1],
        [null, 32.0, 12, 2],
        [32.01, 33.0, 13, 2]
    ])"},
                                                                         &expected);
    ASSERT_TRUE(array_status.ok());
    std::shared_ptr<arrow::Schema> expected_sort_schema = arrow::schema(target_type->fields());
    CheckResult(src_array, target_type, target_to_src_mapping, /*key_arity=*/2, value_schema,
                expected, expected_sort_schema, /*expected_reserve_count=*/5);
}

TEST_P(KeyValueProjectionReaderTest, TestTimestampType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone))};
    std::shared_ptr<arrow::Schema> value_schema = std::make_shared<arrow::Schema>(
        arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6], fields[7],
                            fields[8], fields[9]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_({fields});
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
[0, 0, "1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
[1, 0, "1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null, "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
[2, 0, "1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null, null, "1970-01-01 00:00:06", null, "1970-01-01 00:00:00.000006", null]
    ])")
            .ValueOrDie());

    auto target_type = std::dynamic_pointer_cast<arrow::StructType>(arrow::struct_(
        {fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9]}));
    ASSERT_TRUE(target_type);
    std::vector<int32_t> target_to_src_mapping = {0, 1, 2, 3, 4, 5, 6, 7};
    std::shared_ptr<arrow::ChunkedArray> expected;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(target_type, {R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
["1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null, "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
["1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null, null, "1970-01-01 00:00:06", null, "1970-01-01 00:00:00.000006", null]
    ])"},
                                                                         &expected);
    ASSERT_TRUE(array_status.ok());
    std::shared_ptr<arrow::Schema> expected_sort_schema =
        std::make_shared<arrow::Schema>(arrow::FieldVector({fields[2]}));
    CheckResult(src_array, target_type, target_to_src_mapping, /*key_arity=*/1, value_schema,
                expected, expected_sort_schema, /*expected_reserve_count=*/9);
}

TEST_P(KeyValueProjectionReaderTest, TestComplexType) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::boolean()),
                                 arrow::field("k1", arrow::utf8()),
                                 arrow::field("v0", arrow::binary()),
                                 arrow::field("v1", arrow::date32()),
                                 arrow::field("v2", arrow::timestamp(arrow::TimeUnit::NANO)),
                                 arrow::field("v3", arrow::decimal128(5, 2))};
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema(
        arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6], fields[7]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_({fields});
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, false, "apple",  null,   20, "1970-01-01 00:00:00.000000500", "123.23"],
        [0, 0, false, "banana", "中国", 21, "2025-01-01 23:59:59.000000500", "213.59"],
        [1, 0, true, "apple",   "北京", 22, "2000-01-01 00:00:00.000000500", null],
        [2, 0, true, "papaya",  "你好",   23, "1970-01-02 00:00:00.000000500", "-12.21"]
    ])")
            .ValueOrDie());

    auto target_type = std::dynamic_pointer_cast<arrow::StructType>(
        arrow::struct_({fields[2], fields[3], fields[4], fields[5], fields[6], fields[7]}));
    ASSERT_TRUE(target_type);
    std::vector<int32_t> target_to_src_mapping = {0, 1, 2, 3, 4, 5};
    std::shared_ptr<arrow::ChunkedArray> expected;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(target_type, {R"([
        [false, "apple",  null,   20, "1970-01-01 00:00:00.000000500", "123.23"],
        [false, "banana", "中国", 21, "2025-01-01 23:59:59.000000500", "213.59"],
        [true, "apple",   "北京", 22, "2000-01-01 00:00:00.000000500", null],
        [true, "papaya",  "你好",   23, "1970-01-02 00:00:00.000000500", "-12.21"]
    ])"},
                                                                         &expected);
    ASSERT_TRUE(array_status.ok());
    std::shared_ptr<arrow::Schema> expected_sort_schema = arrow::schema(target_type->fields());
    CheckResult(src_array, target_type, target_to_src_mapping, /*key_arity=*/2, value_schema,
                expected, expected_sort_schema, /*expected_reserve_count=*/9);
}

TEST_P(KeyValueProjectionReaderTest, TestNestedType) {
    arrow::FieldVector fields = {
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("key", arrow::utf8()),
        arrow::field("f0", arrow::list(arrow::int32())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("f2",
                     arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::float64()),
                                     field("sub3", arrow::boolean())}))};
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_({fields});
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, "apple",  [1, 2, 3],    [["apple", 3], ["banana", 4]],          [10, 10.1, false]],
        [0, 0, "banana", [4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], [20, 20.1, true]],
        [0, 0, "cat",    [6],          [["elephant", 7], ["fox", 8]],          [null, 30.1, true]],
        [0, 0, "dog",    [7],          [["giraffe", 9]],                       [null, 40.1, true]],
        [0, 0, "eagle",  null,         [["horse", 10], ["Panda", 11]],        [50, 50.1, null]],
        [0, 0, "fox",    [9],          null,                                   [60, 60.1, false]],
        [0, 0, "giraffe",[10, 11, 12], [["rabbit", null], ["tiger", 13]],      null]
    ])")
            .ValueOrDie());

    auto target_type = std::dynamic_pointer_cast<arrow::StructType>(
        arrow::struct_({fields[2], fields[3], fields[4], fields[5]}));
    ASSERT_TRUE(target_type);
    std::vector<int32_t> target_to_src_mapping = {0, 1, 2, 3};
    std::shared_ptr<arrow::ChunkedArray> expected;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(target_type, {R"([
        ["apple",   [1, 2, 3],    [["apple", 3], ["banana", 4]],          [10, 10.1, false]],
        ["banana",  [4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], [20, 20.1, true]],
        ["cat",     [6],          [["elephant", 7], ["fox", 8]],          [null, 30.1, true]],
        ["dog",     [7],          [["giraffe", 9]],                       [null, 40.1, true]],
        ["eagle",   null,         [["horse", 10], ["Panda", 11]],         [50, 50.1, null]],
        ["fox",     [9],          null,                                   [60, 60.1, false]],
        ["giraffe", [10, 11, 12], [["rabbit", null], ["tiger", 13]],      null]
    ])"},
                                                                         &expected);
    ASSERT_TRUE(array_status.ok());
    std::shared_ptr<arrow::Schema> expected_sort_schema =
        arrow::schema(arrow::FieldVector({fields[2]}));
    CheckResult(src_array, target_type, target_to_src_mapping, /*key_arity=*/1, value_schema,
                expected, expected_sort_schema, /*expected_reserve_count=*/13);
}

TEST_P(KeyValueProjectionReaderTest, TestNestedType2) {
    arrow::FieldVector fields = {
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("key", arrow::utf8()),
        arrow::field("f0", arrow::list(arrow::struct_(
                               {field("a", arrow::int64()), field("b", arrow::boolean())}))),
        arrow::field("f1", arrow::map(arrow::struct_({field("a", arrow::int64()),
                                                      field("b", arrow::boolean())}),
                                      arrow::boolean())),
        arrow::field("f2", arrow::struct_({field("a", arrow::list(arrow::int32())),
                                           field("b", arrow::boolean())})),
    };
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_({fields});
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, "apple",  [null, [1, true], null], [[[1, true], true]], null],
        [0, 0, "banana", [[2, false], null], null, [[1, 2], true]],
        [0, 0, "cat",    [[2, false], [3, true], [4, null]], [[[1, true], true], [[5, false], true]], [[5, 6, 7], true]]
    ])")
            .ValueOrDie());
    assert(src_array);
    auto target_type = std::dynamic_pointer_cast<arrow::StructType>(
        arrow::struct_({fields[4], fields[3], fields[5], fields[2]}));
    ASSERT_TRUE(target_type);
    std::vector<int32_t> target_to_src_mapping = {2, 1, 3, 0};
    std::shared_ptr<arrow::ChunkedArray> expected;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(target_type, {R"([
        [[[[1, true], true]], [null, [1, true], null], null, "apple"],
        [null, [[2, false], null], [[1, 2], true], "banana"],
        [[[[1, true], true], [[5, false], true]], [[2, false], [3, true], [4, null]], [[5, 6, 7], true], "cat"]
    ])"},
                                                                         &expected);
    ASSERT_TRUE(array_status.ok());
    std::shared_ptr<arrow::Schema> expected_sort_schema =
        arrow::schema(arrow::FieldVector({fields[2]}));

    CheckResult(src_array, target_type, target_to_src_mapping, /*key_arity=*/1, value_schema,
                expected, expected_sort_schema, /*expected_reserve_count=*/16);
}

TEST_P(KeyValueProjectionReaderTest, TestDictionary) {
    auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    arrow::FieldVector fields = {
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("key", arrow::utf8()),
        arrow::field("f0", dict_type),
    };
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    auto indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 0, 2, 0]").ValueOrDie();
    auto dict = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["foo", "bar", "baz"])")
                    .ValueOrDie();
    std::shared_ptr<arrow::DictionaryArray> f0_array =
        std::make_shared<arrow::DictionaryArray>(dict_type, indices, dict);
    auto key_array = arrow::ipc::internal::json::ArrayFromJSON(
                         arrow::utf8(), R"(["apple", "banana", "cat", "dog", "mouse"])")
                         .ValueOrDie();
    auto seq_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([1, 2, 3, 4, 5])")
                         .ValueOrDie();
    auto kind_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int8(), R"([0, 0, 0, 0, 0])").ValueOrDie();

    std::shared_ptr<arrow::DataType> src_type = arrow::struct_({fields});
    auto src_array = std::make_shared<arrow::StructArray>(
        src_type, /*length=*/5, arrow::ArrayVector({seq_array, kind_array, key_array, f0_array}));

    auto target_type = std::dynamic_pointer_cast<arrow::StructType>(
        arrow::struct_({arrow::field("f0", arrow::utf8()), fields[2]}));
    ASSERT_TRUE(target_type);
    std::vector<int32_t> target_to_src_mapping = {1, 0};

    auto f0_string_array = arrow::ipc::internal::json::ArrayFromJSON(
                               arrow::utf8(), R"(["bar", "baz", "foo", "baz", "foo"])")
                               .ValueOrDie();

    auto expected = std::make_shared<arrow::ChunkedArray>(std::make_shared<arrow::StructArray>(
        target_type, /*length=*/5, arrow::ArrayVector({f0_string_array, key_array})));

    std::shared_ptr<arrow::Schema> expected_sort_schema = arrow::schema(target_type->fields());
    CheckResult(src_array, target_type, target_to_src_mapping, /*key_arity=*/1, value_schema,
                expected, expected_sort_schema, /*expected_reserve_count=*/5);
}

TEST_P(KeyValueProjectionReaderTest, TestInvalidProducer) {
    // VALUE_KIND is int16 rather than int8
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int16()),
                                 arrow::field("k0", arrow::int8()),
                                 arrow::field("k1", arrow::int16()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int64()),
                                 arrow::field("v2", arrow::float32()),
                                 arrow::field("v3", arrow::float64())};
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema(
        arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6], fields[7]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_({fields});
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 10, 20, null, 30.01],
        [0, 0, 1, 3, 11, 21, 31.0, 31.01],
        [1, 0, 2, 2, 12, 22, 32.0, null],
        [2, 0, 2, 3, 13, 23, 33.0, 32.01]
    ])")
            .ValueOrDie());

    auto target_type = std::dynamic_pointer_cast<arrow::StructType>(
        arrow::struct_({fields[7], fields[6], fields[4], fields[2]}));
    ASSERT_TRUE(target_type);
    std::vector<int32_t> target_to_src_mapping = {5, 4, 2, 0};

    bool multi_thread_row_to_batch = GetParam();
    auto target_schema = arrow::schema(target_type->fields());
    auto projection_reader = GenerateProjectionReader(
        src_array, target_schema, target_to_src_mapping,
        /*key_arity=*/2, value_schema, /*batch_size=*/1, multi_thread_row_to_batch);
    ASSERT_NOK_WITH_MSG(paimon::test::ReadResultCollector::CollectResult(projection_reader.get()),
                        "cannot cast VALUE_KIND column to int8 arrow array");
}

INSTANTIATE_TEST_SUITE_P(EnableMultiThreadRowToBatch, KeyValueProjectionReaderTest,
                         ::testing::Values(false, true));

}  // namespace paimon::test
