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

#include "paimon/core/mergetree/compact/sort_merge_reader.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/concat_key_value_record_reader.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/io/merged_key_value_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/aggregate/aggregate_merge_function.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/mergetree/compact/sort_merge_reader_with_loser_tree.h"
#include "paimon/core/mergetree/compact/sort_merge_reader_with_min_heap.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/mock/mock_key_value_data_file_record_reader.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class SortMergeReaderTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    std::vector<DataField> CreateDataField(const arrow::FieldVector& arrow_fields) {
        // create DataField with fake field id
        std::vector<DataField> data_fields;
        data_fields.reserve(arrow_fields.size());
        for (const auto& field : arrow_fields) {
            data_fields.emplace_back(/*id=*/0, field);
        }
        return data_fields;
    }

    void CheckResult(const std::vector<std::shared_ptr<arrow::StructArray>>& src_array_vec,
                     const std::shared_ptr<FieldsComparator>& user_key_comparator,
                     const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
                     const std::shared_ptr<arrow::Schema>& key_schema,
                     const std::shared_ptr<arrow::Schema>& value_schema,
                     const std::vector<KeyValue>& expected, bool ignore_delete = false) const {
        CheckSortMergeResult<SortMergeReaderWithLoserTree>(src_array_vec, user_key_comparator,
                                                           user_defined_seq_comparator, key_schema,
                                                           value_schema, expected,
                                                           /*need_merge=*/true, ignore_delete);
        CheckSortMergeResult<SortMergeReaderWithMinHeap>(src_array_vec, user_key_comparator,
                                                         user_defined_seq_comparator, key_schema,
                                                         value_schema, expected,
                                                         /*need_merge=*/true, ignore_delete);
    }

 private:
    template <typename SortMergeReaderType>
    std::unique_ptr<SortMergeReader> CreateSortMergeReader(
        const std::vector<std::shared_ptr<arrow::StructArray>>& src_array_vec,
        const std::shared_ptr<FieldsComparator>& user_key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const std::shared_ptr<arrow::Schema>& key_schema,
        const std::shared_ptr<arrow::Schema>& value_schema, int32_t batch_size, bool need_merge,
        bool ignore_delete) const {
        auto mfunc = std::make_unique<DeduplicateMergeFunction>(ignore_delete);
        auto merge_function_wrapper =
            std::make_shared<ReducerMergeFunctionWrapper>(std::move(mfunc));
        if (!need_merge) {
            if constexpr (std::is_same_v<SortMergeReaderType, SortMergeReaderWithMinHeap>) {
                merge_function_wrapper = nullptr;
            } else {
                ADD_FAILURE() << "Only SortMergeReaderWithMinHeap supports no merge";
            }
        }
        std::vector<std::unique_ptr<KeyValueRecordReader>> concat_readers;
        for (const auto& src_array : src_array_vec) {
            auto file_batch_reader = std::make_unique<MockFileBatchReader>(
                src_array, src_array->type(), /*batch_size=*/batch_size);
            auto record_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
                std::move(file_batch_reader), key_schema, value_schema, /*level=*/0, pool_);
            std::vector<std::unique_ptr<KeyValueRecordReader>> readers;
            readers.push_back(std::move(record_reader));
            concat_readers.push_back(
                std::make_unique<ConcatKeyValueRecordReader>(std::move(readers)));
        }

        return std::make_unique<SortMergeReaderType>(std::move(concat_readers), user_key_comparator,
                                                     user_defined_seq_comparator,
                                                     merge_function_wrapper);
    }

    template <typename SortMergeReaderType>
    void CheckSortMergeResult(const std::vector<std::shared_ptr<arrow::StructArray>>& src_array_vec,
                              const std::shared_ptr<FieldsComparator>& user_key_comparator,
                              const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
                              const std::shared_ptr<arrow::Schema>& key_schema,
                              const std::shared_ptr<arrow::Schema>& value_schema,
                              const std::vector<KeyValue>& expected, bool need_merge,
                              bool ignore_delete = false) const {
        for (auto batch_size : {1, 2, 3, 4, 100}) {
            auto sort_merge_reader = CreateSortMergeReader<SortMergeReaderType>(
                src_array_vec, user_key_comparator, user_defined_seq_comparator, key_schema,
                value_schema, batch_size, need_merge, ignore_delete);
            ASSERT_OK_AND_ASSIGN(
                std::vector<KeyValue> results,
                (ReadResultCollector::CollectKeyValueResult<
                    SortMergeReader, SortMergeReader::Iterator>(sort_merge_reader.get())));
            KeyValueChecker::CheckResult(expected, results, key_schema->num_fields(),
                                         value_schema->num_fields());
        }
    }

    template <typename SortMergeReaderType>
    void CheckSortMergeResultForAggregate(
        const std::vector<std::shared_ptr<arrow::StructArray>>& src_array_vec,
        const std::shared_ptr<FieldsComparator>& user_key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const std::shared_ptr<arrow::Schema>& key_schema,
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::vector<std::string>& user_defined_sequence_fields,
        const std::vector<std::string>& primary_keys, const CoreOptions& core_options,
        const std::vector<KeyValue>& expected) const {
        for (auto batch_size : {1, 2, 3, 4, 100}) {
            ASSERT_OK_AND_ASSIGN(
                std::unique_ptr<AggregateMergeFunction> mfunc,
                AggregateMergeFunction::Create(value_schema, primary_keys, core_options));
            auto merge_function_wrapper =
                std::make_shared<ReducerMergeFunctionWrapper>(std::move(mfunc));
            std::vector<std::unique_ptr<KeyValueRecordReader>> merged_readers;

            std::vector<std::unique_ptr<KeyValueRecordReader>> readers;
            for (const auto& src_array : src_array_vec) {
                auto file_batch_reader = std::make_unique<MockFileBatchReader>(
                    src_array, src_array->type(), /*batch_size=*/batch_size);
                auto record_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
                    std::move(file_batch_reader), key_schema, value_schema, /*level=*/0, pool_);
                merged_readers.push_back(std::make_unique<MergedKeyValueRecordReader>(
                    std::move(record_reader), user_key_comparator, merge_function_wrapper));
            }

            auto sort_merge_reader = std::make_unique<SortMergeReaderType>(
                std::move(merged_readers), user_key_comparator, user_defined_seq_comparator,
                merge_function_wrapper);
            ASSERT_OK_AND_ASSIGN(
                std::vector<KeyValue> results,
                (ReadResultCollector::CollectKeyValueResult<
                    SortMergeReader, SortMergeReader::Iterator>(sort_merge_reader.get())));
            KeyValueChecker::CheckResult(expected, results, key_schema->num_fields(),
                                         value_schema->num_fields());
        }
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(SortMergeReaderTest, TestSimpleWithTwoSameKeys) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};

    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(arrow::FieldVector({fields[2]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);

    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 2, 10]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 3, 30]
    ])")
            .ValueOrDie());

    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 5, 50]
    ])")
            .ValueOrDie());

    auto src_array4 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 2, 30]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {1, 1, 1}, {{2}, {3}, {5}}, {{2, 30}, {3, 30}, {5, 50}}, pool_);
    CheckSortMergeResult<SortMergeReaderWithLoserTree>(
        {src_array1, src_array2, src_array3, src_array4}, user_key_comparator, nullptr, key_schema,
        value_schema, expected, /*need_merge=*/true);
}

TEST_F(SortMergeReaderTest, TestSimpleWithThreeSameKeys) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};

    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(arrow::FieldVector({fields[2]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);

    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 2, 10]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [2, 0, 2, 30]
    ])")
            .ValueOrDie());

    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 5, 50]
    ])")
            .ValueOrDie());

    auto src_array4 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 2, 30]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected =
        KeyValueChecker::GenerateKeyValues({2, 1}, {{2}, {5}}, {{2, 30}, {5, 50}}, pool_);
    CheckSortMergeResult<SortMergeReaderWithLoserTree>(
        {src_array1, src_array2, src_array3, src_array4}, user_key_comparator, nullptr, key_schema,
        value_schema, expected, /*need_merge=*/true);
}

TEST_F(SortMergeReaderTest, TestSimpleWithThreeSameKeys2) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};

    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(arrow::FieldVector({fields[2]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 2, 10]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 2, 30]
    ])")
            .ValueOrDie());

    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 5, 50]
    ])")
            .ValueOrDie());

    auto src_array4 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [2, 0, 2, 30]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected =
        KeyValueChecker::GenerateKeyValues({2, 1}, {{2}, {5}}, {{2, 30}, {5, 50}}, pool_);
    CheckSortMergeResult<SortMergeReaderWithLoserTree>(
        {src_array1, src_array2, src_array3, src_array4}, user_key_comparator, nullptr, key_schema,
        value_schema, expected, /*need_merge=*/true);
}

TEST_F(SortMergeReaderTest, TestSortMergeIn2Ways) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};

    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);

    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 10, 20, 30],
        [0, 0, 1, 3, 11, 21, 31],
        [1, 0, 2, 2, 12, 22, 32],
        [2, 0, 2, 3, 13, 23, 33]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 1, 1, 14, 24, 34],
        [0, 0, 1, 2, 15, 25, 35],
        [2, 0, 2, 2, 16, 26, 36],
        [2, 0, 2, 5, 17, 27, 37]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FieldsComparator> user_key_comparator,
        FieldsComparator::Create({data_fields[2], data_fields[3]}, std::vector<int32_t>({0, 1}),
                                 /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {1, 0, 0, 2, 2, 2}, {{1, 1}, {1, 2}, {1, 3}, {2, 2}, {2, 3}, {2, 5}},
        {{1, 1, 14, 24, 34},
         {1, 2, 15, 25, 35},
         {1, 3, 11, 21, 31},
         {2, 2, 16, 26, 36},
         {2, 3, 13, 23, 33},
         {2, 5, 17, 27, 37}},
        pool_);
    CheckResult({src_array1, src_array2}, user_key_comparator,
                /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, expected);
}

TEST_F(SortMergeReaderTest, TestSortMergeIn3Ways) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};
    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(arrow::FieldVector({fields[2]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);

    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 10],
        [0, 0, 2, 11],
        [1, 0, 3, 12],
        [2, 0, 4, 13]
    ])")
            .ValueOrDie());
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 2, 14],
        [2, 0, 3, 15],
        [3, 0, 4, 16],
        [2, 0, 5, 17]
    ])")
            .ValueOrDie());
    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 1, 17],
        [2, 0, 2, 18],
        [4, 0, 4, 19],
        [4, 0, 5, 20]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected =
        KeyValueChecker::GenerateKeyValues({1, 2, 2, 4, 4}, {{1}, {2}, {3}, {4}, {5}},
                                           {{1, 17}, {2, 18}, {3, 15}, {4, 19}, {5, 20}}, pool_);

    CheckResult({src_array1, src_array2, src_array3}, user_key_comparator,
                /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, expected);
}

TEST_F(SortMergeReaderTest, TestSortMergeWithDeleteMessages) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};
    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(arrow::FieldVector({fields[2]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);

    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 3, 1, 10],
        [2, 3, 2, 200],
        [1, 0, 3, 300]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [3, 3, 1, 11],
        [4, 3, 2, 240],
        [5, 3, 3, 30]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));

    std::vector<RowKind*> delete_row_kinds = {const_cast<RowKind*>(RowKind::Delete()),
                                              const_cast<RowKind*>(RowKind::Delete()),
                                              const_cast<RowKind*>(RowKind::Delete())};
    std::vector<KeyValue> expected_delete = KeyValueChecker::GenerateKeyValues(
        delete_row_kinds, /*seq_vec=*/{3, 4, 5}, /*level_vec=*/{0, 0, 0},
        /*key_vec=*/{{1}, {2}, {3}}, /*value_vec=*/{{1, 11}, {2, 240}, {3, 30}}, pool_);
    CheckResult({src_array1, src_array2}, user_key_comparator,
                /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, expected_delete,
                /*ignore_delete=*/false);

    std::vector<KeyValue> expected_ignore_delete = KeyValueChecker::GenerateKeyValues(
        /*seq_vec=*/{1}, /*key_vec=*/{{3}}, /*value_vec=*/{{3, 300}}, pool_);
    CheckResult({src_array1, src_array2}, user_key_comparator,
                /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema,
                expected_ignore_delete, /*ignore_delete=*/true);
}

TEST_F(SortMergeReaderTest, TestSortMergeIn2WaysWithEmptyArray) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};
    auto data_fields = CreateDataField(fields);

    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(arrow::FieldVector({fields[2]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);

    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 10],
        [0, 0, 2, 11],
        [1, 0, 3, 12],
        [2, 0, 4, 13]
    ])")
            .ValueOrDie());
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {0, 0, 1, 2}, {{1}, {2}, {3}, {4}}, {{1, 10}, {2, 11}, {3, 12}, {4, 13}}, pool_);

    CheckResult({src_array1, src_array2}, user_key_comparator,
                /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, expected);
}

TEST_F(SortMergeReaderTest, TestSortMergeIn2WaysWithNoOverlap) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};

    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(arrow::FieldVector({fields[2]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);

    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 10],
        [0, 0, 2, 11],
        [1, 0, 3, 12],
        [2, 0, 4, 13]
    ])")
            .ValueOrDie());
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 102, 14],
        [2, 0, 103, 15],
        [3, 0, 104, 16],
        [2, 0, 105, 17]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {0, 0, 1, 2, 1, 2, 3, 2}, {{1}, {2}, {3}, {4}, {102}, {103}, {104}, {105}},
        {{1, 10}, {2, 11}, {3, 12}, {4, 13}, {102, 14}, {103, 15}, {104, 16}, {105, 17}}, pool_);

    CheckResult({src_array1, src_array2}, user_key_comparator,
                /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, expected);
}

TEST_F(SortMergeReaderTest, TestSortMergeIn2WaysWithFullOverlap) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};

    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(arrow::FieldVector({fields[2]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 10],
        [0, 0, 2, 11],
        [1, 0, 3, 12],
        [2, 0, 4, 13]
    ])")
            .ValueOrDie());
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 1, 14],
        [2, 0, 2, 15],
        [3, 0, 3, 16],
        [3, 0, 4, 17]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {1, 2, 3, 3}, {{1}, {2}, {3}, {4}}, {{1, 14}, {2, 15}, {3, 16}, {4, 17}}, pool_);

    CheckResult({src_array1, src_array2}, user_key_comparator,
                /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, expected);
}

TEST_F(SortMergeReaderTest, TestSortMergeIn2WaysWithPartialOverlap) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};

    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(arrow::FieldVector({fields[2]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);

    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 10],
        [0, 0, 2, 11],
        [1, 0, 3, 12],
        [2, 0, 4, 13]
    ])")
            .ValueOrDie());
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 1, 14],
        [2, 0, 2, 15],
        [3, 0, 3, 16],
        [3, 0, 4, 17],
        [0, 0, 5, 18],
        [0, 0, 6, 19]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {1, 2, 3, 3, 0, 0}, {{1}, {2}, {3}, {4}, {5}, {6}},
        {{1, 14}, {2, 15}, {3, 16}, {4, 17}, {5, 18}, {6, 19}}, pool_);

    CheckResult({src_array1, src_array2}, user_key_comparator,
                /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, expected);
}

TEST_F(SortMergeReaderTest, TestSortMergeIn3WaysWithUserDefinedSeq) {
    // key: k0, k1
    // user defined sequence field: v0, v1
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};

    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [3, 0, 1, 1, 10, 20, 30],
        [0, 0, 1, 3, 11, 21, 31],
        [3, 0, 2, 2, 12, 22, 32],
        [2, 0, 2, 3, 13, 23, 33]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 1, 1, 14, 24, 34],
        [1, 0, 1, 2, 15, 25, 35],
        [2, 0, 2, 2, 18, 28, 38],
        [2, 0, 2, 5, 17, 27, 37]
    ])")
            .ValueOrDie());

    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [2, 0, 1, 1, 14, 24, 34],
        [0, 0, 1, 2, 15, 25, 35],
        [5, 0, 2, 2, 16, 26, 36],
        [3, 0, 2, 5, 17, 28, 37]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FieldsComparator> user_key_comparator,
        FieldsComparator::Create({data_fields[2], data_fields[3]}, std::vector<int32_t>({0, 1}),
                                 /*is_ascending_order=*/true));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_defined_seq_comparator,
                         FieldsComparator::Create({data_fields[2], data_fields[3], data_fields[4],
                                                   data_fields[5], data_fields[6]},
                                                  std::vector<int32_t>({2, 3}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {2, 1, 0, 2, 2, 3}, {{1, 1}, {1, 2}, {1, 3}, {2, 2}, {2, 3}, {2, 5}},
        {{1, 1, 14, 24, 34},
         {1, 2, 15, 25, 35},
         {1, 3, 11, 21, 31},
         {2, 2, 18, 28, 38},
         {2, 3, 13, 23, 33},
         {2, 5, 17, 28, 37}},
        pool_);
    CheckResult({src_array1, src_array2, src_array3}, user_key_comparator,
                user_defined_seq_comparator, key_schema, value_schema, expected);
}

TEST_F(SortMergeReaderTest, TestSortMergeWithAggMergeFunction) {
    // key: k0, user defined sequence field: ts, value: v0
    // Format: [_SEQUENCE_NUMBER, _VALUE_KIND, k0, ts, v0]
    // Using sum aggregation: k0 uses primary-key agg, ts use last_value agg and v0 use sum agg.
    //
    // Reader1 (SEQUENCE_NUMBER 0..5):
    //   [key=1,ts=1,v=1], [key=1,ts=2,v=2], [key=1,ts=3,v=3]
    //   [key=1,ts=4,v=4], [key=2,ts=4,v=40], [key=2,ts=5,v=50]
    // Reader2 (SEQUENCE_NUMBER 6..11):
    //   [key=1,ts=5,v=5], [key=1,ts=6,v=6], [key=2,ts=1,v=10]
    //   [key=2,ts=2,v=20], [key=2,ts=3,v=30], [key=2,ts=6,v=60]
    //
    // With user_defined_seq_comparator on ts field, sort by key asc, then ts asc within same key:
    // key=1: ts=1(v=1), ts=2(v=2), ts=3(v=3), ts=4(v=4), ts=5(v=5), ts=6(v=6)
    // key=2: ts=1(v=10), ts=2(v=20), ts=3(v=30), ts=4(v=40), ts=5(v=50), ts=6(v=60)
    //
    // After sum aggregation:
    // key=1: k0=1, ts=last_value(1,2,3,4,5,6)=6, v0=sum(1,2,3,4,5,6)=21, seq=7
    // key=2: k0=2, ts=last_value(1,2,3,4,5,6)=6, v0=sum(10,20,30,40,50,60)=210, seq=11

    arrow::FieldVector fields = {
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
        arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("k0", arrow::int32()),
        arrow::field("ts", arrow::int32()), arrow::field("v0", arrow::int32())};

    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(arrow::FieldVector({fields[2]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);

    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 1],
        [1, 0, 1, 2, 2],
        [2, 0, 1, 3, 3],
        [3, 0, 1, 4, 4],
        [4, 0, 2, 4, 40],
        [5, 0, 2, 5, 50]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [6, 0, 1, 5, 5],
        [7, 0, 1, 6, 6],
        [8, 0, 2, 1, 10],
        [9, 0, 2, 2, 20],
        [10, 0, 2, 3, 30],
        [11, 0, 2, 6, 60]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    // user_defined_seq_comparator based on ts field (index 1 in value schema {k0, ts, v0})
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_defined_seq_comparator,
                         FieldsComparator::Create({data_fields[2], data_fields[3], data_fields[4]},
                                                  std::vector<int32_t>({1}),
                                                  /*is_ascending_order=*/true));
    // Configure sum aggregation for all non-primary-key fields
    std::string user_defined_sequence_field = "ts";
    ASSERT_OK_AND_ASSIGN(
        CoreOptions core_options,
        CoreOptions::FromMap({{Options::FIELDS_DEFAULT_AGG_FUNC, "sum"},
                              {Options::SEQUENCE_FIELD, user_defined_sequence_field}}));

    // After sum aggregation, same-key rows are merged:
    // key=1: seq=7, k0=1, ts=6, v0=21
    // key=2: seq=11, k0=2, ts=6, v0=210
    std::vector<KeyValue> expected =
        KeyValueChecker::GenerateKeyValues({7, 11}, {{1}, {2}}, {{1, 6, 21}, {2, 6, 210}}, pool_);
    for (auto& kv : expected) {
        kv.level = KeyValue::UNKNOWN_LEVEL;
    }
    CheckSortMergeResultForAggregate<SortMergeReaderWithLoserTree>(
        {src_array1, src_array2}, user_key_comparator, user_defined_seq_comparator, key_schema,
        value_schema, {user_defined_sequence_field}, {"k0"}, core_options, expected);
    CheckSortMergeResultForAggregate<SortMergeReaderWithMinHeap>(
        {src_array1, src_array2}, user_key_comparator, user_defined_seq_comparator, key_schema,
        value_schema, {user_defined_sequence_field}, {"k0"}, core_options, expected);
}

TEST_F(SortMergeReaderTest, TestRawSortNoMergeKeepsDuplicateKeys) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};

    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(arrow::FieldVector({fields[2]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);

    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 2, 10]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [2, 0, 2, 30]
    ])")
            .ValueOrDie());

    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 5, 50]
    ])")
            .ValueOrDie());

    auto src_array4 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 2, 30]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {0, 1, 2, 1}, {{2}, {2}, {2}, {5}}, {{2, 10}, {2, 30}, {2, 30}, {5, 50}}, pool_);
    CheckSortMergeResult<SortMergeReaderWithMinHeap>(
        {src_array1, src_array2, src_array3, src_array4}, user_key_comparator,
        /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, expected,
        /*need_merge=*/false);
}

TEST_F(SortMergeReaderTest, TestRawSortNoMergeWithMinHeap) {
    // key: k0, user defined sequence field: ts, value: v0
    // Format: [_SEQUENCE_NUMBER, _VALUE_KIND, k0, ts, v0]
    // Reader1 (SEQUENCE_NUMBER 0..5):
    //   [key=1,ts=1,v=1], [key=1,ts=2,v=2], [key=1,ts=3,v=3]
    //   [key=1,ts=4,v=4], [key=2,ts=4,v=40], [key=2,ts=5,v=50]
    // Reader2 (SEQUENCE_NUMBER 6..11):
    //   [key=1,ts=5,v=5], [key=1,ts=6,v=6], [key=2,ts=1,v=10]
    //   [key=2,ts=2,v=20], [key=2,ts=3,v=30], [key=2,ts=6,v=60]
    //
    // After sort:
    // With user_defined_seq_comparator on ts field, sort by key asc, then ts asc within same key
    // key=1: ts=1(seq0,v=1), ts=2(seq1,v=2), ts=3(seq2,v=3), ts=4(seq3,v=4),
    //        ts=5(seq6,v=5), ts=6(seq7,v=6)
    // key=2: ts=1(seq8,v=10), ts=2(seq9,v=20), ts=3(seq10,v=30), ts=4(seq4,v=40),
    //        ts=5(seq5,v=50), ts=6(seq11,v=60)

    arrow::FieldVector fields = {
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
        arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("k0", arrow::int32()),
        arrow::field("ts", arrow::int32()), arrow::field("v0", arrow::int32())};

    auto data_fields = CreateDataField(fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(arrow::FieldVector({fields[2]}));
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);

    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 1],
        [1, 0, 1, 2, 2],
        [2, 0, 1, 3, 3],
        [3, 0, 1, 4, 4],
        [4, 0, 2, 4, 40],
        [5, 0, 2, 5, 50]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [6, 0, 1, 5, 5],
        [7, 0, 1, 6, 6],
        [8, 0, 2, 1, 10],
        [9, 0, 2, 2, 20],
        [10, 0, 2, 3, 30],
        [11, 0, 2, 6, 60]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    // user_defined_seq_comparator based on ts field (index 1 in value schema {k0, ts, v0})
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_defined_seq_comparator,
                         FieldsComparator::Create({data_fields[2], data_fields[3], data_fields[4]},
                                                  std::vector<int32_t>({1}),
                                                  /*is_ascending_order=*/true));

    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {0, 1, 2, 3, 6, 7, 8, 9, 10, 4, 5, 11},
        {{1}, {1}, {1}, {1}, {1}, {1}, {2}, {2}, {2}, {2}, {2}, {2}},
        {{1, 1, 1},
         {1, 2, 2},
         {1, 3, 3},
         {1, 4, 4},
         {1, 5, 5},
         {1, 6, 6},
         {2, 1, 10},
         {2, 2, 20},
         {2, 3, 30},
         {2, 4, 40},
         {2, 5, 50},
         {2, 6, 60}},
        pool_);
    CheckSortMergeResult<SortMergeReaderWithMinHeap>({src_array1, src_array2}, user_key_comparator,
                                                     user_defined_seq_comparator, key_schema,
                                                     value_schema, expected, /*need_merge=*/false);
}

}  // namespace paimon::test
