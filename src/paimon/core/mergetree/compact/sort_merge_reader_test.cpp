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
#include "paimon/core/io/concat_key_value_record_reader.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/raw_sort_merge_reader_with_min_heap.h"
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

    static std::string FormatKeyValue(const KeyValue& kv) {
        const auto format_row = [](const InternalRow& row) {
            std::string result = "[";
            for (int32_t i = 0; i < row.GetFieldCount(); ++i) {
                if (i > 0) {
                    result += ", ";
                }
                result += row.IsNullAt(i) ? "null" : std::to_string(row.GetInt(i));
            }
            result += "]";
            return result;
        };

        return "{seq=" + std::to_string(kv.sequence_number) + ", key=" + format_row(*kv.key) +
               ", value=" + format_row(*kv.value) + "}";
    }

    static std::string FormatKeyValueBatch(const std::vector<KeyValue>& batch) {
        std::string result = "[";
        for (size_t i = 0; i < batch.size(); ++i) {
            if (i > 0) {
                result += ", ";
            }
            result += FormatKeyValue(batch[i]);
        }
        result += "]";
        return result;
    }

    static std::string FormatKeyValueBatches(const std::vector<std::vector<KeyValue>>& batches) {
        std::string result = "[";
        for (size_t i = 0; i < batches.size(); ++i) {
            if (i > 0) {
                result += ", ";
            }
            result += FormatKeyValueBatch(batches[i]);
        }
        result += "]";
        return result;
    }

    void CheckResult(const std::vector<std::shared_ptr<arrow::StructArray>>& src_array_vec,
                     const std::shared_ptr<FieldsComparator>& user_key_comparator,
                     const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
                     const std::shared_ptr<arrow::Schema>& key_schema,
                     const std::shared_ptr<arrow::Schema>& value_schema,
                     const std::vector<KeyValue>& expected) const {
        CheckSortMergeResult<SortMergeReaderWithLoserTree>(src_array_vec, user_key_comparator,
                                                           user_defined_seq_comparator, key_schema,
                                                           value_schema, expected);
        CheckSortMergeResult<SortMergeReaderWithMinHeap>(src_array_vec, user_key_comparator,
                                                         user_defined_seq_comparator, key_schema,
                                                         value_schema, expected);
    }

 private:
    template <typename SortMergeReaderType>
    std::unique_ptr<SortMergeReader> CreateSortMergeReader(
        const std::vector<std::shared_ptr<arrow::StructArray>>& src_array_vec,
        const std::shared_ptr<FieldsComparator>& user_key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const std::shared_ptr<arrow::Schema>& key_schema,
        const std::shared_ptr<arrow::Schema>& value_schema, int32_t batch_size) const {
        auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        auto merge_function_wrapper =
            std::make_shared<ReducerMergeFunctionWrapper>(std::move(mfunc));
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

    std::unique_ptr<SortMergeReader> CreateRawSortMergeReader(
        const std::vector<std::shared_ptr<arrow::StructArray>>& src_array_vec,
        const std::shared_ptr<FieldsComparator>& user_key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const std::shared_ptr<arrow::Schema>& key_schema,
        const std::shared_ptr<arrow::Schema>& value_schema, int32_t batch_size) const {
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

        return std::make_unique<RawSortMergeReaderWithMinHeap>(
            std::move(concat_readers), user_key_comparator, user_defined_seq_comparator);
    }

    std::unique_ptr<SortMergeReader> CreateRawSortMergeReaderWithMinHeap(
        const std::vector<std::shared_ptr<arrow::StructArray>>& src_array_vec,
        const std::shared_ptr<FieldsComparator>& user_key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const std::shared_ptr<arrow::Schema>& key_schema,
        const std::shared_ptr<arrow::Schema>& value_schema, int32_t batch_size) const {
        std::vector<std::unique_ptr<KeyValueRecordReader>> concat_readers;
        for (const auto& src_array : src_array_vec) {
            auto file_batch_reader = std::make_unique<MockFileBatchReader>(
                src_array, src_array->type(), /*batch_size=*/batch_size);
            file_batch_reader->EnableRandomizeBatchSize(false);
            auto record_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
                std::move(file_batch_reader), key_schema, value_schema, /*level=*/0, pool_);
            std::vector<std::unique_ptr<KeyValueRecordReader>> readers;
            readers.push_back(std::move(record_reader));
            concat_readers.push_back(
                std::make_unique<ConcatKeyValueRecordReader>(std::move(readers)));
        }

        return std::make_unique<RawSortMergeReaderWithMinHeap>(
            std::move(concat_readers), user_key_comparator, user_defined_seq_comparator);
    }

    template <typename SortMergeReaderType>
    void CheckSortMergeResult(const std::vector<std::shared_ptr<arrow::StructArray>>& src_array_vec,
                              const std::shared_ptr<FieldsComparator>& user_key_comparator,
                              const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
                              const std::shared_ptr<arrow::Schema>& key_schema,
                              const std::shared_ptr<arrow::Schema>& value_schema,
                              const std::vector<KeyValue>& expected) const {
        for (auto batch_size : {1, 2, 3, 4, 100}) {
            auto sort_merge_reader = CreateSortMergeReader<SortMergeReaderType>(
                src_array_vec, user_key_comparator, user_defined_seq_comparator, key_schema,
                value_schema, batch_size);
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
        value_schema, expected);
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
        value_schema, expected);
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
        value_schema, expected);
}

TEST_F(SortMergeReaderTest, TestRawSortMergeKeepsDuplicateKeys) {
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

    for (auto batch_size : {1, 2, 3, 4, 100}) {
        auto sort_merge_reader = CreateRawSortMergeReader(
            {src_array1, src_array2, src_array3, src_array4}, user_key_comparator,
            /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, batch_size);
        ASSERT_OK_AND_ASSIGN(
            std::vector<KeyValue> results,
            (ReadResultCollector::CollectKeyValueResult<SortMergeReader, SortMergeReader::Iterator>(
                sort_merge_reader.get())));
        KeyValueChecker::CheckResult(expected, results, key_schema->num_fields(),
                                     value_schema->num_fields());
    }
}

TEST_F(SortMergeReaderTest, TestRawSortMergeKeepsDuplicateKeysWithinEachReader) {
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
        [0, 0, 2, 10],
        [2, 0, 2, 12],
        [6, 0, 6, 60]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 3, 30],
        [3, 0, 3, 32],
        [7, 0, 7, 70]
    ])")
            .ValueOrDie());

    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [4, 0, 4, 40],
        [5, 0, 4, 41]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {0, 2, 1, 3, 4, 5, 6, 7}, {{2}, {2}, {3}, {3}, {4}, {4}, {6}, {7}},
        {{2, 10}, {2, 12}, {3, 30}, {3, 32}, {4, 40}, {4, 41}, {6, 60}, {7, 70}}, pool_);

    for (auto batch_size : {1, 2, 3, 4, 100}) {
        auto sort_merge_reader = CreateRawSortMergeReader(
            {src_array1, src_array2, src_array3}, user_key_comparator,
            /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, batch_size);
        ASSERT_OK_AND_ASSIGN(
            std::vector<KeyValue> results,
            (ReadResultCollector::CollectKeyValueResult<SortMergeReader, SortMergeReader::Iterator>(
                sort_merge_reader.get())));
        KeyValueChecker::CheckResult(expected, results, key_schema->num_fields(),
                                     value_schema->num_fields());
    }
}

TEST_F(SortMergeReaderTest, TestRawAndMergedSortMergeMatchForDuplicateKeysWithinSingleReader) {
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
        [0, 0, 2, 10],
        [2, 0, 2, 12],
        [3, 0, 3, 30]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {0, 2, 3}, {{2}, {2}, {3}}, {{2, 10}, {2, 12}, {3, 30}}, pool_);

    for (auto batch_size : {1, 2, 3, 4, 100}) {
        auto raw_sort_merge_reader = CreateRawSortMergeReader(
            {src_array1}, user_key_comparator,
            /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, batch_size);
        ASSERT_OK_AND_ASSIGN(
            std::vector<KeyValue> raw_results,
            (ReadResultCollector::CollectKeyValueResult<SortMergeReader, SortMergeReader::Iterator>(
                raw_sort_merge_reader.get())));
        KeyValueChecker::CheckResult(expected, raw_results, key_schema->num_fields(),
                                     value_schema->num_fields());

        auto sort_merge_reader = CreateSortMergeReader<SortMergeReaderWithLoserTree>(
            {src_array1}, user_key_comparator,
            /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, batch_size);
        ASSERT_OK_AND_ASSIGN(
            std::vector<KeyValue> merged_results,
            (ReadResultCollector::CollectKeyValueResult<SortMergeReader, SortMergeReader::Iterator>(
                sort_merge_reader.get())));
        KeyValueChecker::CheckResult(expected, merged_results, key_schema->num_fields(),
                                     value_schema->num_fields());
    }
}

TEST_F(SortMergeReaderTest, TestRawAndMergedSortMergeDifferForDuplicateKeysAcrossReaders) {
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
        [0, 0, 2, 10],
        [4, 0, 4, 40]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 2, 12],
        [3, 0, 3, 30]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> raw_expected = KeyValueChecker::GenerateKeyValues(
        {0, 1, 3, 4}, {{2}, {2}, {3}, {4}}, {{2, 10}, {2, 12}, {3, 30}, {4, 40}}, pool_);
    std::vector<KeyValue> merged_expected = KeyValueChecker::GenerateKeyValues(
        {1, 3, 4}, {{2}, {3}, {4}}, {{2, 12}, {3, 30}, {4, 40}}, pool_);

    for (auto batch_size : {1, 2, 3, 4, 100}) {
        auto raw_sort_merge_reader = CreateRawSortMergeReader(
            {src_array1, src_array2}, user_key_comparator,
            /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, batch_size);
        ASSERT_OK_AND_ASSIGN(
            std::vector<KeyValue> raw_results,
            (ReadResultCollector::CollectKeyValueResult<SortMergeReader, SortMergeReader::Iterator>(
                raw_sort_merge_reader.get())));
        KeyValueChecker::CheckResult(raw_expected, raw_results, key_schema->num_fields(),
                                     value_schema->num_fields());

        auto sort_merge_reader = CreateSortMergeReader<SortMergeReaderWithLoserTree>(
            {src_array1, src_array2}, user_key_comparator,
            /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, batch_size);
        ASSERT_OK_AND_ASSIGN(
            std::vector<KeyValue> merged_results,
            (ReadResultCollector::CollectKeyValueResult<SortMergeReader, SortMergeReader::Iterator>(
                sort_merge_reader.get())));
        KeyValueChecker::CheckResult(merged_expected, merged_results, key_schema->num_fields(),
                                     value_schema->num_fields());
    }
}

TEST_F(SortMergeReaderTest, TestSortMergeMergesDuplicateKeysAcrossReadersRoundByRound) {
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
        [0, 0, 2, 10],
        [2, 0, 2, 20],
        [4, 0, 3, 30]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 2, 11],
        [3, 0, 2, 21],
        [5, 0, 4, 40]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {1, 3, 4, 5}, {{2}, {2}, {3}, {4}}, {{2, 11}, {2, 21}, {3, 30}, {4, 40}}, pool_);

    CheckSortMergeResult<SortMergeReaderWithLoserTree>(
        {src_array1, src_array2}, user_key_comparator,
        /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, expected);
}

TEST_F(SortMergeReaderTest, TestRawSortMergeReadsDuplicateKeysAcrossReadersRoundByRound) {
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
        [0, 0, 2, 10],
        [1, 0, 2, 11],
        [4, 0, 3, 30]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [2, 0, 2, 20],
        [3, 0, 2, 21],
        [5, 0, 4, 40]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {0, 1, 2, 3, 4, 5}, {{2}, {2}, {2}, {2}, {3}, {4}},
        {{2, 10}, {2, 11}, {2, 20}, {2, 21}, {3, 30}, {4, 40}}, pool_);

    for (auto batch_size : {1, 2, 3, 4, 100}) {
        auto sort_merge_reader = CreateRawSortMergeReader(
            {src_array1, src_array2}, user_key_comparator,
            /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, batch_size);
        ASSERT_OK_AND_ASSIGN(
            std::vector<KeyValue> results,
            (ReadResultCollector::CollectKeyValueResult<SortMergeReader, SortMergeReader::Iterator>(
                sort_merge_reader.get())));
        KeyValueChecker::CheckResult(expected, results, key_schema->num_fields(),
                                     value_schema->num_fields());
    }
}

TEST_F(SortMergeReaderTest, TestRawSortMergeWithMinHeapReadsDuplicateKeysAcrossReadersInOrder) {
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
        [0, 0, 2, 10],
        [1, 0, 2, 11],
        [4, 0, 3, 30]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [2, 0, 2, 20],
        [3, 0, 2, 21],
        [5, 0, 4, 40]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        {0, 1, 2, 3, 4, 5}, {{2}, {2}, {2}, {2}, {3}, {4}},
        {{2, 10}, {2, 11}, {2, 20}, {2, 21}, {3, 30}, {4, 40}}, pool_);

    for (auto batch_size : {1, 2, 3, 4, 100}) {
        auto sort_merge_reader = CreateRawSortMergeReaderWithMinHeap(
            {src_array1, src_array2}, user_key_comparator,
            /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema, batch_size);
        ASSERT_OK_AND_ASSIGN(
            std::vector<KeyValue> results,
            (ReadResultCollector::CollectKeyValueResult<SortMergeReader, SortMergeReader::Iterator>(
                sort_merge_reader.get())));
        KeyValueChecker::CheckResult(expected, results, key_schema->num_fields(),
                                     value_schema->num_fields());
    }
}

TEST_F(SortMergeReaderTest, TestRawSortMergeWithMinHeapKeepsGlobalOrderAcrossReaderBatches) {
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
        [0, 0, 0, 0],
        [2, 0, 2, 2],
        [3, 0, 3, 3],
        [4, 0, 4, 4],
        [5, 0, 5, 5],
        [9, 0, 9, 9]
    ])")
            .ValueOrDie());

    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 0, 1, 1],
        [6, 0, 6, 6],
        [7, 0, 7, 7],
        [8, 0, 8, 8],
        [10, 0, 10, 10],
        [11, 0, 11, 11]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> user_key_comparator,
                         FieldsComparator::Create({data_fields[2]}, std::vector<int32_t>({0}),
                                                  /*is_ascending_order=*/true));

    auto sort_merge_reader = CreateRawSortMergeReaderWithMinHeap(
        {src_array1, src_array2}, user_key_comparator,
        /*user_defined_seq_comparator=*/nullptr, key_schema, value_schema,
        /*batch_size=*/100);

    std::vector<std::vector<KeyValue>> actual_batches;
    while (true) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<SortMergeReader::Iterator> iterator,
                             sort_merge_reader->NextBatch());
        if (!iterator) {
            break;
        }

        std::vector<KeyValue> batch_results;
        while (true) {
            ASSERT_OK_AND_ASSIGN(bool has_next, iterator->HasNext());
            if (!has_next) {
                break;
            }
            batch_results.emplace_back(iterator->Next());
        }
        actual_batches.push_back(std::move(batch_results));
    }

    std::vector<std::vector<KeyValue>> expected_batches;
    expected_batches.push_back(KeyValueChecker::GenerateKeyValues({0, 1, 2}, {{0}, {1}, {2}},
                                                                  {{0, 0}, {1, 1}, {2, 2}}, pool_));
    expected_batches.push_back(KeyValueChecker::GenerateKeyValues({3, 4, 5}, {{3}, {4}, {5}},
                                                                  {{3, 3}, {4, 4}, {5, 5}}, pool_));
    expected_batches.push_back(KeyValueChecker::GenerateKeyValues({6, 7, 8}, {{6}, {7}, {8}},
                                                                  {{6, 6}, {7, 7}, {8, 8}}, pool_));
    expected_batches.push_back(KeyValueChecker::GenerateKeyValues(
        {9, 10, 11}, {{9}, {10}, {11}}, {{9, 9}, {10, 10}, {11, 11}}, pool_));

    const std::string expected_batches_str = FormatKeyValueBatches(expected_batches);
    const std::string actual_batches_str = FormatKeyValueBatches(actual_batches);

    std::cout << "expected_batches=" << expected_batches_str << std::endl;
    std::cout << "actual_batches=" << actual_batches_str << std::endl;

    ASSERT_EQ(expected_batches.size(), actual_batches.size())
        << "\nexpected_batches=" << expected_batches_str
        << "\nactual_batches=" << actual_batches_str;
    std::vector<KeyValue> flattened_results;
    for (size_t i = 0; i < actual_batches.size(); ++i) {
        SCOPED_TRACE("expected_batch=" + FormatKeyValueBatch(expected_batches[i]) +
                     " actual_batch=" + FormatKeyValueBatch(actual_batches[i]));
        KeyValueChecker::CheckResult(expected_batches[i], actual_batches[i],
                                     key_schema->num_fields(), value_schema->num_fields());
        for (auto& kv : actual_batches[i]) {
            flattened_results.push_back(std::move(kv));
        }
    }

    std::vector<KeyValue> expected_all = KeyValueChecker::GenerateKeyValues(
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
        {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}},
        {{0, 0},
         {1, 1},
         {2, 2},
         {3, 3},
         {4, 4},
         {5, 5},
         {6, 6},
         {7, 7},
         {8, 8},
         {9, 9},
         {10, 10},
         {11, 11}},
        pool_);
    KeyValueChecker::CheckResult(expected_all, flattened_results, key_schema->num_fields(),
                                 value_schema->num_fields());
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

}  // namespace paimon::test
