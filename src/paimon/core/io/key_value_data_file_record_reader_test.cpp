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

#include "paimon/core/io/key_value_data_file_record_reader.h"

#include <utility>
#include <variant>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/mock/mock_key_value_data_file_record_reader.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class KeyValueDataFileRecordReaderTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(KeyValueDataFileRecordReaderTest, TestSimple) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};

    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 10, 20, 30],
        [0, 1, 1, 2, 11, 21, 31],
        [1, 2, 2, 2, 12, 22, 32],
        [2, 3, 2, 3, 13, 23, 33]
    ])")
            .ValueOrDie());

    std::vector<KeyValue> expected;
    expected.emplace_back(
        RowKind::Insert(), /*sequence_number=*/0, /*level=*/2,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({1, 1}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({1, 1, 10, 20, 30}, pool_.get()));
    expected.emplace_back(
        RowKind::UpdateBefore(), /*sequence_number=*/0, /*level=*/2,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({1, 2}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({1, 2, 11, 21, 31}, pool_.get()));
    expected.emplace_back(
        RowKind::UpdateAfter(), /*sequence_number=*/1, /*level=*/2,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({2, 2}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({2, 2, 12, 22, 32}, pool_.get()));
    expected.emplace_back(
        RowKind::Delete(), /*sequence_number=*/2, /*level=*/2,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({2, 3}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({2, 3, 13, 23, 33}, pool_.get()));

    for (auto batch_size : {1, 2, 3, 4, 100}) {
        auto file_batch_reader =
            std::make_unique<MockFileBatchReader>(src_array, src_type, /*batch_size=*/batch_size);
        auto record_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
            std::move(file_batch_reader), /*key_arity=*/2, value_schema, /*level=*/2, pool_);
        ASSERT_OK_AND_ASSIGN(
            auto results,
            (ReadResultCollector::CollectKeyValueResult<KeyValueDataFileRecordReader,
                                                        KeyValueRecordReader::Iterator>(
                record_reader.get())));
        KeyValueChecker::CheckResult(expected, results, /*key_arity=*/2, /*value_arity=*/5);
    }
}

TEST_F(KeyValueDataFileRecordReaderTest, TestValueSchemaContainsPartialKey) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};

    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 10, 20, 30],
        [0, 1, 1, 2, 11, 21, 31],
        [1, 2, 2, 2, 12, 22, 32],
        [2, 3, 2, 3, 13, 23, 33]
    ])")
            .ValueOrDie());

    std::vector<KeyValue> expected;
    expected.emplace_back(
        RowKind::Insert(), /*sequence_number=*/0, /*level=*/2,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({1, 1}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({1, 10, 20, 30}, pool_.get()));
    expected.emplace_back(
        RowKind::UpdateBefore(), /*sequence_number=*/0, /*level=*/2,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({1, 2}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({2, 11, 21, 31}, pool_.get()));
    expected.emplace_back(
        RowKind::UpdateAfter(), /*sequence_number=*/1, /*level=*/2,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({2, 2}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({2, 12, 22, 32}, pool_.get()));
    expected.emplace_back(
        RowKind::Delete(), /*sequence_number=*/2, /*level=*/2,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({2, 3}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({3, 13, 23, 33}, pool_.get()));

    for (auto batch_size : {1, 2, 3, 4, 100}) {
        auto file_batch_reader =
            std::make_unique<MockFileBatchReader>(src_array, src_type, /*batch_size=*/batch_size);
        auto record_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
            std::move(file_batch_reader), /*key_arity=*/2, value_schema, /*level=*/2, pool_);
        ASSERT_OK_AND_ASSIGN(
            auto results,
            (ReadResultCollector::CollectKeyValueResult<KeyValueDataFileRecordReader,
                                                        KeyValueRecordReader::Iterator>(
                record_reader.get())));
        KeyValueChecker::CheckResult(expected, results, /*key_arity=*/2, /*value_arity=*/4);
    }
}

TEST_F(KeyValueDataFileRecordReaderTest, TestWithSelectedBitmap) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};

    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 10, 20, 30],
        [1, 0, 1, 2, 11, 21, 31],
        [2, 1, 2, 2, 12, 22, 32],
        [3, 2, 2, 3, 13, 23, 33],
        [5, 0, 3, 3, 14, 24, 34],
        [8, 0, 3, 4, 15, 25, 35],
        [4, 1, 5, 4, 16, 26, 36],
        [6, 3, 6, 5, 17, 27, 37]
    ])")
            .ValueOrDie());

    auto get_all_key_values = [&]() {
        std::vector<KeyValue> all_values;
        all_values.emplace_back(
            RowKind::Insert(), /*sequence_number=*/0, /*level=*/2,
            /*key=*/BinaryRowGenerator::GenerateRowPtr({1, 1}, pool_.get()),
            /*value=*/BinaryRowGenerator::GenerateRowPtr({1, 1, 10, 20, 30}, pool_.get()));
        all_values.emplace_back(
            RowKind::Insert(), /*sequence_number=*/1, /*level=*/2,
            /*key=*/BinaryRowGenerator::GenerateRowPtr({1, 2}, pool_.get()),
            /*value=*/BinaryRowGenerator::GenerateRowPtr({1, 2, 11, 21, 31}, pool_.get()));
        all_values.emplace_back(
            RowKind::UpdateBefore(), /*sequence_number=*/2, /*level=*/2,
            /*key=*/BinaryRowGenerator::GenerateRowPtr({2, 2}, pool_.get()),
            /*value=*/BinaryRowGenerator::GenerateRowPtr({2, 2, 12, 22, 32}, pool_.get()));
        all_values.emplace_back(
            RowKind::UpdateAfter(), /*sequence_number=*/3, /*level=*/2,
            /*key=*/BinaryRowGenerator::GenerateRowPtr({2, 3}, pool_.get()),
            /*value=*/BinaryRowGenerator::GenerateRowPtr({2, 3, 13, 23, 33}, pool_.get()));
        all_values.emplace_back(
            RowKind::Insert(), /*sequence_number=*/5, /*level=*/2,
            /*key=*/BinaryRowGenerator::GenerateRowPtr({3, 3}, pool_.get()),
            /*value=*/BinaryRowGenerator::GenerateRowPtr({3, 3, 14, 24, 34}, pool_.get()));
        all_values.emplace_back(
            RowKind::Insert(), /*sequence_number=*/8, /*level=*/2,
            /*key=*/BinaryRowGenerator::GenerateRowPtr({3, 4}, pool_.get()),
            /*value=*/BinaryRowGenerator::GenerateRowPtr({3, 4, 15, 25, 35}, pool_.get()));
        all_values.emplace_back(
            RowKind::UpdateBefore(), /*sequence_number=*/4, /*level=*/2,
            /*key=*/BinaryRowGenerator::GenerateRowPtr({5, 4}, pool_.get()),
            /*value=*/BinaryRowGenerator::GenerateRowPtr({5, 4, 16, 26, 36}, pool_.get()));
        all_values.emplace_back(
            RowKind::Delete(), /*sequence_number=*/6, /*level=*/2,
            /*key=*/BinaryRowGenerator::GenerateRowPtr({6, 5}, pool_.get()),
            /*value=*/BinaryRowGenerator::GenerateRowPtr({6, 5, 17, 27, 37}, pool_.get()));
        return all_values;
    };

    auto check_result = [&](const std::vector<int32_t>& selected_rows) -> void {
        RoaringBitmap32 selected_bitmap = RoaringBitmap32::From(selected_rows);
        for (auto batch_size : {1, 2, 3, 4, 100}) {
            auto file_batch_reader = std::make_unique<MockFileBatchReader>(
                src_array, src_type, /*bitmap=*/selected_bitmap, /*batch_size=*/batch_size);
            auto record_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
                std::move(file_batch_reader), /*key_arity=*/2, value_schema, /*level=*/2, pool_);
            ASSERT_OK_AND_ASSIGN(
                auto results,
                (ReadResultCollector::CollectKeyValueResult<KeyValueDataFileRecordReader,
                                                            KeyValueRecordReader::Iterator>(
                    record_reader.get())));
            std::vector<KeyValue> all_values = get_all_key_values();
            std::vector<KeyValue> expected;
            for (const auto& row : selected_rows) {
                expected.push_back(std::move(all_values[row]));
            }
            KeyValueChecker::CheckResult(expected, results, /*key_arity=*/2,
                                         /*value_arity=*/5);
        }
    };
    // check result with selected bitmap
    check_result({0, 1, 2, 3, 4, 5, 6, 7});
    check_result({0, 1, 2, 6, 7});
    check_result({1, 2, 4, 6});
    check_result({0, 2, 4, 6});
    check_result({1, 3, 5, 7});
    check_result({3, 4, 5});
    check_result({3});
}

TEST_F(KeyValueDataFileRecordReaderTest, TestWithSelectedBitmapWithFilePos) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};

    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_(fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 10, 20, 30],
        [1, 0, 1, 2, 11, 21, 31],
        [2, 1, 2, 2, 12, 22, 32],
        [3, 2, 2, 3, 13, 23, 33],
        [5, 0, 3, 3, 14, 24, 34],
        [8, 0, 3, 4, 15, 25, 35],
        [4, 1, 5, 4, 16, 26, 36],
        [6, 3, 6, 5, 17, 27, 37]
    ])")
            .ValueOrDie());

    RoaringBitmap32 selected_bitmap = RoaringBitmap32::From({1, 2, 4, 6});
    auto file_batch_reader = std::make_unique<MockFileBatchReader>(
        src_array, src_type, /*bitmap=*/selected_bitmap, /*batch_size=*/4);
    file_batch_reader->EnableRandomizeBatchSize(false);
    auto record_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
        std::move(file_batch_reader), /*key_arity=*/2, value_schema, /*level=*/2, pool_);

    auto check_result = [](const std::vector<int64_t>& expected_pos_vector,
                           KeyValueRecordReader::Iterator* iter) {
        auto typed_iter = dynamic_cast<KeyValueDataFileRecordReader::Iterator*>(iter);
        ASSERT_TRUE(typed_iter);
        size_t pos_iter = 0;
        while (iter->HasNext()) {
            ASSERT_OK_AND_ASSIGN(auto kv_and_pos, typed_iter->NextWithFilePos());
            const auto& [pos, kv] = kv_and_pos;
            ASSERT_EQ(pos, expected_pos_vector[pos_iter++]);
        }
        ASSERT_EQ(pos_iter, expected_pos_vector.size());
    };

    // first read row 1, 2
    ASSERT_OK_AND_ASSIGN(auto iter, record_reader->NextBatch());
    check_result({1, 2}, iter.get());

    // second read row 4, 6
    ASSERT_OK_AND_ASSIGN(iter, record_reader->NextBatch());
    check_result({4, 6}, iter.get());

    // eof
    ASSERT_OK_AND_ASSIGN(iter, record_reader->NextBatch());
    ASSERT_FALSE(iter);
}
TEST_F(KeyValueDataFileRecordReaderTest, TestEmptyReader) {
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_({fields});
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
    ])")
            .ValueOrDie());
    auto file_batch_reader =
        std::make_unique<MockFileBatchReader>(src_array, src_type, /*batch_size=*/2);
    auto record_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
        std::move(file_batch_reader), /*key_arity=*/2, value_schema, /*level=*/2, pool_);

    ASSERT_OK_AND_ASSIGN(
        auto results, (ReadResultCollector::CollectKeyValueResult<KeyValueDataFileRecordReader,
                                                                  KeyValueRecordReader::Iterator>(
                          record_reader.get())));
    ASSERT_TRUE(results.empty());
}

TEST_F(KeyValueDataFileRecordReaderTest, TestInvalidSequenceNumerColumn) {
    // _SEQUENCE_NUMBER must be int64
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::utf8()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_({fields});
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        ["0", 0, 1, 1, 10, 20, 30]
    ])")
            .ValueOrDie());
    auto file_batch_reader =
        std::make_unique<MockFileBatchReader>(src_array, src_type, /*batch_size=*/2);
    auto record_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
        std::move(file_batch_reader), /*key_arity=*/2, value_schema, /*level=*/2, pool_);
    ASSERT_NOK_WITH_MSG((ReadResultCollector::CollectKeyValueResult<KeyValueDataFileRecordReader,
                                                                    KeyValueRecordReader::Iterator>(
                            record_reader.get())),
                        "cannot cast SEQUENCE_NUMBER column to int64 arrow array");
}

TEST_F(KeyValueDataFileRecordReaderTest, TestInvalidValueKindColumn) {
    // VALUE_KIND must in {0, 1, 2, 3}
    arrow::FieldVector fields = {arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
                                 arrow::field("_VALUE_KIND", arrow::int8()),
                                 arrow::field("k0", arrow::int32()),
                                 arrow::field("k1", arrow::int32()),
                                 arrow::field("v0", arrow::int32()),
                                 arrow::field("v1", arrow::int32()),
                                 arrow::field("v2", arrow::int32())};
    std::shared_ptr<arrow::Schema> value_schema =
        arrow::schema(arrow::FieldVector({fields[2], fields[3], fields[4], fields[5], fields[6]}));
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_({fields});
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 100, 1, 1, 10, 20, 30]
    ])")
            .ValueOrDie());
    auto file_batch_reader =
        std::make_unique<MockFileBatchReader>(src_array, src_type, /*batch_size=*/2);
    auto record_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
        std::move(file_batch_reader), /*key_arity=*/2, value_schema, /*level=*/2, pool_);
    ASSERT_NOK_WITH_MSG((ReadResultCollector::CollectKeyValueResult<KeyValueDataFileRecordReader,
                                                                    KeyValueRecordReader::Iterator>(
                            record_reader.get())),
                        "Unsupported byte value 100 for row kind.");
}
}  // namespace paimon::test
