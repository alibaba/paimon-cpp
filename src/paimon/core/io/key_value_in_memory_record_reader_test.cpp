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

#include "paimon/core/io/key_value_in_memory_record_reader.h"

#include <map>
#include <utility>
#include <variant>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/utils/fields_comparator.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class KeyValueInMemoryRecordReaderTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        merge_function_wrapper_ = std::make_shared<ReducerMergeFunctionWrapper>(std::move(mfunc));
    }

    void TearDown() override {
        pool_.reset();
        merge_function_wrapper_.reset();
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<ReducerMergeFunctionWrapper> merge_function_wrapper_;
};

TEST_F(KeyValueInMemoryRecordReaderTest, TestSimple) {
    std::vector<DataField> fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                     DataField(1, arrow::field("k1", arrow::int32())),
                                     DataField(2, arrow::field("v0", arrow::int32())),
                                     DataField(3, arrow::field("v1", arrow::int32())),
                                     DataField(4, arrow::field("v2", arrow::int32()))};

    std::shared_ptr<arrow::DataType> src_type =
        DataField::ConvertDataFieldsToArrowStructType(fields);

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 2, 11, 21, 31],
        [1, 1, 10, 20, 30],
        [2, 3, 13, 23, 33],
        [2, 2, 12, 22, 32]
    ])")
            .ValueOrDie());

    std::vector<KeyValue> expected;
    expected.emplace_back(
        RowKind::UpdateBefore(), /*sequence_number=*/1, /*level=*/KeyValue::UNKNOWN_LEVEL,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({1, 1}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({1, 1, 10, 20, 30}, pool_.get()));
    expected.emplace_back(
        RowKind::Insert(), /*sequence_number=*/0, /*level=*/KeyValue::UNKNOWN_LEVEL,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({1, 2}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({1, 2, 11, 21, 31}, pool_.get()));
    expected.emplace_back(
        RowKind::Delete(), /*sequence_number=*/3, /*level=*/KeyValue::UNKNOWN_LEVEL,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({2, 2}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({2, 2, 12, 22, 32}, pool_.get()));
    expected.emplace_back(
        RowKind::UpdateAfter(), /*sequence_number=*/2, /*level=*/KeyValue::UNKNOWN_LEVEL,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({2, 3}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({2, 3, 13, 23, 33}, pool_.get()));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> key_comparator,
                         FieldsComparator::Create({fields[0], fields[1]},
                                                  /*is_ascending_order=*/true,
                                                  /*use_view=*/true));

    auto record_reader = std::make_unique<KeyValueInMemoryRecordReader>(
        /*last_sequence_num=*/0, std::move(src_array),
        std::vector<RecordBatch::RowKind>(
            {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::UPDATE_BEFORE,
             RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::DELETE}),
        std::vector<std::string>({"k0", "k1"}),
        /*user_defined_sequence_fields=*/std::vector<std::string>(), key_comparator,
        merge_function_wrapper_, pool_);
    ASSERT_OK_AND_ASSIGN(
        auto results, (ReadResultCollector::CollectKeyValueResult<KeyValueInMemoryRecordReader,
                                                                  KeyValueRecordReader::Iterator>(
                          record_reader.get())));
    KeyValueChecker::CheckResult(expected, results, /*key_arity=*/2, /*value_arity=*/5);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyValueRecordReader::Iterator> eof_iter,
                         record_reader->NextBatch());
    ASSERT_FALSE(eof_iter);
    auto metrics = record_reader->GetReaderMetrics();
    ASSERT_TRUE(metrics);
    ASSERT_EQ(0, metrics->GetAllCounters().size());
}

TEST_F(KeyValueInMemoryRecordReaderTest, TestUserDefinedSequenceFields) {
    std::vector<DataField> fields = {DataField(0, arrow::field("k1", arrow::int32())),
                                     DataField(1, arrow::field("k0", arrow::int32())),
                                     DataField(2, arrow::field("v0", arrow::int32())),
                                     DataField(3, arrow::field("s1", arrow::int32())),
                                     DataField(4, arrow::field("s0", arrow::int32()))};

    std::shared_ptr<arrow::DataType> src_type =
        DataField::ConvertDataFieldsToArrowStructType(fields);

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [2, 2, 11, 21, 31],
        [1, 2, 11, 21, 31],
        [1, 1, 10, 20, 30],
        [2, 2, 11, 21, null],
        [2, 3, 13, 23, 33],
        [2, 2, 11, 22, 31],
        [2, 2, 12, 22, 32]
    ])")
            .ValueOrDie());

    std::vector<KeyValue> expected;
    expected.emplace_back(
        RowKind::Insert(), /*sequence_number=*/2, /*level=*/KeyValue::UNKNOWN_LEVEL,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({1, 1}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({1, 1, 10, 20, 30}, pool_.get()));
    expected.emplace_back(
        RowKind::Insert(), /*sequence_number=*/1, /*level=*/KeyValue::UNKNOWN_LEVEL,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({2, 1}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({1, 2, 11, 21, 31}, pool_.get()));
    expected.emplace_back(
        RowKind::Insert(), /*sequence_number=*/6, /*level=*/KeyValue::UNKNOWN_LEVEL,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({2, 2}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({2, 2, 12, 22, 32}, pool_.get()));
    expected.emplace_back(
        RowKind::Insert(), /*sequence_number=*/4, /*level=*/KeyValue::UNKNOWN_LEVEL,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({3, 2}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({2, 3, 13, 23, 33}, pool_.get()));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> key_comparator,
                         FieldsComparator::Create({fields[1], fields[0]},
                                                  /*is_ascending_order=*/true,
                                                  /*use_view=*/true));

    auto record_reader = std::make_unique<KeyValueInMemoryRecordReader>(
        /*last_sequence_num=*/0, std::move(src_array), std::vector<RecordBatch::RowKind>(),
        std::vector<std::string>({"k0", "k1"}),
        /*user_defined_sequence_fields=*/std::vector<std::string>({"s0", "s1"}), key_comparator,
        merge_function_wrapper_, pool_);
    ASSERT_OK_AND_ASSIGN(
        auto results, (ReadResultCollector::CollectKeyValueResult<KeyValueInMemoryRecordReader,
                                                                  KeyValueRecordReader::Iterator>(
                          record_reader.get())));
    KeyValueChecker::CheckResult(expected, results, /*key_arity=*/2, /*value_arity=*/5);
}

TEST_F(KeyValueInMemoryRecordReaderTest, TestNonExistPK) {
    std::vector<DataField> fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                     DataField(1, arrow::field("k1", arrow::int32())),
                                     DataField(2, arrow::field("v0", arrow::int32())),
                                     DataField(3, arrow::field("v1", arrow::int32())),
                                     DataField(4, arrow::field("v2", arrow::int32()))};

    std::shared_ptr<arrow::DataType> src_type =
        DataField::ConvertDataFieldsToArrowStructType(fields);

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 2, 11, 21, 31],
        [1, 1, 10, 20, 30],
        [2, 3, 13, 23, 33],
        [2, 2, 12, 22, 32]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> key_comparator,
                         FieldsComparator::Create({fields[0], fields[1]},
                                                  /*is_ascending_order=*/true,
                                                  /*use_view=*/true));

    auto record_reader = std::make_unique<KeyValueInMemoryRecordReader>(
        /*last_sequence_num=*/0, std::move(src_array), std::vector<RecordBatch::RowKind>(),
        std::vector<std::string>({"non_exist_key"}),
        /*user_defined_sequence_fields=*/std::vector<std::string>(), key_comparator,
        merge_function_wrapper_, pool_);
    ASSERT_NOK_WITH_MSG((ReadResultCollector::CollectKeyValueResult<KeyValueInMemoryRecordReader,
                                                                    KeyValueRecordReader::Iterator>(
                            record_reader.get())),
                        "cannot find field non_exist_key");
}

TEST_F(KeyValueInMemoryRecordReaderTest, TestStableSortAndMerge) {
    std::vector<DataField> fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                     DataField(1, arrow::field("k1", arrow::int32())),
                                     DataField(2, arrow::field("v0", arrow::int32())),
                                     DataField(3, arrow::field("v1", arrow::int32())),
                                     DataField(4, arrow::field("v2", arrow::int32()))};

    std::shared_ptr<arrow::DataType> src_type =
        DataField::ConvertDataFieldsToArrowStructType(fields);

    // the first row and the last row are both k0,k1: 1, 2, as stable sort, the first row will be
    // return before the last
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [1, 2, 11, 21, 31],
        [1, 1, 10, 20, 30],
        [2, 3, 13, 23, 33],
        [1, 2, 12, 22, 32]
    ])")
            .ValueOrDie());

    std::vector<KeyValue> expected;
    expected.emplace_back(
        RowKind::UpdateBefore(), /*sequence_number=*/1, /*level=*/KeyValue::UNKNOWN_LEVEL,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({1, 1}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({1, 1, 10, 20, 30}, pool_.get()));
    expected.emplace_back(
        RowKind::Delete(), /*sequence_number=*/3, /*level=*/KeyValue::UNKNOWN_LEVEL,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({1, 2}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({1, 2, 12, 22, 32}, pool_.get()));
    expected.emplace_back(
        RowKind::UpdateAfter(), /*sequence_number=*/2, /*level=*/KeyValue::UNKNOWN_LEVEL,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({2, 3}, pool_.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({2, 3, 13, 23, 33}, pool_.get()));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> key_comparator,
                         FieldsComparator::Create({fields[0], fields[1]},
                                                  /*is_ascending_order=*/true,
                                                  /*use_view=*/true));

    auto record_reader = std::make_unique<KeyValueInMemoryRecordReader>(
        /*last_sequence_num=*/0, std::move(src_array),
        std::vector<RecordBatch::RowKind>(
            {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::UPDATE_BEFORE,
             RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::DELETE}),
        std::vector<std::string>({"k0", "k1"}),
        /*user_defined_sequence_fields=*/std::vector<std::string>(), key_comparator,
        merge_function_wrapper_, pool_);
    ASSERT_OK_AND_ASSIGN(
        auto results, (ReadResultCollector::CollectKeyValueResult<KeyValueInMemoryRecordReader,
                                                                  KeyValueRecordReader::Iterator>(
                          record_reader.get())));
    KeyValueChecker::CheckResult(expected, results, /*key_arity=*/2, /*value_arity=*/5);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyValueRecordReader::Iterator> eof_iter,
                         record_reader->NextBatch());
    ASSERT_FALSE(eof_iter);
}

TEST_F(KeyValueInMemoryRecordReaderTest, TestVariantType) {
    // test null, repeated key, sequence fields, out of order in variant data type
    // precondition: fields[0] is key fields, fields[1] is sequence field, src_array are like:
    // (k2>k1, s2>s1) [k2, s2, 11, 21, 31], [k1, s1, 10, 20, 30], [k2, s1, 13, 23, 33], [k1, null,
    // 12, 22, 32]
    auto CheckVariantType = [&](const std::vector<DataField>& fields,
                                std::shared_ptr<arrow::StructArray>&& src_array,
                                const BinaryRowGenerator::ValueType& key,
                                const BinaryRowGenerator::ValueType& sequence) {
        std::vector<KeyValue> expected;
        expected.emplace_back(
            RowKind::Insert(), /*sequence_number=*/1, /*level=*/KeyValue::UNKNOWN_LEVEL,
            /*key=*/BinaryRowGenerator::GenerateRowPtr({key[0]}, pool_.get()),
            /*value=*/
            BinaryRowGenerator::GenerateRowPtr({key[0], sequence[0], 10, 20, 30}, pool_.get()));
        expected.emplace_back(
            RowKind::Insert(), /*sequence_number=*/0, /*level=*/KeyValue::UNKNOWN_LEVEL,
            /*key=*/BinaryRowGenerator::GenerateRowPtr({key[1]}, pool_.get()),
            /*value=*/
            BinaryRowGenerator::GenerateRowPtr({key[1], sequence[1], 11, 21, 31}, pool_.get()));

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> key_comparator,
                             FieldsComparator::Create({fields[0]},
                                                      /*is_ascending_order=*/true,
                                                      /*use_view=*/true));

        auto record_reader = std::make_unique<KeyValueInMemoryRecordReader>(
            /*last_sequence_num=*/0, std::move(src_array), std::vector<RecordBatch::RowKind>(),
            std::vector<std::string>({"k0"}),
            /*user_defined_sequence_fields=*/std::vector<std::string>({"s0"}), key_comparator,
            merge_function_wrapper_, pool_);
        ASSERT_OK_AND_ASSIGN(
            auto results,
            (ReadResultCollector::CollectKeyValueResult<KeyValueInMemoryRecordReader,
                                                        KeyValueRecordReader::Iterator>(
                record_reader.get())));
        KeyValueChecker::CheckResult(expected, results,
                                     /*key_fields=*/{fields[0]},
                                     /*value_fields=*/fields);
    };
    {
        // test int8 and int16
        std::vector<DataField> fields = {DataField(0, arrow::field("k0", arrow::int8())),
                                         DataField(1, arrow::field("s0", arrow::int16())),
                                         DataField(2, arrow::field("v0", arrow::int32())),
                                         DataField(3, arrow::field("v1", arrow::int32())),
                                         DataField(4, arrow::field("v2", arrow::int32()))};

        auto src_type = DataField::ConvertDataFieldsToArrowStructType(fields);
        auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [2, 2, 11, 21, 31],
        [1, 1, 10, 20, 30],
        [2, 1, 13, 23, 33],
        [1, null, 12, 22, 32]
    ])")
                .ValueOrDie());
        CheckVariantType(fields, std::move(src_array),
                         {static_cast<int8_t>(1), static_cast<int8_t>(2)},
                         {static_cast<int16_t>(1), static_cast<int16_t>(2)});
    }
    {
        // test int32 and int64
        std::vector<DataField> fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                         DataField(1, arrow::field("s0", arrow::int64())),
                                         DataField(2, arrow::field("v0", arrow::int32())),
                                         DataField(3, arrow::field("v1", arrow::int32())),
                                         DataField(4, arrow::field("v2", arrow::int32()))};

        auto src_type = DataField::ConvertDataFieldsToArrowStructType(fields);
        auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [2, 2, 11, 21, 31],
        [1, 1, 10, 20, 30],
        [2, 1, 13, 23, 33],
        [1, null, 12, 22, 32]
    ])")
                .ValueOrDie());
        CheckVariantType(fields, std::move(src_array),
                         {static_cast<int32_t>(1), static_cast<int32_t>(2)},
                         {static_cast<int64_t>(1), static_cast<int64_t>(2)});
    }
    {
        // test float and double
        std::vector<DataField> fields = {DataField(0, arrow::field("k0", arrow::float32())),
                                         DataField(1, arrow::field("s0", arrow::float64())),
                                         DataField(2, arrow::field("v0", arrow::int32())),
                                         DataField(3, arrow::field("v1", arrow::int32())),
                                         DataField(4, arrow::field("v2", arrow::int32()))};

        auto src_type = DataField::ConvertDataFieldsToArrowStructType(fields);
        auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [2.1, 2.12, 11, 21, 31],
        [1.1, 1.12, 10, 20, 30],
        [2.1, 1.12, 13, 23, 33],
        [1.1, null, 12, 22, 32]
    ])")
                .ValueOrDie());
        CheckVariantType(fields, std::move(src_array),
                         {static_cast<float>(1.1), static_cast<float>(2.1)}, {1.12, 2.12});
    }
    {
        // test string and binary
        std::vector<DataField> fields = {DataField(0, arrow::field("k0", arrow::utf8())),
                                         DataField(1, arrow::field("s0", arrow::binary())),
                                         DataField(2, arrow::field("v0", arrow::int32())),
                                         DataField(3, arrow::field("v1", arrow::int32())),
                                         DataField(4, arrow::field("v2", arrow::int32()))};

        auto src_type = DataField::ConvertDataFieldsToArrowStructType(fields);
        auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        ["2.1", "2.12", 11, 21, 31],
        ["1.1", "1.12", 10, 20, 30],
        ["2.1", "1.12", 13, 23, 33],
        ["1.1", null, 12, 22, 32]
    ])")
                .ValueOrDie());

        CheckVariantType(fields, std::move(src_array), {std::string("1.1"), std::string("2.1")},
                         {std::make_shared<Bytes>("1.12", pool_.get()),
                          std::make_shared<Bytes>("2.12", pool_.get())});
    }
    {
        // test date and bool
        std::vector<DataField> fields = {DataField(0, arrow::field("k0", arrow::date32())),
                                         DataField(1, arrow::field("s0", arrow::boolean())),
                                         DataField(2, arrow::field("v0", arrow::int32())),
                                         DataField(3, arrow::field("v1", arrow::int32())),
                                         DataField(4, arrow::field("v2", arrow::int32()))};

        auto src_type = DataField::ConvertDataFieldsToArrowStructType(fields);
        auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [2, true, 11, 21, 31],
        [1, false, 10, 20, 30],
        [2, false, 13, 23, 33],
        [1, null, 12, 22, 32]
    ])")
                .ValueOrDie());
        CheckVariantType(fields, std::move(src_array),
                         {static_cast<int32_t>(1), static_cast<int32_t>(2)}, {false, true});
    }
    {
        // test timestamp and decimal
        std::vector<DataField> fields = {
            DataField(0, arrow::field("k0", arrow::timestamp(arrow::TimeUnit::NANO))),
            DataField(1, arrow::field("s0", arrow::decimal128(5, 3))),
            DataField(2, arrow::field("v0", arrow::int32())),
            DataField(3, arrow::field("v1", arrow::int32())),
            DataField(4, arrow::field("v2", arrow::int32()))};

        auto src_type = DataField::ConvertDataFieldsToArrowStructType(fields);
        auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        ["2033-05-18 03:33:20.0", "12.345", 11, 21, 31],
        ["1923-05-18 03:33:20.0", "-12.345", 10, 20, 30],
        ["2033-05-18 03:33:20.0", "-12.345", 13, 23, 33],
        ["1923-05-18 03:33:20.0", null, 12, 22, 32]
    ])")
                .ValueOrDie());
        CheckVariantType(fields, std::move(src_array),
                         {TimestampType(Timestamp(-1471379200000l, 0), 9),
                          TimestampType(Timestamp(2000000000000l, 0), 9)},
                         {Decimal(5, 3, -12345), Decimal(5, 3, 12345)});
    }
}

}  // namespace paimon::test
