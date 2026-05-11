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

#include "paimon/core/io/merged_key_value_record_reader.h"

#include <memory>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/mock/mock_key_value_data_file_record_reader.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class MergedKeyValueRecordReaderTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        merge_function_wrapper_ = std::make_shared<ReducerMergeFunctionWrapper>(std::move(mfunc));
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<ReducerMergeFunctionWrapper> merge_function_wrapper_;
};

TEST_F(MergedKeyValueRecordReaderTest, TestMergeAcrossUnderlyingBatches) {
    std::vector<DataField> fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                     DataField(1, arrow::field("k1", arrow::int32())),
                                     DataField(2, arrow::field("v0", arrow::int32())),
                                     DataField(3, arrow::field("v1", arrow::int32())),
                                     DataField(4, arrow::field("v2", arrow::int32()))};

    auto value_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    auto arrow_fields = value_schema->fields();
    auto key_schema = arrow::schema({arrow_fields[0], arrow_fields[1]});
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(value_schema)->fields());

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 10, 20, 30],
        [1, 0, 1, 1, 11, 21, 31],
        [2, 0, 2, 2, 12, 22, 32],
        [3, 0, 2, 2, 13, 23, 33]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> key_comparator,
                         FieldsComparator::Create({fields[0], fields[1]},
                                                  /*is_ascending_order=*/true));

    auto expected = KeyValueChecker::GenerateKeyValues(
        /*seq_vec=*/{1, 3}, /*key_vec=*/{{1, 1}, {2, 2}},
        /*value_vec=*/{{1, 1, 11, 21, 31}, {2, 2, 13, 23, 33}}, pool_);

    for (auto batch_size : {1, 2, 3}) {
        auto file_batch_reader = std::make_unique<MockFileBatchReader>(src_array, src_type,
                                                                       /*batch_size=*/batch_size);
        auto raw_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
            std::move(file_batch_reader), key_schema, value_schema, /*level=*/0, pool_);
        auto merged_reader = std::make_unique<MergedKeyValueRecordReader>(
            std::move(raw_reader), key_comparator, merge_function_wrapper_);

        ASSERT_OK_AND_ASSIGN(
            auto results,
            (ReadResultCollector::CollectKeyValueResult<
                MergedKeyValueRecordReader, KeyValueRecordReader::Iterator>(merged_reader.get())));
        KeyValueChecker::CheckResult(expected, results, /*key_arity=*/2, /*value_arity=*/5);
    }
}

TEST_F(MergedKeyValueRecordReaderTest, TestSkipMergedNulloptResultInHasNext) {
    auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/true);
    auto merge_function_wrapper = std::make_shared<ReducerMergeFunctionWrapper>(std::move(mfunc));

    std::vector<DataField> fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                     DataField(1, arrow::field("v0", arrow::int32()))};

    auto value_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    auto arrow_fields = value_schema->fields();
    auto key_schema = arrow::schema({arrow_fields[0]});
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(value_schema)->fields());

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 3, 1, 10],
        [3, 3, 1, 11],
        [2, 3, 2, 200],
        [4, 3, 2, 240],
        [1, 0, 3, 300],
        [5, 3, 3, 30]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> key_comparator,
                         FieldsComparator::Create({fields[0]}, /*is_ascending_order=*/true));

    auto expected = KeyValueChecker::GenerateKeyValues(
        /*seq_vec=*/{1}, /*key_vec=*/{{3}}, /*value_vec=*/{{3, 300}}, pool_);

    for (auto batch_size : {1, 2, 3}) {
        auto file_batch_reader =
            std::make_unique<MockFileBatchReader>(src_array, src_type, /*batch_size=*/batch_size);
        auto raw_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
            std::move(file_batch_reader), key_schema, value_schema, /*level=*/0, pool_);
        auto merged_reader = std::make_unique<MergedKeyValueRecordReader>(
            std::move(raw_reader), key_comparator, merge_function_wrapper);

        ASSERT_OK_AND_ASSIGN(
            auto results,
            (ReadResultCollector::CollectKeyValueResult<
                MergedKeyValueRecordReader, KeyValueRecordReader::Iterator>(merged_reader.get())));
        KeyValueChecker::CheckResult(expected, results, /*key_arity=*/1, /*value_arity=*/2);
    }
}

}  // namespace paimon::test
