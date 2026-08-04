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

#include "paimon/core/utils/primary_key_table_utils.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <utility>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/core_options.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(PrimaryKeyTableUtilsTest, TestCreateSequenceFieldsComparator) {
    {
        std::vector<DataField> value_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                               DataField(1, arrow::field("v1", arrow::int32())),
                                               DataField(1, arrow::field("s0", arrow::int32())),
                                               DataField(1, arrow::field("s1", arrow::int32()))};
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                             CoreOptions::FromMap({{Options::SEQUENCE_FIELD, "s0,s1"}}));
        ASSERT_OK_AND_ASSIGN(auto comparator, PrimaryKeyTableUtils::CreateSequenceFieldsComparator(
                                                  value_fields, core_options));
        ASSERT_EQ(comparator->CompareFields(), std::vector<int32_t>({2, 3}));
    }
    {
        std::vector<DataField> value_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                               DataField(1, arrow::field("v1", arrow::int32()))};
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                             CoreOptions::FromMap({{Options::SEQUENCE_FIELD, "s0,s1"}}));
        ASSERT_NOK_WITH_MSG(
            PrimaryKeyTableUtils::CreateSequenceFieldsComparator(value_fields, core_options),
            "sequence field s0 does not in value fields");
    }
}

TEST(PrimaryKeyTableUtilsTest, TestCreateFirstRowMergeFunctionWithIgnoreDelete) {
    auto pool = GetDefaultPool();
    auto value_schema = arrow::schema({arrow::field("v0", arrow::int32())});

    // ignore-delete can also be configured through the merge-engine-specific
    // first-row.ignore-delete key, which must reach FirstRowMergeFunction the same way.
    for (const char* ignore_delete_key :
         {Options::IGNORE_DELETE, Options::FALLBACK_FIRST_ROW_IGNORE_DELETE}) {
        ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                             CoreOptions::FromMap({{Options::MERGE_ENGINE, "first-row"},
                                                   {ignore_delete_key, "true"}}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<MergeFunction> merge_function,
                             PrimaryKeyTableUtils::CreateMergeFunction(
                                 value_schema, {"k0"}, core_options, GetDefaultPool()));
        merge_function->Reset();

        KeyValue insert_kv(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                           BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                           /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
        KeyValue delete_kv(RowKind::Delete(), /*sequence_number=*/1, /*level=*/0, /*key=*/
                           BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                           /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
        ASSERT_OK(merge_function->Add(std::move(insert_kv)));
        ASSERT_OK(merge_function->Add(std::move(delete_kv)));

        ASSERT_OK_AND_ASSIGN(std::optional<KeyValue> result_kv, merge_function->GetResult());
        ASSERT_TRUE(result_kv.has_value());
        KeyValue expected(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                          BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                          /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
        KeyValueChecker::CheckResult(expected, result_kv.value(), /*key_arity=*/1,
                                     /*value_arity=*/1);
    }

    // Without the option, the first-row merge engine still rejects retract records.
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::MERGE_ENGINE, "first-row"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MergeFunction> merge_function,
                         PrimaryKeyTableUtils::CreateMergeFunction(value_schema, {"k0"},
                                                                   core_options, GetDefaultPool()));
    merge_function->Reset();
    KeyValue delete_kv(RowKind::Delete(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                       BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                       /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    ASSERT_NOK_WITH_MSG(merge_function->Add(std::move(delete_kv)),
                        "First row merge engine can not accept DELETE/UPDATE_BEFORE records");
}

}  // namespace paimon::test
