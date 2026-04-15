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

#include "paimon/core/mergetree/compact/lookup_merge_function.h"

#include <map>
#include <string>
#include <variant>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/aggregate/aggregate_merge_function.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(LookupMergeFunctionTest, TestSimple) {
    arrow::FieldVector fields = {arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};
    auto value_schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::FIELDS_DEFAULT_AGG_FUNC, "sum"}}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<AggregateMergeFunction> agg_merge_func,
        AggregateMergeFunction::Create(value_schema, /*primary_keys=*/{"k0"}, core_options));
    auto merge_func = std::make_unique<LookupMergeFunction>(std::move(agg_merge_func));

    auto pool = GetDefaultPool();
    KeyValue kv0(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 50}, pool.get()));
    ASSERT_OK(merge_func->Add(std::move(kv0)));
    auto result_kv = std::move(merge_func->GetResult().value().value());
    KeyValue expected(RowKind::Insert(), /*sequence_number=*/0,
                      /*level=*/KeyValue::UNKNOWN_LEVEL, /*key=*/
                      BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                      /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 50}, pool.get()));
    KeyValueChecker::CheckResult(expected, result_kv, /*key_arity=*/1, /*value_arity=*/2);

    merge_func->Reset();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/2, /*level=*/2,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/3, /*level=*/1, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 300}, pool.get()));

    // only kv1 (level 0) and kv3 (level 1) will be merged, and kv2 will be ignored
    ASSERT_OK(merge_func->Add(std::move(kv1)));
    ASSERT_OK(merge_func->Add(std::move(kv2)));
    ASSERT_OK(merge_func->Add(std::move(kv3)));
    result_kv = std::move(merge_func->GetResult().value().value());
    KeyValue expected2(RowKind::Insert(), /*sequence_number=*/3,
                       /*level=*/KeyValue::UNKNOWN_LEVEL, /*key=*/
                       BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                       /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 400}, pool.get()));
    KeyValueChecker::CheckResult(expected2, result_kv, /*key_arity=*/1, /*value_arity=*/2);
}

TEST(LookupMergeFunctionTest, TestDeduplicate) {
    auto deduplicate_merge_func =
        std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
    auto merge_func = std::make_unique<LookupMergeFunction>(std::move(deduplicate_merge_func));

    auto pool = GetDefaultPool();
    KeyValue kv0(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 50}, pool.get()));
    ASSERT_OK(merge_func->Add(std::move(kv0)));
    auto result_kv = std::move(merge_func->GetResult().value().value());
    KeyValue expected(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                      BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                      /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 50}, pool.get()));
    KeyValueChecker::CheckResult(expected, result_kv, /*key_arity=*/1, /*value_arity=*/2);

    merge_func->Reset();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/2, /*level=*/2,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/3, /*level=*/1, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 300}, pool.get()));

    // only kv1 (level 0) and kv3 (level 1) will be merged, and kv2 will be ignored
    // as add order is kv1, then kv3, the result is 300
    ASSERT_OK(merge_func->Add(std::move(kv1)));
    ASSERT_OK(merge_func->Add(std::move(kv2)));
    ASSERT_OK(merge_func->Add(std::move(kv3)));
    result_kv = std::move(merge_func->GetResult().value().value());
    KeyValue expected2(RowKind::Insert(), /*sequence_number=*/3, /*level=*/1, /*key=*/
                       BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                       /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 300}, pool.get()));
    KeyValueChecker::CheckResult(expected2, result_kv, /*key_arity=*/1, /*value_arity=*/2);
}

TEST(LookupMergeFunctionTest, TestPickHighLevel) {
    auto deduplicate_merge_func =
        std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
    auto merge_func = std::make_unique<LookupMergeFunction>(std::move(deduplicate_merge_func));

    auto pool = GetDefaultPool();

    merge_func->Reset();
    ASSERT_FALSE(merge_func->PickHighLevelIdx());
    ASSERT_FALSE(merge_func->ContainLevel0());

    merge_func->Reset();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/1, /*level=*/2, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/2, /*level=*/1,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/4, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 300}, pool.get()));

    ASSERT_OK(merge_func->Add(std::move(kv1)));
    ASSERT_OK(merge_func->Add(std::move(kv2)));
    ASSERT_OK(merge_func->Add(std::move(kv3)));
    ASSERT_TRUE(merge_func->ContainLevel0());
    ASSERT_EQ(merge_func->GetKey()->GetInt(0), 10);
    ASSERT_EQ(merge_func->PickHighLevelIdx(), 1);
}

TEST(LookupMergeFunctionTest, TestInsertInto) {
    auto deduplicate_merge_func =
        std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
    auto merge_func = std::make_unique<LookupMergeFunction>(std::move(deduplicate_merge_func));

    auto pool = GetDefaultPool();

    merge_func->Reset();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/0, /*level=*/3, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 300}, pool.get()));

    ASSERT_OK(merge_func->Add(std::move(kv1)));
    ASSERT_OK(merge_func->Add(std::move(kv2)));
    merge_func->InsertInto(std::move(kv3), [](const KeyValue& o1, const KeyValue& o2) {
        return o1.sequence_number < o2.sequence_number;
    });
    ASSERT_EQ(merge_func->candidates_.size(), 3);
    ASSERT_EQ(merge_func->candidates_[0].sequence_number, 0);
    ASSERT_EQ(merge_func->candidates_[1].sequence_number, 1);
    ASSERT_EQ(merge_func->candidates_[2].sequence_number, 2);

    auto result_kv = std::move(merge_func->GetResult().value().value());
    KeyValue expected(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0,
                      /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                      /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 200}, pool.get()));
    KeyValueChecker::CheckResult(expected, result_kv, /*key_arity=*/1, /*value_arity=*/2);
}
}  // namespace paimon::test
