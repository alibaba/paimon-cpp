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

#include "paimon/core/mergetree/compact/aggregate/field_nested_update_agg.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/generic_array.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/data/serializer/binary_serializer_utils.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/core/core_options.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

std::shared_ptr<arrow::DataType> NestedType() {
    return arrow::list(
        arrow::struct_({arrow::field("id", arrow::int32()), arrow::field("seq", arrow::int32()),
                        arrow::field("value", arrow::int32())}));
}

std::shared_ptr<InternalRow> Row(VariantType id, int32_t sequence, int32_t value) {
    std::shared_ptr<GenericRow> row = std::make_shared<GenericRow>(3);
    row->SetField(0, id);
    row->SetField(1, sequence);
    row->SetField(2, value);
    return row;
}

VariantType Rows(std::vector<VariantType> rows) {
    return VariantType(
        std::static_pointer_cast<InternalArray>(std::make_shared<GenericArray>(std::move(rows))));
}

std::shared_ptr<InternalArray> GetRows(const VariantType& value) {
    return DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(value);
}

std::shared_ptr<InternalRow> FindRow(const VariantType& value, int32_t id) {
    std::shared_ptr<InternalArray> rows = GetRows(value);
    for (int32_t i = 0; i < rows->Size(); ++i) {
        if (rows->IsNullAt(i)) {
            continue;
        }
        std::shared_ptr<InternalRow> row = rows->GetRow(i, 3);
        if (!row->IsNullAt(0) && row->GetInt(0) == id) {
            return row;
        }
    }
    return nullptr;
}

Result<std::unique_ptr<FieldNestedUpdateAgg>> MakeAgg(
    const std::map<std::string, std::string>& options_map) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_map));
    return FieldNestedUpdateAgg::Create(NestedType(), options, "f");
}

}  // namespace

TEST(FieldNestedUpdateAggTest, UpsertsByKeySequenceAndCountLimit) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> agg,
                         MakeAgg({{"fields.f.nested-key", "id"},
                                  {"fields.f.nested-sequence-field", "seq"},
                                  {"fields.f.count-limit", "2"}}));

    VariantType accumulator = Rows({Row(int32_t{1}, 1, 10), Row(int32_t{2}, 1, 20)});
    VariantType input =
        Rows({Row(int32_t{1}, 0, 100), Row(int32_t{1}, 2, 200), Row(int32_t{3}, 3, 300)});
    ASSERT_OK_AND_ASSIGN(VariantType result, agg->Agg(accumulator, input));

    ASSERT_EQ(2, GetRows(result)->Size());
    ASSERT_EQ(200, FindRow(result, 1)->GetInt(2));
    ASSERT_EQ(20, FindRow(result, 2)->GetInt(2));
    ASSERT_FALSE(FindRow(result, 3));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BinaryArray> binary_result,
                         BinarySerializerUtils::WriteBinaryArray(GetRows(result), NestedType(),
                                                                 GetDefaultPool().get()));
    ASSERT_EQ(2, binary_result->Size());

    ASSERT_OK_AND_ASSIGN(VariantType retracted,
                         agg->Retract(result, Rows({Row(int32_t{1}, 999, -1)})));
    ASSERT_EQ(1, GetRows(retracted)->Size());
    ASSERT_FALSE(FindRow(retracted, 1));
}

TEST(FieldNestedUpdateAggTest, AppendsNonNullRowsUpToLimit) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> agg,
                         MakeAgg({{"fields.f.count-limit", "2"}}));
    ASSERT_OK_AND_ASSIGN(
        VariantType result,
        agg->Agg(VariantType(NullType()), Rows({VariantType(NullType()), Row(int32_t{1}, 1, 10),
                                                Row(int32_t{2}, 1, 20), Row(int32_t{3}, 1, 30)})));
    ASSERT_EQ(2, GetRows(result)->Size());
    ASSERT_TRUE(FindRow(result, 1));
    ASSERT_TRUE(FindRow(result, 2));
}

// count limit is measured against the raw element count, so null elements consume the limit even
// though they are dropped from the result
TEST(FieldNestedUpdateAggTest, CountLimitCountsNullElementsOfAccumulator) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> full_agg,
                         MakeAgg({{"fields.f.count-limit", "3"}}));
    VariantType full = Rows({VariantType(NullType()), Row(int32_t{1}, 1, 10),
                             VariantType(NullType()), Row(int32_t{2}, 1, 20)});
    ASSERT_OK_AND_ASSIGN(VariantType unchanged,
                         full_agg->Agg(full, Rows({Row(int32_t{3}, 1, 30)})));
    ASSERT_EQ(4, GetRows(unchanged)->Size());
    ASSERT_FALSE(FindRow(unchanged, 3));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> agg,
                         MakeAgg({{"fields.f.count-limit", "4"}}));
    VariantType accumulator = Rows({VariantType(NullType()), Row(int32_t{1}, 1, 10)});
    ASSERT_OK_AND_ASSIGN(VariantType result,
                         agg->Agg(accumulator, Rows({Row(int32_t{2}, 1, 20), Row(int32_t{3}, 1, 30),
                                                     Row(int32_t{4}, 1, 40)})));
    ASSERT_EQ(3, GetRows(result)->Size());
    ASSERT_TRUE(FindRow(result, 1));
    ASSERT_TRUE(FindRow(result, 2));
    ASSERT_TRUE(FindRow(result, 3));
    ASSERT_FALSE(FindRow(result, 4));
}

// matches Java's RecordEqualiser, which compares the row kind before any field
TEST(FieldNestedUpdateAggTest, RetractRequiresMatchingRowKind) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> agg, MakeAgg({}));
    std::shared_ptr<GenericRow> retract_row = std::make_shared<GenericRow>(3);
    retract_row->SetField(0, int32_t{1});
    retract_row->SetField(1, 1);
    retract_row->SetField(2, 10);
    retract_row->SetRowKind(RowKind::Delete());

    ASSERT_OK_AND_ASSIGN(
        VariantType kept,
        agg->Retract(Rows({Row(int32_t{1}, 1, 10)}),
                     Rows({VariantType(std::static_pointer_cast<InternalRow>(retract_row))})));
    ASSERT_EQ(1, GetRows(kept)->Size());
    ASSERT_TRUE(FindRow(kept, 1));
}

TEST(FieldNestedUpdateAggTest, AppliesNullKeyStrategies) {
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FieldNestedUpdateAgg> ignore_agg,
        MakeAgg({{"fields.f.nested-key", "id"}, {"fields.f.nested-key-null-strategy", "ignore"}}));
    ASSERT_OK_AND_ASSIGN(
        VariantType ignored,
        ignore_agg->Agg(VariantType(NullType()), Rows({Row(VariantType(NullType()), 1, 10)})));
    ASSERT_EQ(0, GetRows(ignored)->Size());

    ASSERT_OK_AND_ASSIGN(VariantType normalized,
                         ignore_agg->Retract(Rows({Row(VariantType(NullType()), 1, 10),
                                                   Row(int32_t{1}, 1, 10), Row(int32_t{1}, 2, 20)}),
                                             Rows({})));
    ASSERT_EQ(1, GetRows(normalized)->Size());
    ASSERT_EQ(20, FindRow(normalized, 1)->GetInt(2));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FieldNestedUpdateAgg> error_agg,
        MakeAgg({{"fields.f.nested-key", "id"}, {"fields.f.nested-key-null-strategy", "error"}}));
    ASSERT_NOK(
        error_agg->Agg(VariantType(NullType()), Rows({Row(VariantType(NullType()), 1, 10)})));
}

TEST(FieldNestedUpdateAggTest, ValidatesTypeAndOptionDependencies) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
    ASSERT_NOK(FieldNestedUpdateAgg::Create(arrow::list(arrow::int32()), options, "f"));

    ASSERT_OK_AND_ASSIGN(CoreOptions strategy_without_key,
                         CoreOptions::FromMap({{"fields.f.nested-key-null-strategy", "ignore"}}));
    ASSERT_NOK(FieldNestedUpdateAgg::Create(NestedType(), strategy_without_key, "f"));

    ASSERT_OK_AND_ASSIGN(CoreOptions sequence_without_key,
                         CoreOptions::FromMap({{"fields.f.nested-sequence-field", "seq"}}));
    ASSERT_NOK(FieldNestedUpdateAgg::Create(NestedType(), sequence_without_key, "f"));

    ASSERT_OK_AND_ASSIGN(CoreOptions invalid_strategy,
                         CoreOptions::FromMap({{"fields.f.nested-key", "id"},
                                               {"fields.f.nested-key-null-strategy", "invalid"}}));
    ASSERT_NOK(FieldNestedUpdateAgg::Create(NestedType(), invalid_strategy, "f"));

    ASSERT_OK_AND_ASSIGN(CoreOptions negative_limit,
                         CoreOptions::FromMap({{"fields.f.count-limit", "-1"}}));
    ASSERT_NOK(FieldNestedUpdateAgg::Create(NestedType(), negative_limit, "f"));
}

}  // namespace paimon::test
