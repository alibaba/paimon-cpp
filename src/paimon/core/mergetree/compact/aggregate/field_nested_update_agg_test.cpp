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

#include <algorithm>
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
#include "paimon/memory/memory_pool.h"
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
    return FieldNestedUpdateAgg::Create(NestedType(), options, "f", GetDefaultPool());
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
    ASSERT_NOK(
        FieldNestedUpdateAgg::Create(arrow::list(arrow::int32()), options, "f", GetDefaultPool()));

    ASSERT_OK_AND_ASSIGN(CoreOptions strategy_without_key,
                         CoreOptions::FromMap({{"fields.f.nested-key-null-strategy", "ignore"}}));
    ASSERT_NOK(
        FieldNestedUpdateAgg::Create(NestedType(), strategy_without_key, "f", GetDefaultPool()));

    ASSERT_OK_AND_ASSIGN(CoreOptions sequence_without_key,
                         CoreOptions::FromMap({{"fields.f.nested-sequence-field", "seq"}}));
    ASSERT_NOK(
        FieldNestedUpdateAgg::Create(NestedType(), sequence_without_key, "f", GetDefaultPool()));

    ASSERT_OK_AND_ASSIGN(CoreOptions invalid_strategy,
                         CoreOptions::FromMap({{"fields.f.nested-key", "id"},
                                               {"fields.f.nested-key-null-strategy", "invalid"}}));
    ASSERT_NOK(FieldNestedUpdateAgg::Create(NestedType(), invalid_strategy, "f", GetDefaultPool()));

    ASSERT_OK_AND_ASSIGN(CoreOptions negative_limit,
                         CoreOptions::FromMap({{"fields.f.count-limit", "-1"}}));
    ASSERT_NOK(FieldNestedUpdateAgg::Create(NestedType(), negative_limit, "f", GetDefaultPool()));

    // Java resolves nested-key names with List.indexOf and accepts repeats, so we must too
    ASSERT_OK_AND_ASSIGN(CoreOptions repeated_key,
                         CoreOptions::FromMap({{"fields.f.nested-key", "id,id"}}));
    ASSERT_OK(FieldNestedUpdateAgg::Create(NestedType(), repeated_key, "f", GetDefaultPool()));
}

// Ported from Java FieldAggregatorTest: composite nested keys, multiple sequence fields and the
// count-limit / null-key-strategy boundaries.
namespace {

std::shared_ptr<arrow::DataType> CompositeKeyType() {
    return arrow::list(
        arrow::struct_({arrow::field("k0", arrow::int32()), arrow::field("k1", arrow::int32()),
                        arrow::field("v", arrow::utf8()), arrow::field("seq", arrow::int32()),
                        arrow::field("seq2", arrow::int32())}));
}

VariantType KeyedRow(VariantType k0, VariantType k1, std::string_view v, int32_t seq,
                     int32_t seq2) {
    std::shared_ptr<GenericRow> row = std::make_shared<GenericRow>(5);
    row->SetField(0, k0);
    row->SetField(1, k1);
    row->SetField(2, v);
    row->SetField(3, seq);
    row->SetField(4, seq2);
    return VariantType(std::static_pointer_cast<InternalRow>(row));
}

Result<std::unique_ptr<FieldNestedUpdateAgg>> MakeKeyedAgg(
    const std::map<std::string, std::string>& options_map) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_map));
    return FieldNestedUpdateAgg::Create(CompositeKeyType(), options, "f", GetDefaultPool());
}

std::vector<std::string> SortedKeyed(const VariantType& value) {
    std::shared_ptr<InternalArray> rows = GetRows(value);
    std::vector<std::string> out;
    for (int32_t i = 0; i < rows->Size(); ++i) {
        std::shared_ptr<InternalRow> row = rows->GetRow(i, 5);
        std::string k0 = row->IsNullAt(0) ? "null" : std::to_string(row->GetInt(0));
        std::string k1 = row->IsNullAt(1) ? "null" : std::to_string(row->GetInt(1));
        out.push_back(k0 + "/" + k1 + "/" + std::string(row->GetStringView(2)));
    }
    std::sort(out.begin(), out.end());
    return out;
}

}  // namespace

TEST(FieldNestedUpdateAggTest, CountLimitStillUpdatesExistingCompositeKey) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> agg,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"},
                                       {"fields.f.nested-sequence-field", "seq"},
                                       {"fields.f.count-limit", "2"}}));
    VariantType acc = VariantType(NullType());
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "B", 1, 0)})));
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(1, 2, "C", 3, 0)})));

    // at the limit an existing key can still be updated
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "B_updated", 4, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B_updated", "1/2/C"}), SortedKeyed(acc));

    // but a new key is rejected
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(2, 3, "D", 5, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B_updated", "1/2/C"}), SortedKeyed(acc));
}

TEST(FieldNestedUpdateAggTest, MultipleSequenceFieldsCompareInOrder) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> agg,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"},
                                       {"fields.f.nested-sequence-field", "seq,seq2"}}));
    VariantType acc = VariantType(NullType());
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "A", 1, 5)})));

    // same leading sequence, smaller second field, so the row is kept
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "older", 1, 4)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/A"}), SortedKeyed(acc));

    // same leading sequence, larger second field, so the row wins
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "newer", 1, 6)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/newer"}), SortedKeyed(acc));
}

TEST(FieldNestedUpdateAggTest, NullKeyStrategyAppliesToRetractInput) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> merge_agg,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"}}));
    VariantType acc = VariantType(NullType());
    ASSERT_OK_AND_ASSIGN(acc, merge_agg->Agg(acc, Rows({KeyedRow(0, 0, "A", 0, 0)})));
    ASSERT_OK_AND_ASSIGN(acc, merge_agg->Agg(acc, Rows({KeyedRow(1, 1, "B", 0, 0)})));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> ignore_agg,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"},
                                       {"fields.f.nested-key-null-strategy", "ignore"}}));
    ASSERT_OK_AND_ASSIGN(
        VariantType kept,
        ignore_agg->Retract(acc, Rows({KeyedRow(0, VariantType(NullType()), "X", 0, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/0/A", "1/1/B"}), SortedKeyed(kept));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> error_agg,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"},
                                       {"fields.f.nested-key-null-strategy", "error"}}));
    ASSERT_NOK(error_agg->Retract(acc, Rows({KeyedRow(0, VariantType(NullType()), "X", 0, 0)})));
}

// Ported from Java FieldAggregatorTest#testFieldNestedAppendAgg*: without a nested key rows are
// appended rather than upserted, and retraction removes an equal row.
TEST(FieldNestedUpdateAggTest, AppendsWithoutNestedKey) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> agg, MakeKeyedAgg({}));
    VariantType acc = VariantType(NullType());
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "B", 0, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B"}), SortedKeyed(acc));

    // same key fields but a different value, so it is appended instead of replacing
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "b", 0, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B", "0/1/b"}), SortedKeyed(acc));

    ASSERT_OK_AND_ASSIGN(acc, agg->Retract(acc, Rows({KeyedRow(0, 1, "b", 0, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B"}), SortedKeyed(acc));
}

TEST(FieldNestedUpdateAggTest, AppendsWithoutNestedKeyRespectCountLimit) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> agg,
                         MakeKeyedAgg({{"fields.f.count-limit", "2"}}));
    VariantType acc = VariantType(NullType());
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "B", 0, 0)})));
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "b", 0, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B", "0/1/b"}), SortedKeyed(acc));

    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "C", 0, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B", "0/1/b"}), SortedKeyed(acc));

    // the limit also applies within a single input array, and null elements are skipped
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> first_input_agg,
                         MakeKeyedAgg({{"fields.f.count-limit", "2"}}));
    ASSERT_OK_AND_ASSIGN(
        VariantType first,
        first_input_agg->Agg(VariantType(NullType()),
                             Rows({KeyedRow(0, 1, "B", 0, 0), VariantType(NullType()),
                                   KeyedRow(0, 1, "b", 0, 0), KeyedRow(0, 1, "C", 0, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B", "0/1/b"}), SortedKeyed(first));
}

TEST(FieldNestedUpdateAggTest, CountLimitAppliesWithinFirstInputArray) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> with_seq,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"},
                                       {"fields.f.nested-sequence-field", "seq"},
                                       {"fields.f.count-limit", "2"}}));
    ASSERT_OK_AND_ASSIGN(
        VariantType seq_result,
        with_seq->Agg(VariantType(NullType()),
                      Rows({KeyedRow(0, 1, "B", 1, 0), KeyedRow(1, 2, "C", 3, 0),
                            KeyedRow(2, 3, "D", 5, 0), KeyedRow(0, 1, "B_updated", 4, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B_updated", "1/2/C"}), SortedKeyed(seq_result));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FieldNestedUpdateAgg> without_seq,
        MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"}, {"fields.f.count-limit", "2"}}));
    ASSERT_OK_AND_ASSIGN(
        VariantType no_seq_result,
        without_seq->Agg(VariantType(NullType()),
                         Rows({KeyedRow(0, 1, "B", 0, 0), KeyedRow(1, 2, "C", 0, 0),
                               KeyedRow(2, 3, "D", 0, 0), KeyedRow(0, 1, "B_updated", 0, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B_updated", "1/2/C"}), SortedKeyed(no_seq_result));
}

TEST(FieldNestedUpdateAggTest, CountLimitStillUpdatesExistingKeyWithoutSequence) {
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FieldNestedUpdateAgg> agg,
        MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"}, {"fields.f.count-limit", "2"}}));
    VariantType acc = VariantType(NullType());
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "B", 0, 0)})));
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(1, 2, "C", 0, 0)})));
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "B_updated", 0, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B_updated", "1/2/C"}), SortedKeyed(acc));

    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(2, 3, "D", 0, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B_updated", "1/2/C"}), SortedKeyed(acc));
}

// MERGE keeps rows whose nested key is partially or fully null, treating null as a key value.
TEST(FieldNestedUpdateAggTest, MergeStrategyKeepsNullNestedKeys) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> agg,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"}}));
    VariantType null_k1 = VariantType(NullType());

    ASSERT_OK_AND_ASSIGN(VariantType partial, agg->Agg(VariantType(NullType()),
                                                       Rows({KeyedRow(0, null_k1, "C", 3, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/null/C"}), SortedKeyed(partial));

    ASSERT_OK_AND_ASSIGN(VariantType full, agg->Agg(VariantType(NullType()),
                                                    Rows({KeyedRow(null_k1, null_k1, "D", 4, 0)})));
    ASSERT_EQ((std::vector<std::string>{"null/null/D"}), SortedKeyed(full));

    VariantType acc = VariantType(NullType());
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 0, "A", 1, 0)})));
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, 1, "B", 2, 0)})));
    ASSERT_OK_AND_ASSIGN(acc, agg->Agg(acc, Rows({KeyedRow(0, null_k1, "C", 3, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/0/A", "0/1/B", "0/null/C"}), SortedKeyed(acc));
}

// Null-keyed rows consume the count limit under MERGE but are skipped entirely under IGNORE.
TEST(FieldNestedUpdateAggTest, CountLimitInteractsWithNullKeyStrategies) {
    VariantType null_key = VariantType(NullType());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> merge_agg,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"},
                                       {"fields.f.nested-sequence-field", "seq"},
                                       {"fields.f.count-limit", "3"}}));
    VariantType merged = VariantType(NullType());
    ASSERT_OK_AND_ASSIGN(merged, merge_agg->Agg(merged, Rows({KeyedRow(0, 1, "B", 1, 0)})));
    ASSERT_OK_AND_ASSIGN(merged,
                         merge_agg->Agg(merged, Rows({KeyedRow(null_key, 2, "NULL_2", 2, 0)})));
    ASSERT_OK_AND_ASSIGN(
        merged, merge_agg->Agg(merged, Rows({KeyedRow(null_key, null_key, "NULL_NULL", 3, 0)})));
    ASSERT_OK_AND_ASSIGN(merged, merge_agg->Agg(merged, Rows({KeyedRow(1, 2, "C", 5, 0)})));
    ASSERT_OK_AND_ASSIGN(merged, merge_agg->Agg(merged, Rows({KeyedRow(0, 1, "B_updated", 4, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B_updated", "null/2/NULL_2", "null/null/NULL_NULL"}),
              SortedKeyed(merged));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> ignore_agg,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"},
                                       {"fields.f.nested-key-null-strategy", "ignore"},
                                       {"fields.f.nested-sequence-field", "seq"},
                                       {"fields.f.count-limit", "3"}}));
    VariantType ignored = VariantType(NullType());
    ASSERT_OK_AND_ASSIGN(ignored, ignore_agg->Agg(ignored, Rows({KeyedRow(0, 1, "B", 1, 0)})));
    ASSERT_OK_AND_ASSIGN(ignored,
                         ignore_agg->Agg(ignored, Rows({KeyedRow(null_key, 2, "NULL_2", 2, 0)})));
    ASSERT_OK_AND_ASSIGN(
        ignored, ignore_agg->Agg(ignored, Rows({KeyedRow(null_key, null_key, "NN", 3, 0)})));
    ASSERT_OK_AND_ASSIGN(ignored, ignore_agg->Agg(ignored, Rows({KeyedRow(1, 2, "C", 3, 0)})));
    ASSERT_OK_AND_ASSIGN(ignored,
                         ignore_agg->Agg(ignored, Rows({KeyedRow(0, 1, "B_updated", 4, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B_updated", "1/2/C"}), SortedKeyed(ignored));

    // room is left for a third real key
    ASSERT_OK_AND_ASSIGN(ignored, ignore_agg->Agg(ignored, Rows({KeyedRow(2, 3, "D", 5, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/1/B_updated", "1/2/C", "2/3/D"}), SortedKeyed(ignored));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> error_agg,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"},
                                       {"fields.f.nested-key-null-strategy", "error"},
                                       {"fields.f.count-limit", "3"}}));
    ASSERT_NOK(
        error_agg->Agg(VariantType(NullType()), Rows({KeyedRow(null_key, 2, "NULL_2", 2, 0)})));
}

TEST(FieldNestedUpdateAggTest, NullKeyStrategyAppliesToRetractAccumulator) {
    VariantType null_key = VariantType(NullType());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> merge_agg,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"}}));
    VariantType acc = VariantType(NullType());
    ASSERT_OK_AND_ASSIGN(acc, merge_agg->Agg(acc, Rows({KeyedRow(0, 0, "A", 0, 0)})));
    ASSERT_OK_AND_ASSIGN(acc, merge_agg->Agg(acc, Rows({KeyedRow(null_key, 1, "N", 0, 0)})));

    // IGNORE drops the null-keyed accumulator row while retracting an unrelated key
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> ignore_agg,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"},
                                       {"fields.f.nested-key-null-strategy", "ignore"}}));
    ASSERT_OK_AND_ASSIGN(VariantType result,
                         ignore_agg->Retract(acc, Rows({KeyedRow(9, 9, "X", 0, 0)})));
    ASSERT_EQ((std::vector<std::string>{"0/0/A"}), SortedKeyed(result));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> error_agg,
                         MakeKeyedAgg({{"fields.f.nested-key", "k0,k1"},
                                       {"fields.f.nested-key-null-strategy", "error"}}));
    ASSERT_NOK(error_agg->Retract(acc, Rows({KeyedRow(9, 9, "X", 0, 0)})));
}

// Ported from Java FieldAggregatorRetractNullTest: retraction is supported and returns a value.
TEST(FieldNestedUpdateAggTest, RetractOnEmptyArraysIsSupported) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldNestedUpdateAgg> agg, MakeKeyedAgg({}));
    ASSERT_OK_AND_ASSIGN(VariantType result, agg->Retract(Rows({}), Rows({})));
    ASSERT_EQ(0, GetRows(result)->Size());
}

}  // namespace paimon::test
