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

#include "paimon/core/mergetree/compact/aggregate/field_collect_agg.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/generic_array.h"
#include "paimon/common/data/generic_map.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/data/serializer/binary_serializer_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

VariantType IntArray(std::vector<VariantType> values) {
    return VariantType(
        std::static_pointer_cast<InternalArray>(std::make_shared<GenericArray>(std::move(values))));
}

std::vector<int32_t> Values(const VariantType& value) {
    auto array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(value);
    std::vector<int32_t> values;
    for (int32_t i = 0; i < array->Size(); ++i) {
        values.push_back(array->GetInt(i));
    }
    return values;
}

Result<std::unique_ptr<FieldCollectAgg>> MakeCollectAgg(bool distinct) {
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions options,
        CoreOptions::FromMap({{"fields.f.distinct", distinct ? "true" : "false"}}));
    return FieldCollectAgg::Create(arrow::list(arrow::int32()), options, "f", GetDefaultPool());
}

}  // namespace

TEST(FieldCollectAggTest, ConcatenatesWithoutReversing) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg, MakeCollectAgg(false));
    VariantType left = IntArray({int32_t{1}, int32_t{2}});
    VariantType right = IntArray({int32_t{3}, int32_t{4}});

    ASSERT_OK_AND_ASSIGN(VariantType result, agg->AggReversed(left, right));
    ASSERT_EQ((std::vector<int32_t>{1, 2, 3, 4}), Values(result));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BinaryArray> binary_result,
                         BinarySerializerUtils::WriteBinaryArray(
                             DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(result),
                             arrow::list(arrow::int32()), GetDefaultPool().get()));
    ASSERT_EQ((std::vector<int32_t>{1, 2, 3, 4}), binary_result->ToIntArray().value());

    ASSERT_OK_AND_ASSIGN(VariantType null_result,
                         agg->Agg(VariantType(NullType()), VariantType(NullType())));
    ASSERT_TRUE(DataDefine::IsVariantNull(null_result));
}

TEST(FieldCollectAggTest, DistinctAndRetractOneOccurrence) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> distinct_agg, MakeCollectAgg(true));
    ASSERT_OK_AND_ASSIGN(VariantType distinct_result,
                         distinct_agg->Agg(IntArray({int32_t{1}, int32_t{2}, int32_t{2}}),
                                           IntArray({int32_t{2}, int32_t{3}})));
    ASSERT_EQ((std::vector<int32_t>{1, 2, 3}), Values(distinct_result));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg, MakeCollectAgg(false));
    ASSERT_OK_AND_ASSIGN(VariantType retract_result,
                         agg->Retract(IntArray({int32_t{1}, int32_t{2}, int32_t{2}, int32_t{3}}),
                                      IntArray({int32_t{2}})));
    ASSERT_EQ((std::vector<int32_t>{1, 2, 3}), Values(retract_result));
}

TEST(FieldCollectAggTest, RejectsNonArrayType) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
    ASSERT_NOK(FieldCollectAgg::Create(arrow::int32(), options, "f", GetDefaultPool()));
}

// Ported from Java FieldAggregatorTest#testFiledCollectAggWith{Row,Array,Map}Type: distinct
// collection over composite element types.
namespace {

Result<std::unique_ptr<FieldCollectAgg>> MakeDistinctAgg(
    const std::shared_ptr<arrow::DataType>& element_type) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options,
                           CoreOptions::FromMap({{"fields.f.distinct", "true"}}));
    return FieldCollectAgg::Create(arrow::list(element_type), options, "f", GetDefaultPool());
}

VariantType Array(std::vector<VariantType> values) {
    return VariantType(
        std::static_pointer_cast<InternalArray>(std::make_shared<GenericArray>(std::move(values))));
}

VariantType IntStringRow(int32_t id, std::string_view name) {
    std::shared_ptr<GenericRow> row = std::make_shared<GenericRow>(2);
    row->SetField(0, id);
    row->SetField(1, name);
    return VariantType(std::static_pointer_cast<InternalRow>(row));
}

VariantType IntStringMap(std::vector<std::pair<int32_t, std::string_view>> entries) {
    std::vector<VariantType> keys;
    std::vector<VariantType> values;
    for (const auto& entry : entries) {
        keys.emplace_back(entry.first);
        values.emplace_back(entry.second);
    }
    return VariantType(std::static_pointer_cast<InternalMap>(
        std::make_shared<GenericMap>(std::make_shared<GenericArray>(std::move(keys)),
                                     std::make_shared<GenericArray>(std::move(values)))));
}

/// Decode without going through FieldAggregateUtils, so the assertions stay independent of the
/// equality code under test.
std::vector<std::string> SortedRows(const VariantType& result) {
    auto array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(result);
    std::vector<std::string> out;
    for (int32_t i = 0; i < array->Size(); ++i) {
        std::shared_ptr<InternalRow> row = array->GetRow(i, 2);
        out.push_back(std::to_string(row->GetInt(0)) + ":" + std::string(row->GetStringView(1)));
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<std::string> SortedArrays(const VariantType& result) {
    auto array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(result);
    std::vector<std::string> out;
    for (int32_t i = 0; i < array->Size(); ++i) {
        std::shared_ptr<InternalArray> inner = array->GetArray(i);
        std::string encoded;
        for (int32_t j = 0; j < inner->Size(); ++j) {
            encoded += std::to_string(inner->GetInt(j)) + ",";
        }
        out.push_back(encoded);
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<std::string> SortedMaps(const VariantType& result) {
    auto array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(result);
    std::vector<std::string> out;
    for (int32_t i = 0; i < array->Size(); ++i) {
        std::shared_ptr<InternalMap> map = array->GetMap(i);
        std::shared_ptr<InternalArray> keys = map->KeyArray();
        std::shared_ptr<InternalArray> values = map->ValueArray();
        std::vector<std::string> entries;
        for (int32_t j = 0; j < map->Size(); ++j) {
            entries.push_back(std::to_string(keys->GetInt(j)) + "=" +
                              std::string(values->GetStringView(j)));
        }
        std::sort(entries.begin(), entries.end());
        std::string encoded;
        for (const std::string& entry : entries) {
            encoded += entry + ";";
        }
        out.push_back(encoded);
    }
    std::sort(out.begin(), out.end());
    return out;
}

}  // namespace

TEST(FieldCollectAggTest, DistinctOverRowElements) {
    std::shared_ptr<arrow::DataType> row_type =
        arrow::struct_({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg, MakeDistinctAgg(row_type));

    ASSERT_OK_AND_ASSIGN(VariantType empty,
                         agg->Agg(VariantType(NullType()), VariantType(NullType())));
    ASSERT_TRUE(DataDefine::IsVariantNull(empty));

    VariantType input1 = Array({IntStringRow(1, "A"), IntStringRow(1, "B")});
    ASSERT_OK_AND_ASSIGN(VariantType first, agg->Agg(VariantType(NullType()), input1));
    ASSERT_EQ((std::vector<std::string>{"1:A", "1:B"}), SortedRows(first));

    VariantType input2 = Array({IntStringRow(1, "A"), IntStringRow(2, "A")});
    ASSERT_OK_AND_ASSIGN(VariantType merged, agg->Agg(input1, input2));
    ASSERT_EQ((std::vector<std::string>{"1:A", "1:B", "2:A"}), SortedRows(merged));
}

TEST(FieldCollectAggTest, DistinctOverArrayElements) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg,
                         MakeDistinctAgg(arrow::list(arrow::int32())));

    ASSERT_OK_AND_ASSIGN(VariantType empty,
                         agg->Agg(VariantType(NullType()), VariantType(NullType())));
    ASSERT_TRUE(DataDefine::IsVariantNull(empty));

    VariantType input1 = Array({Array({int32_t{1}, int32_t{1}}), Array({int32_t{1}, int32_t{2}})});
    ASSERT_OK_AND_ASSIGN(VariantType first, agg->Agg(VariantType(NullType()), input1));
    ASSERT_EQ((std::vector<std::string>{"1,1,", "1,2,"}), SortedArrays(first));

    VariantType input2 = Array({Array({int32_t{1}, int32_t{1}}), Array({int32_t{1}, int32_t{2}}),
                                Array({int32_t{2}, int32_t{1}})});
    ASSERT_OK_AND_ASSIGN(VariantType merged, agg->Agg(input1, input2));
    ASSERT_EQ((std::vector<std::string>{"1,1,", "1,2,", "2,1,"}), SortedArrays(merged));
}

TEST(FieldCollectAggTest, DistinctOverMapElements) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg,
                         MakeDistinctAgg(arrow::map(arrow::int32(), arrow::utf8())));

    ASSERT_OK_AND_ASSIGN(VariantType empty,
                         agg->Agg(VariantType(NullType()), VariantType(NullType())));
    ASSERT_TRUE(DataDefine::IsVariantNull(empty));

    VariantType input1 = Array({IntStringMap({{1, "A"}}), IntStringMap({{1, "A"}, {2, "B"}})});
    ASSERT_OK_AND_ASSIGN(VariantType first, agg->Agg(VariantType(NullType()), input1));
    ASSERT_EQ((std::vector<std::string>{"1=A;", "1=A;2=B;"}), SortedMaps(first));

    // the second entry has the same content as input1's, only inserted in a different order
    VariantType input2 = Array(
        {IntStringMap({{1, "A"}}), IntStringMap({{2, "B"}, {1, "A"}}), IntStringMap({{1, "C"}})});
    ASSERT_OK_AND_ASSIGN(VariantType merged, agg->Agg(input1, input2));
    ASSERT_EQ((std::vector<std::string>{"1=A;", "1=A;2=B;", "1=C;"}), SortedMaps(merged));
}

// Ported from Java FieldAggregatorTest#testFieldCollectAggRetractWith{,out}Distinct: retraction
// removes one occurrence per retracted element, for every element type.
TEST(FieldCollectAggTest, RetractRemovesOneOccurrencePerElement) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> int_agg, MakeCollectAgg(true));
    ASSERT_OK_AND_ASSIGN(
        VariantType ints,
        int_agg->Retract(IntArray({int32_t{1}, int32_t{2}, int32_t{3}}), IntArray({int32_t{1}})));
    ASSERT_EQ((std::vector<int32_t>{2, 3}), Values(ints));
    // duplicates in the accumulator are retracted one at a time
    ASSERT_OK_AND_ASSIGN(
        VariantType dups,
        int_agg->Retract(IntArray({int32_t{1}, int32_t{1}, int32_t{2}, int32_t{2}, int32_t{3}}),
                         IntArray({int32_t{1}, int32_t{2}, int32_t{3}})));
    ASSERT_EQ((std::vector<int32_t>{1, 2}), Values(dups));

    std::shared_ptr<arrow::DataType> row_type =
        arrow::struct_({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> row_agg, MakeDistinctAgg(row_type));
    ASSERT_OK_AND_ASSIGN(VariantType rows,
                         row_agg->Retract(Array({IntStringRow(1, "A"), IntStringRow(1, "A"),
                                                 IntStringRow(1, "B"), IntStringRow(2, "B")}),
                                          Array({IntStringRow(1, "A"), IntStringRow(2, "B")})));
    ASSERT_EQ((std::vector<std::string>{"1:A", "1:B"}), SortedRows(rows));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> array_agg,
                         MakeDistinctAgg(arrow::list(arrow::int32())));
    ASSERT_OK_AND_ASSIGN(
        VariantType arrays,
        array_agg->Retract(
            Array({Array({int32_t{1}, int32_t{1}}), Array({int32_t{1}, int32_t{1}}),
                   Array({int32_t{1}, int32_t{2}}), Array({int32_t{2}, int32_t{1}})}),
            Array({Array({int32_t{1}, int32_t{1}}), Array({int32_t{1}, int32_t{2}})})));
    ASSERT_EQ((std::vector<std::string>{"1,1,", "2,1,"}), SortedArrays(arrays));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> map_agg,
                         MakeDistinctAgg(arrow::map(arrow::int32(), arrow::utf8())));
    // the retracted {1=A,2=B} matches the accumulator entry written as {2=B,1=A}
    ASSERT_OK_AND_ASSIGN(
        VariantType maps,
        map_agg->Retract(Array({IntStringMap({{1, "A"}}), IntStringMap({{1, "A"}}),
                                IntStringMap({{2, "B"}, {1, "A"}}), IntStringMap({{1, "C"}})}),
                         Array({IntStringMap({{1, "A"}}), IntStringMap({{1, "A"}, {2, "B"}})})));
    ASSERT_EQ((std::vector<std::string>{"1=A;", "1=C;"}), SortedMaps(maps));
}

// Ported from Java FieldAggregatorRetractNullTest: retraction is supported and returns a value.
TEST(FieldCollectAggTest, RetractOnEmptyArraysIsSupported) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg, MakeCollectAgg(false));
    ASSERT_OK_AND_ASSIGN(VariantType result, agg->Retract(IntArray({}), IntArray({})));
    ASSERT_TRUE(Values(result).empty());
}

}  // namespace paimon::test
