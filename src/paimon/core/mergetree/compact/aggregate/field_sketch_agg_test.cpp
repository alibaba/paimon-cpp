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

#include "paimon/core/mergetree/compact/aggregate/field_sketch_agg.h"

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "DataSketches/hll.hpp"
#include "DataSketches/theta_sketch.hpp"
#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/core/core_options.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/aggregate/aggregate_merge_function.h"
#include "paimon/defs.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

template <typename T>
VariantType Serialized(const std::vector<T>& data) {
    std::shared_ptr<Bytes> bytes =
        Bytes::AllocateBytes(data.size() * sizeof(T), GetDefaultPool().get());
    if (!data.empty()) {
        std::memcpy(bytes->data(), data.data(), bytes->size());
    }
    return VariantType(std::move(bytes));
}

VariantType Hll(std::initializer_list<int32_t> values) {
    datasketches::hll_sketch sketch(12, datasketches::HLL_4);
    for (int32_t value : values) {
        sketch.update(value);
    }
    return Serialized(sketch.serialize_compact());
}

VariantType Theta(std::initializer_list<int32_t> values) {
    datasketches::update_theta_sketch sketch = datasketches::update_theta_sketch::builder().build();
    for (int32_t value : values) {
        sketch.update(value);
    }
    return Serialized(sketch.compact(/*ordered=*/true).serialize());
}

std::shared_ptr<Bytes> HllBytes(std::initializer_list<int32_t> values) {
    return DataDefine::GetVariantValue<std::shared_ptr<Bytes>>(Hll(values));
}

std::shared_ptr<Bytes> ThetaBytes(std::initializer_list<int32_t> values) {
    return DataDefine::GetVariantValue<std::shared_ptr<Bytes>>(Theta(values));
}

// reuse freed heap blocks so a row still pointing into released memory yields corrupted bytes
std::vector<pooled_unique_ptr<Bytes>> ScribbleFreedMemory() {
    std::vector<pooled_unique_ptr<Bytes>> blocks;
    for (int32_t i = 0; i < 64; ++i) {
        pooled_unique_ptr<Bytes> block = Bytes::AllocateBytes(4096, GetDefaultPool().get());
        std::memset(block->data(), 0xAB, block->size());
        blocks.push_back(std::move(block));
    }
    return blocks;
}

}  // namespace

TEST(BinaryAggMergeFunctionTest, OwnedAccumulatorSurvivesNullInput) {
    arrow::FieldVector fields = {arrow::field("k0", arrow::int32()),
                                 arrow::field("hll", arrow::binary()),
                                 arrow::field("theta", arrow::binary())};
    ASSERT_OK_AND_ASSIGN(
        CoreOptions options,
        CoreOptions::FromMap({{"fields.hll.aggregate-function", "hll_sketch"},
                              {"fields.theta.aggregate-function", "theta_sketch"}}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<AggregateMergeFunction> merge_func,
        AggregateMergeFunction::Create(arrow::schema(fields),
                                       /*primary_keys=*/{"k0"}, options, GetDefaultPool()));

    MemoryPool* pool = GetDefaultPool().get();
    ASSERT_OK(
        merge_func->Add(KeyValue(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0,
                                 BinaryRowGenerator::GenerateRowPtr({10}, pool),
                                 BinaryRowGenerator::GenerateRowPtr(
                                     {10, HllBytes({1, 2, 3}), ThetaBytes({1, 2, 3})}, pool))));
    // both sides non-null, so each aggregator now owns a freshly allocated buffer in the row
    ASSERT_OK(
        merge_func->Add(KeyValue(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0,
                                 BinaryRowGenerator::GenerateRowPtr({10}, pool),
                                 BinaryRowGenerator::GenerateRowPtr(
                                     {10, HllBytes({3, 4, 5}), ThetaBytes({3, 4, 5})}, pool))));
    // input side is null, so the accumulator is passed through and written back into that field
    ASSERT_OK(merge_func->Add(
        KeyValue(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0,
                 BinaryRowGenerator::GenerateRowPtr({10}, pool),
                 BinaryRowGenerator::GenerateRowPtr({10, NullType(), NullType()}, pool))));

    ASSERT_OK_AND_ASSIGN(std::optional<KeyValue> result, merge_func->GetResult());
    ASSERT_TRUE(result.has_value());
    std::vector<pooled_unique_ptr<Bytes>> scribbled = ScribbleFreedMemory();

    std::string_view hll_bytes = result->value->GetStringView(1);
    ASSERT_NEAR(
        5.0,
        datasketches::hll_sketch::deserialize(hll_bytes.data(), hll_bytes.size()).get_estimate(),
        0.1);

    std::string_view theta_bytes = result->value->GetStringView(2);
    ASSERT_DOUBLE_EQ(
        5.0, datasketches::compact_theta_sketch::deserialize(theta_bytes.data(), theta_bytes.size())
                 .get_estimate());
}

TEST(FieldSketchAggTest, UnionsHllSketchesAsCompactHll4) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldHllSketchAgg> agg,
                         FieldHllSketchAgg::Create(arrow::binary(), "f", GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(VariantType result, agg->Agg(Hll({1, 2, 3}), Hll({3, 4, 5})));
    std::string_view bytes = DataDefine::GetStringView(result);
    datasketches::hll_sketch sketch =
        datasketches::hll_sketch::deserialize(bytes.data(), bytes.size());
    ASSERT_EQ(datasketches::HLL_4, sketch.get_target_type());
    ASSERT_NEAR(5.0, sketch.get_estimate(), 0.1);
}

TEST(FieldSketchAggTest, UnionsOrderedThetaSketches) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldThetaSketchAgg> agg,
                         FieldThetaSketchAgg::Create(arrow::binary(), "f", GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(VariantType result, agg->Agg(Theta({1, 2, 3}), Theta({3, 4, 5})));
    std::string_view bytes = DataDefine::GetStringView(result);
    datasketches::compact_theta_sketch sketch =
        datasketches::compact_theta_sketch::deserialize(bytes.data(), bytes.size());
    ASSERT_TRUE(sketch.is_ordered());
    ASSERT_EQ(5, sketch.get_num_retained());
    ASSERT_DOUBLE_EQ(5.0, sketch.get_estimate());
}

// AggReversed swaps its arguments, so either side may carry the row-owned accumulator
TEST(FieldSketchAggTest, NullArgumentsKeepOwnershipInBothDirections) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldHllSketchAgg> agg,
                         FieldHllSketchAgg::Create(arrow::binary(), "f", GetDefaultPool()));
    VariantType null_value = VariantType(NullType());
    // pass a view, not an owning value, or OwnedBinary short-circuits and skips the copy path
    VariantType owned = Hll({1, 2, 3});
    VariantType sketch = VariantType(DataDefine::GetStringView(owned));

    for (const VariantType& result :
         {agg->Agg(sketch, null_value).value(), agg->Agg(null_value, sketch).value(),
          agg->AggReversed(sketch, null_value).value(),
          agg->AggReversed(null_value, sketch).value()}) {
        ASSERT_TRUE(DataDefine::GetVariantPtr<std::shared_ptr<Bytes>>(result))
            << "the surviving value must own its buffer";
        std::string_view bytes = DataDefine::GetStringView(result);
        ASSERT_NEAR(
            3.0, datasketches::hll_sketch::deserialize(bytes.data(), bytes.size()).get_estimate(),
            0.1);
    }

    ASSERT_OK_AND_ASSIGN(VariantType both_null, agg->Agg(null_value, null_value));
    ASSERT_TRUE(DataDefine::IsVariantNull(both_null));
    ASSERT_OK_AND_ASSIGN(VariantType both_null_reversed, agg->AggReversed(null_value, null_value));
    ASSERT_TRUE(DataDefine::IsVariantNull(both_null_reversed));
}

// Java leaves retract unimplemented for the sketch aggregators, so it must surface as an error
// pointing at fields.<f>.ignore-retract rather than silently succeeding.
TEST(FieldSketchAggTest, RetractionIsUnsupported) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldHllSketchAgg> hll_agg,
                         FieldHllSketchAgg::Create(arrow::binary(), "f", GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldThetaSketchAgg> theta_agg,
                         FieldThetaSketchAgg::Create(arrow::binary(), "f", GetDefaultPool()));
    ASSERT_NOK_WITH_MSG(hll_agg->Retract(Hll({1}), Hll({1})), "does not support retraction");
    ASSERT_NOK_WITH_MSG(theta_agg->Retract(Theta({1}), Theta({1})), "does not support retraction");
}

TEST(FieldSketchAggTest, ReportsInvalidBytesAndTypes) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldHllSketchAgg> hll_agg,
                         FieldHllSketchAgg::Create(arrow::binary(), "f", GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldThetaSketchAgg> theta_agg,
                         FieldThetaSketchAgg::Create(arrow::binary(), "f", GetDefaultPool()));
    VariantType invalid = VariantType(std::string_view("bad"));
    ASSERT_NOK(hll_agg->Agg(invalid, Hll({1})));
    ASSERT_NOK(hll_agg->AggReversed(invalid, Hll({1})));
    ASSERT_NOK(theta_agg->Agg(invalid, Theta({1})));
    ASSERT_NOK(FieldHllSketchAgg::Create(arrow::int32(), "f", GetDefaultPool()));
    ASSERT_NOK(FieldThetaSketchAgg::Create(arrow::int32(), "f", GetDefaultPool()));
}

}  // namespace paimon::test
