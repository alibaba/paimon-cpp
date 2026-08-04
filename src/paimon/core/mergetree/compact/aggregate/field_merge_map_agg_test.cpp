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

#include "paimon/core/mergetree/compact/aggregate/field_merge_map_agg.h"

#include <memory>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/generic_array.h"
#include "paimon/common/data/generic_map.h"
#include "paimon/common/data/serializer/binary_serializer_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

VariantType IntMap(std::vector<VariantType> keys, std::vector<VariantType> values) {
    std::shared_ptr<InternalArray> key_array = std::make_shared<GenericArray>(std::move(keys));
    std::shared_ptr<InternalArray> value_array = std::make_shared<GenericArray>(std::move(values));
    return VariantType(std::static_pointer_cast<InternalMap>(
        std::make_shared<GenericMap>(std::move(key_array), std::move(value_array))));
}

int32_t FindValue(const VariantType& value, int32_t key) {
    auto map = DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(value);
    for (int32_t i = 0; i < map->Size(); ++i) {
        if (map->KeyArray()->GetInt(i) == key) {
            return map->ValueArray()->GetInt(i);
        }
    }
    return -1;
}

}  // namespace

TEST(FieldMergeMapAggTest, InputOverwritesAndRetractUsesKeys) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldMergeMapAgg> agg,
                         FieldMergeMapAgg::Create(arrow::map(arrow::int32(), arrow::int32()), "f",
                                                  GetDefaultPool()));

    ASSERT_OK_AND_ASSIGN(VariantType merged,
                         agg->Agg(IntMap({int32_t{1}, int32_t{2}}, {int32_t{10}, int32_t{20}}),
                                  IntMap({int32_t{2}, int32_t{3}}, {int32_t{200}, int32_t{30}})));
    auto merged_map = DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(merged);
    ASSERT_EQ(3, merged_map->Size());
    ASSERT_EQ(10, FindValue(merged, 1));
    ASSERT_EQ(200, FindValue(merged, 2));
    ASSERT_EQ(30, FindValue(merged, 3));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BinaryMap> binary_merged,
        BinarySerializerUtils::WriteBinaryMap(
            merged_map, arrow::map(arrow::int32(), arrow::int32()), GetDefaultPool().get()));
    ASSERT_EQ(3, binary_merged->Size());

    ASSERT_OK_AND_ASSIGN(VariantType retracted,
                         agg->Retract(merged, IntMap({int32_t{2}}, {int32_t{-999}})));
    auto retracted_map = DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(retracted);
    ASSERT_EQ(2, retracted_map->Size());
    ASSERT_EQ(-1, FindValue(retracted, 2));
}

TEST(FieldMergeMapAggTest, NullAndTypeValidation) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldMergeMapAgg> agg,
                         FieldMergeMapAgg::Create(arrow::map(arrow::int32(), arrow::int32()), "f",
                                                  GetDefaultPool()));
    VariantType map = IntMap({int32_t{1}}, {int32_t{10}});
    ASSERT_OK_AND_ASSIGN(VariantType result, agg->Agg(VariantType(NullType()), map));
    ASSERT_EQ(10, FindValue(result, 1));
    ASSERT_NOK(FieldMergeMapAgg::Create(arrow::int32(), "f", GetDefaultPool()));
}

// Ported from Java FieldAggregatorRetractNullTest: retraction is supported and returns a value.
TEST(FieldMergeMapAggTest, RetractOnEmptyMapIsSupported) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldMergeMapAgg> agg,
                         FieldMergeMapAgg::Create(arrow::map(arrow::int32(), arrow::int32()), "f",
                                                  GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(VariantType result, agg->Retract(IntMap({}, {}), IntMap({}, {})));
    ASSERT_EQ(0, DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(result)->Size());
}

}  // namespace paimon::test
