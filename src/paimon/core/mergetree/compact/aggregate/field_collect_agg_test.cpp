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

#include <memory>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/generic_array.h"
#include "paimon/common/data/serializer/binary_serializer_utils.h"
#include "paimon/core/core_options.h"
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
    return FieldCollectAgg::Create(arrow::list(arrow::int32()), options, "f");
}

}  // namespace

TEST(FieldCollectAggTest, ConcatenatesWithoutReversing) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg, MakeCollectAgg(false));
    VariantType left = IntArray({int32_t{1}, int32_t{2}});
    VariantType right = IntArray({int32_t{3}, int32_t{4}});

    ASSERT_OK_AND_ASSIGN(VariantType result, agg->AggReversedResult(left, right));
    ASSERT_EQ((std::vector<int32_t>{1, 2, 3, 4}), Values(result));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BinaryArray> binary_result,
                         BinarySerializerUtils::WriteBinaryArray(
                             DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(result),
                             arrow::list(arrow::int32()), GetDefaultPool().get()));
    ASSERT_EQ((std::vector<int32_t>{1, 2, 3, 4}), binary_result->ToIntArray().value());

    ASSERT_OK_AND_ASSIGN(VariantType null_result,
                         agg->AggResult(VariantType(NullType()), VariantType(NullType())));
    ASSERT_TRUE(DataDefine::IsVariantNull(null_result));
}

TEST(FieldCollectAggTest, DistinctAndRetractOneOccurrence) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> distinct_agg, MakeCollectAgg(true));
    ASSERT_OK_AND_ASSIGN(VariantType distinct_result,
                         distinct_agg->AggResult(IntArray({int32_t{1}, int32_t{2}, int32_t{2}}),
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
    ASSERT_NOK(FieldCollectAgg::Create(arrow::int32(), options, "f"));
}

}  // namespace paimon::test
