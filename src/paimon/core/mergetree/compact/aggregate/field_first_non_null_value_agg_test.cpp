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

#include "paimon/core/mergetree/compact/aggregate/field_first_non_null_value_agg.h"

#include <cstdint>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace paimon::test {
TEST(FieldFirstNonNullValueAggTest, TestSimple) {
    auto agg = std::make_unique<FieldFirstNonNullValueAgg>(arrow::int32(), GetDefaultPool());

    auto agg_ret = agg->Agg(5, 10).value();
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);
    agg_ret = agg->Agg(10, 20).value();
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);
    agg_ret = agg->Agg(10, 30).value();
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);

    agg->Reset();
    agg_ret = agg->Agg(10, 30).value();
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 30);

    auto retract_ret = agg->Retract(10, 30);
    ASSERT_FALSE(retract_ret.ok());
}

TEST(FieldFirstNonNullValueAggTest, TestNull) {
    auto agg = std::make_unique<FieldFirstNonNullValueAgg>(arrow::int32(), GetDefaultPool());
    auto agg_ret = agg->Agg(5, NullType()).value();
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 5);
    agg_ret = agg->Agg(5, 10).value();
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);
    agg_ret = agg->Agg(10, NullType()).value();
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);
    agg_ret = agg->Agg(10, 20).value();
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);

    agg->Reset();

    agg_ret = agg->Agg(NullType(), NullType()).value();
    ASSERT_TRUE(DataDefine::IsVariantNull(agg_ret));

    agg->Reset();

    agg_ret = agg->Agg(NullType(), 5).value();
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 5);
    agg_ret = agg->Agg(5, NullType()).value();
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 5);
}

}  // namespace paimon::test
