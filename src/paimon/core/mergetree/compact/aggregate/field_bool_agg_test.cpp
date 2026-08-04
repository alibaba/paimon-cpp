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

#include <memory>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/common/data/data_define.h"
#include "paimon/core/mergetree/compact/aggregate/field_bool_and_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_bool_or_agg.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(FieldBoolAndAggTest, TestSimple) {
    ASSERT_OK_AND_ASSIGN(auto agg, FieldBoolAndAgg::Create(arrow::boolean(), GetDefaultPool()));
    auto agg_ret = agg->Agg(true, true).value();
    ASSERT_EQ(DataDefine::GetVariantValue<bool>(agg_ret), true);
    agg_ret = agg->Agg(true, false).value();
    ASSERT_EQ(DataDefine::GetVariantValue<bool>(agg_ret), false);
    agg_ret = agg->Agg(false, true).value();
    ASSERT_EQ(DataDefine::GetVariantValue<bool>(agg_ret), false);
    agg_ret = agg->Agg(false, false).value();
    ASSERT_EQ(DataDefine::GetVariantValue<bool>(agg_ret), false);

    auto retract_ret = agg->Retract(false, false);
    ASSERT_FALSE(retract_ret.ok());
}

TEST(FieldBoolAndAggTest, TestInvalidType) {
    auto agg = FieldBoolAndAgg::Create(arrow::utf8(), GetDefaultPool());
    ASSERT_FALSE(agg.ok());
}

TEST(FieldBoolAndAggTest, TestNull) {
    ASSERT_OK_AND_ASSIGN(auto agg, FieldBoolAndAgg::Create(arrow::boolean(), GetDefaultPool()));
    {
        auto agg_ret = agg->Agg(true, NullType()).value();
        ASSERT_EQ(DataDefine::GetVariantValue<bool>(agg_ret), true);
    }
    {
        auto agg_ret = agg->Agg(NullType(), true).value();
        ASSERT_EQ(DataDefine::GetVariantValue<bool>(agg_ret), true);
    }
    {
        auto agg_ret = agg->Agg(NullType(), NullType()).value();
        ASSERT_TRUE(DataDefine::IsVariantNull(agg_ret));
    }
}

TEST(FieldBoolOrAggTest, TestSimple) {
    ASSERT_OK_AND_ASSIGN(auto agg, FieldBoolOrAgg::Create(arrow::boolean(), GetDefaultPool()));
    auto agg_ret = agg->Agg(true, true).value();
    ASSERT_EQ(DataDefine::GetVariantValue<bool>(agg_ret), true);
    agg_ret = agg->Agg(true, false).value();
    ASSERT_EQ(DataDefine::GetVariantValue<bool>(agg_ret), true);
    agg_ret = agg->Agg(false, true).value();
    ASSERT_EQ(DataDefine::GetVariantValue<bool>(agg_ret), true);
    agg_ret = agg->Agg(false, false).value();
    ASSERT_EQ(DataDefine::GetVariantValue<bool>(agg_ret), false);

    auto retract_ret = agg->Retract(false, false);
    ASSERT_FALSE(retract_ret.ok());
}

TEST(FieldBoolOrAggTest, TestInvalidType) {
    auto agg = FieldBoolOrAgg::Create(arrow::utf8(), GetDefaultPool());
    ASSERT_FALSE(agg.ok());
}

TEST(FieldBoolOrAggTest, TestNull) {
    ASSERT_OK_AND_ASSIGN(auto agg, FieldBoolOrAgg::Create(arrow::boolean(), GetDefaultPool()));
    {
        auto agg_ret = agg->Agg(true, NullType()).value();
        ASSERT_EQ(DataDefine::GetVariantValue<bool>(agg_ret), true);
    }
    {
        auto agg_ret = agg->Agg(NullType(), true).value();
        ASSERT_EQ(DataDefine::GetVariantValue<bool>(agg_ret), true);
    }
    {
        auto agg_ret = agg->Agg(NullType(), NullType()).value();
        ASSERT_TRUE(DataDefine::IsVariantNull(agg_ret));
    }
}

}  // namespace paimon::test
