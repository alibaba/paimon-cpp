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

#include "paimon/core/mergetree/compact/aggregate/field_last_value_agg.h"

#include <cstdint>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(FieldLastValueAggTest, TestSimple) {
    auto agg = std::make_unique<FieldLastValueAgg>(arrow::int32());

    auto agg_ret = agg->Agg(5, 10).value();
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);

    ASSERT_OK_AND_ASSIGN(auto retract_ret, agg->Retract(5, 10));
    ASSERT_TRUE(DataDefine::IsVariantNull(retract_ret));
}

TEST(FieldLastValueAggTest, TestNull) {
    auto agg = std::make_unique<FieldLastValueAgg>(arrow::int32());
    {
        auto agg_ret = agg->Agg(5, NullType()).value();
        ASSERT_TRUE(DataDefine::IsVariantNull(agg_ret));
    }
    {
        auto agg_ret = agg->Agg(NullType(), 10).value();
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);
    }
    {
        auto agg_ret = agg->Agg(NullType(), NullType()).value();
        ASSERT_TRUE(DataDefine::IsVariantNull(agg_ret));
    }

    {
        ASSERT_OK_AND_ASSIGN(auto retract_ret, agg->Retract(5, NullType()));
        ASSERT_TRUE(DataDefine::IsVariantNull(retract_ret));
    }
    {
        ASSERT_OK_AND_ASSIGN(auto retract_ret, agg->Retract(NullType(), 10));
        ASSERT_TRUE(DataDefine::IsVariantNull(retract_ret));
    }
    {
        ASSERT_OK_AND_ASSIGN(auto retract_ret, agg->Retract(NullType(), NullType()));
        ASSERT_TRUE(DataDefine::IsVariantNull(retract_ret));
    }
}

}  // namespace paimon::test
