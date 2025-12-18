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

#include "paimon/core/mergetree/compact/aggregate/field_sum_agg.h"

#include <cstdint>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(FieldSumAggTest, TestSimple) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldSumAgg> field_sum_agg,
                         FieldSumAgg::Create(arrow::int32()));
    auto agg_ret = field_sum_agg->Agg(5, 10);
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 15);

    ASSERT_OK_AND_ASSIGN(auto retract_ret, field_sum_agg->Retract(5, 10));
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(retract_ret), -5);
}
TEST(FieldSumAggTest, TestNull) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldSumAgg> field_sum_agg,
                         FieldSumAgg::Create(arrow::int32()));
    {
        auto agg_ret = field_sum_agg->Agg(5, NullType());
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 5);
    }
    {
        auto agg_ret = field_sum_agg->Agg(NullType(), 10);
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);
    }
    {
        auto agg_ret = field_sum_agg->Agg(NullType(), NullType());
        ASSERT_TRUE(DataDefine::IsVariantNull(agg_ret));
    }

    {
        ASSERT_OK_AND_ASSIGN(auto retract_ret, field_sum_agg->Retract(5, NullType()));
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(retract_ret), 5);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto retract_ret, field_sum_agg->Retract(NullType(), 10));
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(retract_ret), -10);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto retract_ret, field_sum_agg->Retract(NullType(), NullType()));
        ASSERT_TRUE(DataDefine::IsVariantNull(retract_ret));
    }
}

TEST(FieldSumAggTest, TestVariantType) {
    {
        ASSERT_OK_AND_ASSIGN(auto field_sum_agg, FieldSumAgg::Create(arrow::int8()));
        auto agg_ret = field_sum_agg->Agg(static_cast<char>(100), static_cast<char>(15));
        ASSERT_EQ(DataDefine::GetVariantValue<char>(agg_ret), 115);
        ASSERT_OK_AND_ASSIGN(auto retract_ret,
                             field_sum_agg->Retract(static_cast<char>(100), static_cast<char>(15)));
        ASSERT_EQ(DataDefine::GetVariantValue<char>(retract_ret), 85);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto field_sum_agg, FieldSumAgg::Create(arrow::int16()));
        auto agg_ret = field_sum_agg->Agg(static_cast<int16_t>(100), static_cast<int16_t>(15));
        ASSERT_EQ(DataDefine::GetVariantValue<int16_t>(agg_ret), 115);
        ASSERT_OK_AND_ASSIGN(auto retract_ret, field_sum_agg->Retract(static_cast<int16_t>(100),
                                                                      static_cast<int16_t>(15)));
        ASSERT_EQ(DataDefine::GetVariantValue<int16_t>(retract_ret), 85);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto field_sum_agg, FieldSumAgg::Create(arrow::int32()));
        auto agg_ret = field_sum_agg->Agg(static_cast<int32_t>(100), static_cast<int32_t>(15));
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 115);
        ASSERT_OK_AND_ASSIGN(auto retract_ret, field_sum_agg->Retract(static_cast<int32_t>(100),
                                                                      static_cast<int32_t>(15)));
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(retract_ret), 85);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto field_sum_agg, FieldSumAgg::Create(arrow::int64()));
        auto agg_ret = field_sum_agg->Agg(static_cast<int64_t>(100), static_cast<int64_t>(15));
        ASSERT_EQ(DataDefine::GetVariantValue<int64_t>(agg_ret), 115);
        ASSERT_OK_AND_ASSIGN(auto retract_ret, field_sum_agg->Retract(static_cast<int64_t>(100),
                                                                      static_cast<int64_t>(15)));
        ASSERT_EQ(DataDefine::GetVariantValue<int64_t>(retract_ret), 85);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto field_sum_agg, FieldSumAgg::Create(arrow::float32()));
        auto agg_ret = field_sum_agg->Agg(static_cast<float>(100.2), static_cast<float>(15.1));
        ASSERT_NEAR(DataDefine::GetVariantValue<float>(agg_ret), 115.3, 0.0001);
        ASSERT_OK_AND_ASSIGN(auto retract_ret, field_sum_agg->Retract(static_cast<float>(100.2),
                                                                      static_cast<float>(15.1)));
        ASSERT_NEAR(DataDefine::GetVariantValue<float>(retract_ret), 85.1, 0.0001);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto field_sum_agg, FieldSumAgg::Create(arrow::float64()));
        auto agg_ret = field_sum_agg->Agg(100.23, 15.11);
        ASSERT_NEAR(DataDefine::GetVariantValue<double>(agg_ret), 115.34, 0.0001);
        ASSERT_OK_AND_ASSIGN(auto retract_ret, field_sum_agg->Retract(static_cast<double>(100.23),
                                                                      static_cast<double>(15.11)));
        ASSERT_NEAR(DataDefine::GetVariantValue<double>(retract_ret), 85.12, 0.0001);
    }
    {
        Decimal decimal1(/*precision=*/30, /*scale=*/20,
                         DecimalUtils::StrToInt128("12345678998765432145678").value());
        Decimal decimal2(/*precision=*/30, /*scale=*/20,
                         DecimalUtils::StrToInt128("2345679987639475677478").value());
        ASSERT_OK_AND_ASSIGN(auto field_sum_agg, FieldSumAgg::Create(arrow::decimal128(30, 20)));
        auto agg_ret = field_sum_agg->Agg(decimal1, decimal2);
        ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(agg_ret),
                  Decimal(/*precision=*/30, /*scale=*/20,
                          DecimalUtils::StrToInt128("14691358986404907823156").value()));
        ASSERT_OK_AND_ASSIGN(auto retract_ret, field_sum_agg->Retract(decimal1, decimal2));
        ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(retract_ret),
                  Decimal(/*precision=*/30, /*scale=*/20,
                          DecimalUtils::StrToInt128("9999999011125956468200").value()));
    }
}

TEST(FieldSumAggTest, TestInvalidType) {
    auto field_sum_agg = FieldSumAgg::Create(arrow::boolean());
    ASSERT_FALSE(field_sum_agg.ok());
    ASSERT_TRUE(field_sum_agg.status().ToString().find("type bool not support in FieldSumAgg") !=
                std::string::npos)
        << field_sum_agg.status().ToString();
}
}  // namespace paimon::test
