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

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/core/mergetree/compact/aggregate/field_max_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_min_agg.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon::test {

template <typename T>
void AssertSameFloatingPoint(T actual, T expected) {
    if (std::isnan(expected)) {
        ASSERT_TRUE(std::isnan(actual));
        return;
    }
    ASSERT_EQ(actual, expected);
    if (expected == static_cast<T>(0.0)) {
        ASSERT_EQ(std::signbit(actual), std::signbit(expected));
    }
}

template <typename T>
void CheckJavaCompatibleFloatingPointMinMax(const std::shared_ptr<arrow::DataType>& type) {
    const T infinity = std::numeric_limits<T>::infinity();
    const T nan = std::numeric_limits<T>::quiet_NaN();
    // This is the total order defined by Java Float.compare and Double.compare.
    const std::array<T, 5> values = {-infinity, -static_cast<T>(0.0), static_cast<T>(0.0), infinity,
                                     nan};

    ASSERT_OK_AND_ASSIGN(auto field_min_agg, FieldMinAgg::Create(type));
    ASSERT_OK_AND_ASSIGN(auto field_max_agg, FieldMaxAgg::Create(type));
    for (size_t i = 0; i < values.size(); ++i) {
        for (size_t j = 0; j < values.size(); ++j) {
            ASSERT_OK_AND_ASSIGN(VariantType min_result, field_min_agg->Agg(values[i], values[j]));
            AssertSameFloatingPoint(DataDefine::GetVariantValue<T>(min_result),
                                    values[std::min(i, j)]);

            ASSERT_OK_AND_ASSIGN(VariantType max_result, field_max_agg->Agg(values[i], values[j]));
            AssertSameFloatingPoint(DataDefine::GetVariantValue<T>(max_result),
                                    values[std::max(i, j)]);
        }
    }
}

TEST(FieldMinMaxAggTest, TestSimple) {
    {
        ASSERT_OK_AND_ASSIGN(auto field_min_agg, FieldMinAgg::Create(arrow::int32()));
        auto agg_ret = field_min_agg->Agg(5, 10).value();
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 5);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto field_max_agg, FieldMaxAgg::Create(arrow::int32()));
        auto agg_ret = field_max_agg->Agg(5, 10).value();
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);
    }
}

TEST(FieldMinMaxAggTest, TestJavaCompatibleFloatingPointOrder) {
    CheckJavaCompatibleFloatingPointMinMax<float>(arrow::float32());
    CheckJavaCompatibleFloatingPointMinMax<double>(arrow::float64());
}

TEST(FieldMinMaxAggTest, TestInvalidType) {
    auto field_min_agg = FieldMinAgg::Create(arrow::boolean());
    ASSERT_FALSE(field_min_agg.ok());
    auto field_max_agg = FieldMaxAgg::Create(arrow::boolean());
    ASSERT_FALSE(field_max_agg.ok());
}

TEST(FieldMinMaxAggTest, TestNull) {
    ASSERT_OK_AND_ASSIGN(auto field_min_agg, FieldMinAgg::Create(arrow::int32()));
    ASSERT_OK_AND_ASSIGN(auto field_max_agg, FieldMaxAgg::Create(arrow::int32()));
    {
        auto agg_ret = field_min_agg->Agg(5, NullType()).value();
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 5);
        agg_ret = field_max_agg->Agg(5, NullType()).value();
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 5);
    }
    {
        auto agg_ret = field_min_agg->Agg(NullType(), 10).value();
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);
        agg_ret = field_max_agg->Agg(NullType(), 10).value();
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);
    }
    {
        auto agg_ret = field_min_agg->Agg(NullType(), NullType()).value();
        ASSERT_TRUE(DataDefine::IsVariantNull(agg_ret));
        agg_ret = field_max_agg->Agg(NullType(), NullType()).value();
        ASSERT_TRUE(DataDefine::IsVariantNull(agg_ret));
    }
}

TEST(FieldMinMaxAggTest, TestVariantType) {
    auto CheckResult = [](const std::shared_ptr<arrow::DataType>& type, const VariantType& large,
                          const VariantType& small) {
        ASSERT_OK_AND_ASSIGN(auto field_min_agg, FieldMinAgg::Create(type));
        ASSERT_OK_AND_ASSIGN(auto field_max_agg, FieldMaxAgg::Create(type));
        auto agg_ret = field_min_agg->Agg(small, large).value();
        ASSERT_EQ(agg_ret, small);
        agg_ret = field_min_agg->Agg(large, small).value();
        ASSERT_EQ(agg_ret, small);

        agg_ret = field_max_agg->Agg(small, large).value();
        ASSERT_EQ(agg_ret, large);
        agg_ret = field_max_agg->Agg(large, small).value();
        ASSERT_EQ(agg_ret, large);
    };

    CheckResult(arrow::int8(), static_cast<char>(100), static_cast<char>(15));
    CheckResult(arrow::int16(), static_cast<int16_t>(100), static_cast<int16_t>(15));
    CheckResult(arrow::int32(), static_cast<int32_t>(100), static_cast<int32_t>(15));
    CheckResult(arrow::date32(), static_cast<int32_t>(100), static_cast<int32_t>(15));
    CheckResult(arrow::int64(), static_cast<int64_t>(100), static_cast<int64_t>(15));
    CheckResult(arrow::float32(), static_cast<float>(100.2), static_cast<float>(15.1));
    CheckResult(arrow::float64(), 100.23, 15.11);
    CheckResult(arrow::timestamp(arrow::TimeUnit::NANO),
                Timestamp(/*millisecond=*/100, /*nano_of_millisecond=*/999),
                Timestamp(/*millisecond=*/10, /*nano_of_millisecond=*/999));
    CheckResult(arrow::decimal128(30, 20),
                Decimal(/*precision=*/30, /*scale=*/20,
                        DecimalUtils::StrToInt128("12345678998765432145678").value()),
                Decimal(/*precision=*/30, /*scale=*/20,
                        DecimalUtils::StrToInt128("2345679987639475677478").value()));
    std::string str1 = "bcd";
    std::string str2 = "abc";
    CheckResult(arrow::utf8(), std::string_view(str1.data(), str1.size()),
                std::string_view(str2.data(), str2.size()));
    CheckResult(arrow::binary(), std::string_view(str1.data(), str1.size()),
                std::string_view(str2.data(), str2.size()));
}
}  // namespace paimon::test
