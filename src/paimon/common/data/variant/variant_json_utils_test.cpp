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

#include "paimon/common/data/variant/variant_json_utils.h"

#include <cmath>
#include <limits>

#include "gtest/gtest.h"

namespace paimon::test {

TEST(VariantJsonUtilsTest, JavaDoubleToString) {
    // Mirrors java.lang.Double#toString exactly.
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(0.0), "0.0");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(-0.0), "-0.0");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(std::numeric_limits<double>::infinity()),
              "Infinity");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(-std::numeric_limits<double>::infinity()),
              "-Infinity");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(std::nan("")), "NaN");
    // Double.MIN_VALUE (the smallest positive subnormal) and Double.MAX_VALUE.
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(std::numeric_limits<double>::denorm_min()),
              "4.9E-324");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(std::numeric_limits<double>::max()),
              "1.7976931348623157E308");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(std::numeric_limits<double>::min()),
              "2.2250738585072014E-308");

    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(1.0), "1.0");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(-1.0), "-1.0");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(-0.001), "-0.001");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(1e7), "1.0E7");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(1234567.0), "1234567.0");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(0.001), "0.001");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(1.0E-3), "0.001");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(1.0E-4), "1.0E-4");
    EXPECT_EQ(VariantJsonUtils::JavaDoubleToString(0.1 + 0.2), "0.30000000000000004");
}

TEST(VariantJsonUtilsTest, JavaFloatToString) {
    // Mirrors java.lang.Float#toString exactly.
    EXPECT_EQ(VariantJsonUtils::JavaFloatToString(0.0F), "0.0");
    EXPECT_EQ(VariantJsonUtils::JavaFloatToString(-0.0F), "-0.0");
    EXPECT_EQ(VariantJsonUtils::JavaFloatToString(std::numeric_limits<float>::infinity()),
              "Infinity");
    EXPECT_EQ(VariantJsonUtils::JavaFloatToString(-std::numeric_limits<float>::infinity()),
              "-Infinity");
    EXPECT_EQ(VariantJsonUtils::JavaFloatToString(std::nanf("")), "NaN");
    // Float.MIN_VALUE (the smallest positive subnormal) and Float.MAX_VALUE.
    EXPECT_EQ(VariantJsonUtils::JavaFloatToString(std::numeric_limits<float>::denorm_min()),
              "1.4E-45");
    EXPECT_EQ(VariantJsonUtils::JavaFloatToString(std::numeric_limits<float>::max()),
              "3.4028235E38");
    EXPECT_EQ(VariantJsonUtils::JavaFloatToString(1.0F), "1.0");
    EXPECT_EQ(VariantJsonUtils::JavaFloatToString(2.5F), "2.5");
}

}  // namespace paimon::test
