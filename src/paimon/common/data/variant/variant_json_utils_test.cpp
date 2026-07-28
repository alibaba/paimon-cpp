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
#include <string>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

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

TEST(VariantJsonUtilsTest, AppendEscapedJsonControlChars) {
    // Control characters without a named escape are emitted as \uXXXX.
    std::string out;
    VariantJsonUtils::AppendEscapedJson(std::string_view("\x01\x1f", 2), &out);
    EXPECT_EQ(out, "\"\\u0001\\u001f\"");
}

TEST(VariantJsonUtilsTest, DateToStringNegativeYear) {
    // A date far before the epoch renders a negative (BCE) year with a leading '-'.
    std::string result = VariantJsonUtils::DateToString(-800000);
    ASSERT_FALSE(result.empty());
    EXPECT_EQ(result.front(), '-');
}

TEST(VariantJsonUtilsTest, TimestampToStringEdgeCases) {
    // Sub-second fraction trailing zeros are trimmed.
    EXPECT_EQ(VariantJsonUtils::TimestampToString(500000, 0, /*with_offset=*/false),
              "1970-01-01 00:00:00.5");
    // Negative epoch micros floor to the previous day (FloorDiv keeps a non-negative remainder).
    EXPECT_EQ(VariantJsonUtils::TimestampToString(-1, 0, /*with_offset=*/false),
              "1969-12-31 23:59:59.999999");
    // A positive zone offset is appended as +HH:MM.
    EXPECT_EQ(VariantJsonUtils::TimestampToString(0, 8 * 3600, /*with_offset=*/true),
              "1970-01-01 08:00:00+08:00");
}

TEST(VariantJsonUtilsTest, ZoneOffsetParsing) {
    // `UTC`/`GMT`/`UT` prefixes are stripped before the fixed offset is parsed.
    ASSERT_OK_AND_ASSIGN(int32_t utc_prefixed,
                         VariantJsonUtils::GetZoneOffsetSeconds("UTC+08:00", 0));
    EXPECT_EQ(utc_prefixed, 8 * 3600);
    ASSERT_OK_AND_ASSIGN(int32_t ut_prefixed, VariantJsonUtils::GetZoneOffsetSeconds("UT+05", 0));
    EXPECT_EQ(ut_prefixed, 5 * 3600);
    // HHMMSS form and a negative offset.
    ASSERT_OK_AND_ASSIGN(int32_t hms, VariantJsonUtils::GetZoneOffsetSeconds("+18:30:15", 0));
    EXPECT_EQ(hms, 18 * 3600 + 30 * 60 + 15);
    ASSERT_OK_AND_ASSIGN(int32_t neg, VariantJsonUtils::GetZoneOffsetSeconds("-06:30", 0));
    EXPECT_EQ(neg, -(6 * 3600 + 30 * 60));
    // Invalid forms are rejected: a non-digit char, a wrong digit count, and an out-of-range hour.
    ASSERT_NOK(VariantJsonUtils::GetZoneOffsetSeconds("+9A", 0));
    ASSERT_NOK(VariantJsonUtils::GetZoneOffsetSeconds("+123", 0));
    ASSERT_NOK(VariantJsonUtils::GetZoneOffsetSeconds("+19:00", 0));
}

}  // namespace paimon::test
