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

#include "paimon/common/utils/math.h"

#include <limits>

#include "gtest/gtest.h"

namespace paimon::test {

// Test case: Test EndianSwapValue for different integral types
TEST(MathTest, EndianSwapValue) {
    // Test 16-bit value
    uint16_t value16 = 0x1234;
    uint16_t swapped16 = EndianSwapValue(value16);
    ASSERT_EQ(swapped16, 0x3412);

    // Test 32-bit value
    uint32_t value32 = 0x12345678;
    uint32_t swapped32 = EndianSwapValue(value32);
    ASSERT_EQ(swapped32, 0x78563412);

    // Test 64-bit value
    uint64_t value64 = 0x123456789ABCDEF0;
    uint64_t swapped64 = EndianSwapValue(value64);
    ASSERT_EQ(swapped64, 0xF0DEBC9A78563412);
}

TEST(MathTest, InRange) {
    // signed -> unsigned: negative values out of range, boundary values in range
    ASSERT_TRUE(InRange<uint32_t>(0));
    ASSERT_TRUE(InRange<uint32_t>(std::numeric_limits<int32_t>::max()));
    ASSERT_FALSE(InRange<uint32_t>(std::numeric_limits<int32_t>::lowest()));
    ASSERT_FALSE(InRange<uint32_t>(-1));

    // unsigned -> signed: values beyond signed max are out of range
    ASSERT_TRUE(InRange<int32_t>(static_cast<uint32_t>(0)));
    ASSERT_TRUE(InRange<int32_t>(static_cast<uint32_t>(std::numeric_limits<int32_t>::max())));
    ASSERT_FALSE(InRange<int32_t>(static_cast<uint32_t>(std::numeric_limits<int32_t>::max()) + 1U));
    ASSERT_FALSE(InRange<int32_t>(std::numeric_limits<uint32_t>::max()));

    // wider signed -> narrower signed: overflow detection
    ASSERT_TRUE(InRange<int32_t>(static_cast<int64_t>(std::numeric_limits<int32_t>::lowest())));
    ASSERT_TRUE(InRange<int32_t>(static_cast<int64_t>(std::numeric_limits<int32_t>::max())));
    ASSERT_FALSE(
        InRange<int32_t>(static_cast<int64_t>(std::numeric_limits<int32_t>::lowest()) - 1));
    ASSERT_FALSE(InRange<int32_t>(static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1));

    // wider unsigned -> narrower unsigned: overflow detection
    ASSERT_TRUE(InRange<uint32_t>(static_cast<uint64_t>(0)));
    ASSERT_TRUE(InRange<uint32_t>(static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())));
    ASSERT_FALSE(
        InRange<uint32_t>(static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1ULL));

    // mixed width: narrower -> wider always in range, wider -> narrower may overflow
    ASSERT_TRUE(InRange<int32_t>(static_cast<int16_t>(12)));
    ASSERT_FALSE(InRange<int16_t>(std::numeric_limits<int32_t>::max()));
    ASSERT_TRUE(InRange<uint32_t>(std::numeric_limits<uint32_t>::max()));
    ASSERT_FALSE(InRange<uint32_t>(static_cast<int64_t>(std::numeric_limits<uint32_t>::max()) + 1));
}

}  // namespace paimon::test
