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

#include "paimon/common/utils/var_length_int_utils.h"

#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(VarLengthIntUtilsTest, TestEncodeAndDecodeInt) {
    std::vector<int32_t> test_values = {0,    1,     127,    128,     255,      256,
                                        1000, 10000, 100000, 1000000, 10000000, 2147483647};

    for (int32_t value : test_values) {
        // Encode
        auto buffer =
            std::make_shared<Bytes>(VarLengthIntUtils::kMaxVarIntSize, GetDefaultPool().get());
        ASSERT_OK_AND_ASSIGN(int32_t encoded_length,
                             VarLengthIntUtils::EncodeInt(0, value, buffer.get()));

        // Decode
        int32_t offset = 0;
        ASSERT_OK_AND_ASSIGN(int32_t decoded_value,
                             VarLengthIntUtils::DecodeInt(buffer.get(), &offset));

        ASSERT_EQ(value, decoded_value) << "Encoded and decoded values don't match for: " << value;
        ASSERT_EQ(offset, encoded_length) << "Offset doesn't match encoded length for: " << value;
    }
}

TEST(VarLengthIntUtilsTest, TestEncodeAndDecodeLong) {
    std::vector<int64_t> test_values = {0ll,
                                        127ll,
                                        128ll,
                                        16383ll,
                                        16384ll,
                                        2097151ll,
                                        2097152ll,
                                        268435455ll,
                                        268435456ll,
                                        1234567890123456789ll,
                                        202405170000000000ll,
                                        999999999999999999ll,
                                        std::numeric_limits<int64_t>::max()};

    for (int64_t value : test_values) {
        // Encode
        auto buffer =
            std::make_shared<Bytes>(VarLengthIntUtils::kMaxVarLongSize, GetDefaultPool().get());
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] int32_t encoded_length,
                             VarLengthIntUtils::EncodeLong(value, buffer.get()));

        // Decode
        ASSERT_OK_AND_ASSIGN(int64_t decoded_value,
                             VarLengthIntUtils::DecodeLong(buffer.get(), /*index=*/0));

        ASSERT_EQ(value, decoded_value) << "Encoded and decoded values don't match for: " << value;
    }
}

TEST(VarLengthIntUtilsTest, TestEncodeNegativeValue) {
    auto buffer =
        std::make_shared<Bytes>(VarLengthIntUtils::kMaxVarIntSize, GetDefaultPool().get());
    ASSERT_NOK_WITH_MSG(VarLengthIntUtils::EncodeInt(0, -1, buffer.get()),
                        "negative value: v=-1 for VarLengthInt Encoding");
}

TEST(VarLengthIntUtilsTest, TestEncodeNegativeLongValue) {
    auto buffer =
        std::make_shared<Bytes>(VarLengthIntUtils::kMaxVarLongSize, GetDefaultPool().get());
    ASSERT_NOK_WITH_MSG(VarLengthIntUtils::EncodeLong(-1, buffer.get()),
                        "negative value: v=-1 for VarLengthInt Encoding");
}

TEST(VarLengthIntUtilsTest, TestEncodeIntWithOffset) {
    auto buffer =
        std::make_shared<Bytes>(VarLengthIntUtils::kMaxVarIntSize * 2, GetDefaultPool().get());
    int32_t value1 = 100;
    int32_t value2 = 200;

    // Encode first value at offset 0
    ASSERT_OK_AND_ASSIGN(auto length1, VarLengthIntUtils::EncodeInt(0, value1, buffer.get()));
    // Encode second value at offset length1
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] auto length2,
                         VarLengthIntUtils::EncodeInt(length1, value2, buffer.get()));

    // Decode both values
    int32_t offset = 0;
    ASSERT_OK_AND_ASSIGN(int32_t decoded_result1,
                         VarLengthIntUtils::DecodeInt(buffer.get(), &offset));
    ASSERT_EQ(value1, decoded_result1);
    ASSERT_OK_AND_ASSIGN(int32_t decoded_result2,
                         VarLengthIntUtils::DecodeInt(buffer.get(), &offset));
    ASSERT_EQ(value2, decoded_result2);
}

TEST(VarLengthIntUtilsTest, TestEncodeBytesNumber) {
    std::vector<int32_t> values = {
        0x7F,       // 127 - fits in 1 byte
        0x80,       // 128 - needs 2 bytes
        0x4000,     // 16384 - needs 3 bytes
        0x200000,   // 2097152 - needs 4 bytes
        2147483647  // 2147483647 - needs 5 bytes
    };

    for (int32_t i = 0; i < static_cast<int32_t>(values.size()); ++i) {
        auto buffer =
            std::make_shared<Bytes>(VarLengthIntUtils::kMaxVarIntSize, GetDefaultPool().get());
        ASSERT_OK_AND_ASSIGN(int32_t encoded_length,
                             VarLengthIntUtils::EncodeInt(0, values[i], buffer.get()));
        ASSERT_EQ(encoded_length, i + 1);
    }
}

TEST(VarLengthIntUtilsTest, TestEncodeLongBytesNumber) {
    std::vector<int64_t> values = {
        0x7F,                                // 127 - fits in 1 byte
        0x80,                                // 128 - needs 2 bytes
        0x4000,                              // 16384 - needs 3 bytes
        0x200000,                            // 2097152 - needs 4 bytes
        2147483647,                          // 2147483647 - needs 5 bytes
        34359738368ll,                       // 0x800000000 - needs 6 bytes
        562949953421311ll,                   // 0x1FFFFFFFFFFFFF - needs 7 bytes
        72057594037927935ll,                 // 0xFFFFFFFFFFFFFF - needs 8 bytes
        std::numeric_limits<int64_t>::max()  // needs 9 bytes
    };

    for (int32_t i = 0; i < static_cast<int32_t>(values.size()); ++i) {
        auto buffer =
            std::make_shared<Bytes>(VarLengthIntUtils::kMaxVarLongSize, GetDefaultPool().get());
        ASSERT_OK_AND_ASSIGN(int32_t encoded_length,
                             VarLengthIntUtils::EncodeLong(values[i], buffer.get()));
        ASSERT_EQ(encoded_length, i + 1) << values[i];
    }
}
}  // namespace paimon::test
