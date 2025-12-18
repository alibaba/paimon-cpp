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

#include "paimon/data/decimal.h"

#include <memory>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(DecimalTest, TestSimple) {
    auto CheckResult = [](const Decimal& decimal, const std::vector<uint8_t>& bytes) {
        auto pool = GetDefaultPool();
        // prepare java bytes
        auto java_bytes = Bytes::AllocateBytes(bytes.size(), pool.get());
        memcpy(java_bytes->data(), bytes.data(), bytes.size());
        // prepare result cpp bytes
        auto cpp_serialized_bytes = decimal.ToUnscaledBytes();

        // check serialized bytes equal
        ASSERT_EQ(std::vector<char>(java_bytes->data(), java_bytes->data() + java_bytes->size()),
                  cpp_serialized_bytes);
        // check deserialize equal
        auto decimal2 = Decimal::FromUnscaledBytes(38, 38, java_bytes.get());
        ASSERT_EQ(decimal, decimal2);
    };
    {
        Decimal decimal(38, 38, DecimalUtils::StrToInt128("12345678998765432145678").value());
        std::vector<uint8_t> java_bytes = {0x2, 0x9d, 0x42, 0xb6, 0xa7, 0x2a, 0x9d, 0xc7, 0x7, 0xe};
        CheckResult(decimal, java_bytes);
    }
    {
        Decimal decimal(38, 38, DecimalUtils::StrToInt128("-12345678998765432145678").value());
        std::vector<uint8_t> java_bytes = {0xfd, 0x62, 0xbd, 0x49, 0x58,
                                           0xd5, 0x62, 0x38, 0xf8, 0xf2};
        CheckResult(decimal, java_bytes);
    }
    {
        Decimal decimal(38, 38, DecimalUtils::StrToInt128("0").value());
        std::vector<uint8_t> java_bytes = {0x0};
        CheckResult(decimal, java_bytes);
    }
    {
        Decimal decimal(38, 38, DecimalUtils::StrToInt128("1").value());
        std::vector<uint8_t> java_bytes = {0x1};
        CheckResult(decimal, java_bytes);
    }
    {
        Decimal decimal(38, 38, DecimalUtils::StrToInt128("-1").value());
        std::vector<uint8_t> java_bytes = {0xff};
        CheckResult(decimal, java_bytes);
    }
    {
        Decimal decimal(38, 38, DecimalUtils::StrToInt128("128").value());
        std::vector<uint8_t> java_bytes = {0x0, 0x80};
        CheckResult(decimal, java_bytes);
    }
    {
        Decimal decimal(38, 38, Decimal::INT128_MINIMUM_VALUE);
        std::vector<uint8_t> java_bytes = {0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                           0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        CheckResult(decimal, java_bytes);
    }
    {
        Decimal decimal(38, 38, Decimal::INT128_MAXIMUM_VALUE);
        std::vector<uint8_t> java_bytes = {0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                                           0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
        CheckResult(decimal, java_bytes);
    }
}

TEST(DecimalTest, TestCompatibleWithJava) {
    auto pool = GetDefaultPool();
    auto file_system = std::make_unique<LocalFileSystem>();
    auto file_name = paimon::test::GetDataDir() + "/decimal_bytes.data";
    uint64_t file_length = file_system->GetFileStatus(file_name).value()->GetLen();
    ASSERT_GT(file_length, 0);
    ASSERT_OK_AND_ASSIGN(auto input_stream, file_system->Open(file_name));
    auto data_bytes = Bytes::AllocateBytes(file_length, pool.get());
    ASSERT_OK(input_stream->Read(data_bytes->data(), file_length));
    auto byte_array_input_stream =
        std::make_shared<ByteArrayInputStream>(data_bytes->data(), file_length);
    DataInputStream data_input_stream(byte_array_input_stream);
    for (int32_t i = 0; i < 2000; i++) {
        // read decimal str
        ASSERT_OK_AND_ASSIGN(int32_t decimal_str_len, data_input_stream.ReadValue<int32_t>());
        auto decimal_str = Bytes::AllocateBytes(decimal_str_len, pool.get());
        ASSERT_OK(data_input_stream.ReadBytes(decimal_str.get()));

        // read decimal serialized bytes from java
        ASSERT_OK_AND_ASSIGN(int32_t decimal_bytes_len, data_input_stream.ReadValue<int32_t>());
        auto decimal_bytes = Bytes::AllocateBytes(decimal_bytes_len, pool.get());
        ASSERT_OK(data_input_stream.ReadBytes(decimal_bytes.get()));

        // check result
        auto str = std::string(decimal_str->data(), decimal_str->size());
        Decimal decimal(/*precision=*/29, /*scale=*/29, DecimalUtils::StrToInt128(str).value());
        auto serialized_bytes = decimal.ToUnscaledBytes();
        ASSERT_EQ(
            std::vector<char>(decimal_bytes->data(), decimal_bytes->data() + decimal_bytes->size()),
            serialized_bytes);
        auto decimal2 =
            Decimal::FromUnscaledBytes(/*precision=*/29, /*scale=*/29, decimal_bytes.get());
        ASSERT_EQ(decimal, decimal2);
    }
}

TEST(DecimalTest, TestCompareTo) {
    auto CheckResult = [](const Decimal& decimal1, const Decimal& decimal2) {
        ASSERT_FALSE(decimal1 < decimal1);
        ASSERT_FALSE(decimal1 > decimal1);
        ASSERT_EQ(decimal1, decimal1);

        ASSERT_EQ(decimal1.CompareTo(decimal2), -1);
        ASSERT_LT(decimal1, decimal2);
        ASSERT_EQ(decimal2.CompareTo(decimal1), 1);
        ASSERT_GT(decimal2, decimal1);
        auto decimal3 = decimal1;
        ASSERT_EQ(decimal3.CompareTo(decimal1), 0);
        ASSERT_EQ(decimal3, decimal1);

        Decimal negative_decimal1(decimal1.Precision(), decimal1.Scale(), -decimal1.Value());
        Decimal negative_decimal2(decimal2.Precision(), decimal2.Scale(), -decimal2.Value());
        ASSERT_EQ(negative_decimal1.CompareTo(negative_decimal2), 1);
        ASSERT_GT(negative_decimal1, negative_decimal2);
        ASSERT_EQ(negative_decimal2.CompareTo(negative_decimal1), -1);
        ASSERT_LT(negative_decimal2, negative_decimal1);
        auto negative_decimal3 = negative_decimal1;
        ASSERT_EQ(negative_decimal3.CompareTo(negative_decimal1), 0);
        ASSERT_EQ(negative_decimal3, negative_decimal1);
    };

    auto CheckEqual = [](const Decimal& decimal1, const Decimal& decimal2) {
        ASSERT_EQ(decimal1.CompareTo(decimal2), 0);
        ASSERT_EQ(decimal2.CompareTo(decimal1), 0);

        Decimal negative_decimal1(decimal1.Precision(), decimal1.Scale(), -decimal1.Value());
        Decimal negative_decimal2(decimal2.Precision(), decimal2.Scale(), -decimal2.Value());
        ASSERT_EQ(negative_decimal1.CompareTo(negative_decimal2), 0);
        ASSERT_EQ(negative_decimal2.CompareTo(negative_decimal1), 0);
    };

    // same scales
    {
        Decimal decimal1(23, 0, DecimalUtils::StrToInt128("99").value());
        Decimal decimal2(23, 0, DecimalUtils::StrToInt128("100").value());
        CheckResult(decimal1, decimal2);
    }
    {
        Decimal decimal1(23, 5, DecimalUtils::StrToInt128("34543").value());
        Decimal decimal2(23, 5, DecimalUtils::StrToInt128("4324324").value());
        CheckResult(decimal1, decimal2);
    }
    {
        Decimal decimal1(23, 15, DecimalUtils::StrToInt128("345345435432").value());
        Decimal decimal2(23, 15, DecimalUtils::StrToInt128("345344425435432").value());
        CheckResult(decimal1, decimal2);
    }
    {
        Decimal decimal1(23, 20, DecimalUtils::StrToInt128("5").value());
        Decimal decimal2(23, 20, DecimalUtils::StrToInt128("50").value());
        CheckResult(decimal1, decimal2);
    }

    // different scales
    {
        Decimal decimal1(23, 4, DecimalUtils::StrToInt128("10000").value());
        Decimal decimal2(23, 3, DecimalUtils::StrToInt128("10000").value());
        CheckResult(decimal1, decimal2);
    }
    {
        Decimal decimal1(23, 2, DecimalUtils::StrToInt128("111").value());
        Decimal decimal2(23, 3, DecimalUtils::StrToInt128("1111").value());
        CheckResult(decimal1, decimal2);
    }
    {
        Decimal decimal1(23, 5, DecimalUtils::StrToInt128("999999").value());
        Decimal decimal2(23, 5, DecimalUtils::StrToInt128("9999999").value());
        CheckResult(decimal1, decimal2);
    }
    {
        Decimal decimal1(23, 1, DecimalUtils::StrToInt128("100").value());
        Decimal decimal2(23, 0, DecimalUtils::StrToInt128("99").value());
        CheckResult(decimal1, decimal2);
    }

    // same integral parts
    {
        Decimal decimal1(23, 0, DecimalUtils::StrToInt128("99999").value());
        Decimal decimal2(23, 1, DecimalUtils::StrToInt128("999999").value());
        CheckResult(decimal1, decimal2);
    }
    {
        Decimal decimal1(23, 3, DecimalUtils::StrToInt128("12345123").value());
        Decimal decimal2(23, 5, DecimalUtils::StrToInt128("1234553432").value());
        CheckResult(decimal1, decimal2);
    }

    // equal numbers
    {
        Decimal decimal1(23, 3, DecimalUtils::StrToInt128("100000").value());
        Decimal decimal2(23, 0, DecimalUtils::StrToInt128("100").value());
        CheckEqual(decimal1, decimal2);
    }
    {
        Decimal decimal1(23, 3, DecimalUtils::StrToInt128("100000").value());
        Decimal decimal2(23, 3, DecimalUtils::StrToInt128("100000").value());
        CheckEqual(decimal1, decimal2);
    }
    {
        Decimal decimal1(23, 10, DecimalUtils::StrToInt128("1").value());
        Decimal decimal2(23, 11, DecimalUtils::StrToInt128("10").value());
        CheckEqual(decimal1, decimal2);
    }

    // large scales (>18)
    {
        Decimal decimal1(128, 35, DecimalUtils::StrToInt128("99").value());
        Decimal decimal2(128, 35, DecimalUtils::StrToInt128("100").value());
        CheckResult(decimal1, decimal2);
    }
    {
        Decimal decimal1(
            128, 29, DecimalUtils::StrToInt128("12345678999999999999999999999999999998").value());
        Decimal decimal2(
            128, 30, DecimalUtils::StrToInt128("123456789999999999999999999999999999999").value());
        CheckResult(decimal1, decimal2);
    }
    {
        Decimal decimal1(
            128, 30, DecimalUtils::StrToInt128("123456789999999999999999999999999999900").value());
        Decimal decimal2(
            128, 29, DecimalUtils::StrToInt128("12345678999999999999999999999999999990").value());
        CheckEqual(decimal1, decimal2);
    }

    // minimum and maximum
    {
        Decimal decimal1(128, 39, Decimal::INT128_MAXIMUM_VALUE);
        Decimal decimal2(
            128, 39, DecimalUtils::StrToInt128("170141183460469231731687303715884105727").value());
        CheckEqual(decimal1, decimal2);
    }
    {
        Decimal decimal1(128, 39, Decimal::INT128_MINIMUM_VALUE);
        Decimal decimal2(
            128, 39, DecimalUtils::StrToInt128("-170141183460469231731687303715884105728").value());
        CheckEqual(decimal1, decimal2);
    }

    // fractional overflow
    {
        Decimal decimal1(128, 39, Decimal::INT128_MAXIMUM_VALUE);
        Decimal decimal2(
            128, 38, DecimalUtils::StrToInt128("99999999999999999999999999999999999999").value());
        CheckResult(decimal1, decimal2);
    }
    {
        Decimal decimal1(
            128, 38, DecimalUtils::StrToInt128("-99999999999999999999999999999999999999").value());
        Decimal decimal2(128, 39, Decimal::INT128_MINIMUM_VALUE);
        CheckResult(decimal1, decimal2);
    }
}
}  // namespace paimon::test
