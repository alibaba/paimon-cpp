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

#include "paimon/common/data/data_define.h"

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// Test case: IsVariantNull should return true for NullType
TEST(DataDefineTest, IsVariantNullReturnsTrueForNull) {
    VariantType null_variant = NullType{};
    ASSERT_TRUE(DataDefine::IsVariantNull(null_variant));
}

// Test case: IsVariantNull should return false for non-NullType variants
TEST(DataDefineTest, IsVariantNullReturnsFalseForNonNullTypes) {
    VariantType non_null_variant = 42;  // Example with int
    ASSERT_FALSE(DataDefine::IsVariantNull(non_null_variant));

    non_null_variant = true;  // Example with bool
    ASSERT_FALSE(DataDefine::IsVariantNull(non_null_variant));
}

// Test case: GetVariantValue should return valid value for matched types
TEST(DataDefineTest, GetVariantValue) {
    {
        VariantType int_variant = 42;  // Variant holding an int
        const auto int_value = DataDefine::GetVariantValue<int32_t>(int_variant);
        ASSERT_EQ(int_value, 42);
    }
    {
        VariantType bool_variant = true;  // Variant holding a bool
        const bool bool_value = DataDefine::GetVariantValue<bool>(bool_variant);
        ASSERT_EQ(bool_value, true);
    }
    {
        auto array = std::dynamic_pointer_cast<arrow::StringArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["abc", "def", "hello"])")
                .ValueOrDie());
        VariantType view_variant = array->GetView(2);  // Variant holding a StringView
        const auto view_value = DataDefine::GetVariantValue<std::string_view>(view_variant);
        ASSERT_EQ(std::string(view_value), "hello");
        ASSERT_EQ(DataDefine::VariantValueToString(view_variant), "hello");
    }
}

// Test case: GetVariantValue should return valid value for matching types
TEST(DataDefineTest, GetVariantValueReturnsPointerForMatchingType) {}

// Test case: VariantValueToString should handle NullType
TEST(DataDefineTest, VariantValueToStringReturnsStringForNullType) {
    VariantType null_variant = NullType{};
    ASSERT_EQ(DataDefine::VariantValueToString(null_variant), "null");
}

// Test case: VariantValueToString should handle bool
TEST(DataDefineTest, VariantValueToStringReturnsStringForBool) {
    VariantType bool_variant = true;
    ASSERT_EQ(DataDefine::VariantValueToString(bool_variant), "true");

    bool_variant = false;
    ASSERT_EQ(DataDefine::VariantValueToString(bool_variant), "false");
}

// Test case: VariantValueToString should handle integer types
TEST(DataDefineTest, VariantValueToStringReturnsStringForInt) {
    VariantType int_variant = 42;
    ASSERT_EQ(DataDefine::VariantValueToString(int_variant), "42");
}

// Test case: VariantValueToString should handle string data (BinaryString)
TEST(DataDefineTest, VariantValueToStringReturnsStringForBinaryString) {
    auto pool = GetDefaultPool();
    auto binary_str = BinaryString::FromString("Hello, world!", pool.get());
    VariantType binary_variant = binary_str;
    ASSERT_EQ(DataDefine::VariantValueToString(binary_variant), "Hello, world!");
}

// Test case: VariantValueToString should handle shared_ptr<Bytes>
TEST(DataDefineTest, VariantValueToStringReturnsStringForSharedPtrBytes) {
    auto pool = GetDefaultPool();
    std::shared_ptr<Bytes> bytes_ptr = Bytes::AllocateBytes("abc", pool.get());
    VariantType bytes_variant = bytes_ptr;
    ASSERT_EQ(DataDefine::VariantValueToString(bytes_variant), "abc");
}

// Test case: VariantValueToString should handle Timestamp
TEST(DataDefineTest, VariantValueToStringReturnsStringForTimestamp) {
    auto timestamp =
        Timestamp(/*millisecond=*/1622520000000l, /*nano_of_millisecond=*/0);  // A timestamp value
    VariantType timestamp_variant = timestamp;
    ASSERT_EQ(DataDefine::VariantValueToString(timestamp_variant), timestamp.ToString());
}

// Test case: VariantValueToString should handle Decimal
TEST(DataDefineTest, VariantValueToStringReturnsStringForDecimal) {
    Decimal decimal(38, 38, DecimalUtils::StrToInt128("12345678998765432145678").value());
    VariantType decimal_variant = decimal;
    ASSERT_EQ(DataDefine::VariantValueToString(decimal_variant), decimal.ToString());
}

// Test case: VariantValueToString should handle shared_ptr<InternalRow> (mocking with a string)
TEST(DataDefineTest, VariantValueToStringReturnsStringForInternalRow) {
    std::shared_ptr<InternalRow> row_ptr = std::make_shared<BinaryRow>(0);
    VariantType row_variant = row_ptr;
    ASSERT_EQ(DataDefine::VariantValueToString(row_variant), "row");
}

// Test case: VariantValueToString should handle shared_ptr<InternalArray> (mocking with a string)
TEST(DataDefineTest, VariantValueToStringReturnsStringForInternalArray) {
    auto pool = GetDefaultPool();
    std::shared_ptr<InternalArray> array_ptr = std::make_shared<BinaryArray>();
    VariantType array_variant = array_ptr;
    ASSERT_EQ(DataDefine::VariantValueToString(array_variant), "array");
}

// Test case: GetStringView should handle all variant types and edge cases
TEST(DataDefineTest, GetStringView) {
    auto pool = GetDefaultPool();

    {
        // from string_view
        std::string original = "hello world";
        VariantType view_variant = std::string_view(original.data(), original.size());
        auto result = DataDefine::GetStringView(view_variant);
        ASSERT_EQ(result, "hello world");
        ASSERT_EQ(result.data(), original.data());
    }
    {
        // from shared_ptr<Bytes>
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes("test bytes", pool.get());
        VariantType bytes_variant = bytes;
        auto result = DataDefine::GetStringView(bytes_variant);
        ASSERT_EQ(std::string(result), "test bytes");
        ASSERT_EQ(result.size(), 10);
    }
    {
        // from BinaryString
        auto binary_str = BinaryString::FromString("binary string content", pool.get());
        VariantType binary_variant = binary_str;
        auto result = DataDefine::GetStringView(binary_variant);
        ASSERT_EQ(std::string(result), "binary string content");
    }
    {
        // empty string_view
        VariantType view_variant = std::string_view();
        auto result = DataDefine::GetStringView(view_variant);
        ASSERT_TRUE(result.empty());
        ASSERT_EQ(result.size(), 0);
    }
    {
        // empty Bytes
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes("", pool.get());
        VariantType bytes_variant = bytes;
        auto result = DataDefine::GetStringView(bytes_variant);
        ASSERT_TRUE(result.empty());
        ASSERT_EQ(result.size(), 0);
    }
    {
        // empty BinaryString
        VariantType binary_variant = BinaryString::EmptyUtf8();
        auto result = DataDefine::GetStringView(binary_variant);
        ASSERT_TRUE(result.empty());
        ASSERT_EQ(result.size(), 0);
    }
}

TEST(DataDefineTest, VariantValueToLiteral) {
    auto pool = GetDefaultPool();

    {
        // BOOL
        VariantType value = true;
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::BOOL));
        ASSERT_EQ(literal.GetValue<bool>(), true);
    }
    {
        // INT8
        VariantType value = static_cast<char>(42);
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::INT8));
        ASSERT_EQ(literal.GetValue<int8_t>(), 42);
    }
    {
        // INT16
        VariantType value = static_cast<int16_t>(1000);
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::INT16));
        ASSERT_EQ(literal.GetValue<int16_t>(), 1000);
    }
    {
        // INT32
        VariantType value = static_cast<int32_t>(100000);
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::INT32));
        ASSERT_EQ(literal.GetValue<int32_t>(), 100000);
    }
    {
        // INT64
        VariantType value = static_cast<int64_t>(123456789L);
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::INT64));
        ASSERT_EQ(literal.GetValue<int64_t>(), 123456789L);
    }
    {
        // FLOAT
        VariantType value = 3.14f;
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::FLOAT));
        ASSERT_FLOAT_EQ(literal.GetValue<float>(), 3.14f);
    }
    {
        // DOUBLE
        VariantType value = 2.718;
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::DOUBLE));
        ASSERT_DOUBLE_EQ(literal.GetValue<double>(), 2.718);
    }
    {
        // STRING from BinaryString
        auto binary_str = BinaryString::FromString("hello", pool.get());
        VariantType value = binary_str;
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::STRING));
        ASSERT_EQ(literal.GetValue<std::string>(), "hello");
    }
    {
        // STRING from shared_ptr<Bytes>
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes("world", pool.get());
        VariantType value = bytes;
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::STRING));
        ASSERT_EQ(literal.GetValue<std::string>(), "world");
    }
    {
        // STRING from string_view
        std::string original = "view_str";
        VariantType value = std::string_view(original.data(), original.size());
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::STRING));
        ASSERT_EQ(literal.GetValue<std::string>(), "view_str");
    }
    {
        // BINARY from shared_ptr<Bytes>
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes("binary_data", pool.get());
        VariantType value = bytes;
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::BINARY));
        ASSERT_EQ(literal.GetValue<std::string>(), "binary_data");
    }
    {
        // BINARY from BinaryString
        auto binary_str = BinaryString::FromString("bin_str", pool.get());
        VariantType value = binary_str;
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::BINARY));
        ASSERT_EQ(literal.GetValue<std::string>(), "bin_str");
    }
    {
        // TIMESTAMP
        Timestamp ts(12345, 1);
        VariantType value = ts;
        ASSERT_OK_AND_ASSIGN(
            auto literal, DataDefine::VariantValueToLiteral(value, arrow::Type::type::TIMESTAMP));
        ASSERT_EQ(literal.GetValue<Timestamp>(), ts);
    }
    {
        // DECIMAL128
        Decimal decimal(20, 3, 123456);
        VariantType value = decimal;
        ASSERT_OK_AND_ASSIGN(
            auto literal, DataDefine::VariantValueToLiteral(value, arrow::Type::type::DECIMAL128));
        ASSERT_EQ(literal.GetValue<Decimal>(), decimal);
    }
    {
        // DATE32
        VariantType value = static_cast<int32_t>(19000);
        ASSERT_OK_AND_ASSIGN(auto literal,
                             DataDefine::VariantValueToLiteral(value, arrow::Type::type::DATE32));
        ASSERT_EQ(literal.GetValue<int32_t>(), 19000);
    }
    {
        // unsupported type
        VariantType value = static_cast<int32_t>(0);
        ASSERT_NOK_WITH_MSG(DataDefine::VariantValueToLiteral(value, arrow::Type::type::LIST),
                            "Not support arrow type");
    }
}

}  // namespace paimon::test
