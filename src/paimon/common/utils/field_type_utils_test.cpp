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

#include "paimon/common/utils/field_type_utils.h"

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/defs.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// Test case: Check if Integer Numeric types are correctly identified
TEST(FieldTypeUtilsTest, IsIntegerNumeric) {
    ASSERT_TRUE(FieldTypeUtils::IsIntegerNumeric(FieldType::TINYINT));
    ASSERT_TRUE(FieldTypeUtils::IsIntegerNumeric(FieldType::SMALLINT));
    ASSERT_TRUE(FieldTypeUtils::IsIntegerNumeric(FieldType::INT));
    ASSERT_TRUE(FieldTypeUtils::IsIntegerNumeric(FieldType::BIGINT));

    // Non-integer types should return false
    ASSERT_FALSE(FieldTypeUtils::IsIntegerNumeric(FieldType::FLOAT));
    ASSERT_FALSE(FieldTypeUtils::IsIntegerNumeric(FieldType::STRING));
    ASSERT_FALSE(FieldTypeUtils::IsIntegerNumeric(FieldType::TIMESTAMP));
    ASSERT_FALSE(FieldTypeUtils::IsIntegerNumeric(FieldType::DECIMAL));
}

// Test case: Check IntegerScaleLargerThan function
TEST(FieldTypeUtilsTest, IntegerScaleLargerThan) {
    ASSERT_TRUE(FieldTypeUtils::IntegerScaleLargerThan(FieldType::SMALLINT, FieldType::TINYINT));
    ASSERT_TRUE(FieldTypeUtils::IntegerScaleLargerThan(FieldType::INT, FieldType::SMALLINT));
    ASSERT_TRUE(FieldTypeUtils::IntegerScaleLargerThan(FieldType::BIGINT, FieldType::INT));
    ASSERT_TRUE(FieldTypeUtils::IntegerScaleLargerThan(FieldType::BIGINT, FieldType::SMALLINT));

    // Should return false for other cases
    ASSERT_FALSE(FieldTypeUtils::IntegerScaleLargerThan(FieldType::TINYINT, FieldType::SMALLINT));
    ASSERT_FALSE(FieldTypeUtils::IntegerScaleLargerThan(FieldType::SMALLINT, FieldType::INT));
    ASSERT_FALSE(FieldTypeUtils::IntegerScaleLargerThan(FieldType::INT, FieldType::BIGINT));
}

// Test case: Check ConvertToFieldType with various Arrow types
TEST(FieldTypeUtilsTest, ConvertToFieldType) {
    // Test valid Arrow types and their conversions
    ASSERT_OK_AND_ASSIGN(auto result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::BOOL));
    ASSERT_EQ(result, FieldType::BOOLEAN);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::INT8));
    ASSERT_EQ(result, FieldType::TINYINT);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::INT16));
    ASSERT_EQ(result, FieldType::SMALLINT);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::INT32));
    ASSERT_EQ(result, FieldType::INT);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::INT64));
    ASSERT_EQ(result, FieldType::BIGINT);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::FLOAT));
    ASSERT_EQ(result, FieldType::FLOAT);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::DOUBLE));
    ASSERT_EQ(result, FieldType::DOUBLE);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::STRING));
    ASSERT_EQ(result, FieldType::STRING);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::BINARY));
    ASSERT_EQ(result, FieldType::BINARY);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::TIMESTAMP));
    ASSERT_EQ(result, FieldType::TIMESTAMP);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::DECIMAL128));
    ASSERT_EQ(result, FieldType::DECIMAL);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::DATE32));
    ASSERT_EQ(result, FieldType::DATE);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::LIST));
    ASSERT_EQ(result, FieldType::ARRAY);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::MAP));
    ASSERT_EQ(result, FieldType::MAP);

    ASSERT_OK_AND_ASSIGN(result, FieldTypeUtils::ConvertToFieldType(arrow::Type::type::STRUCT));
    ASSERT_EQ(result, FieldType::STRUCT);

    // Test unsupported Arrow type
    ASSERT_NOK(FieldTypeUtils::ConvertToFieldType(
        static_cast<arrow::Type::type>(999)));  // Invalid Arrow type
}

// Test case: FieldTypeToString
TEST(FieldTypeUtilsTest, FieldTypeToString) {
    // Check string representation of different FieldTypes
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::BOOLEAN), "BOOLEAN");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::TINYINT), "TINYINT");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::SMALLINT), "SMALLINT");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::INT), "INT");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::BIGINT), "BIGINT");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::FLOAT), "FLOAT");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::DOUBLE), "DOUBLE");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::STRING), "STRING");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::BINARY), "BINARY");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::BLOB), "BLOB");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::TIMESTAMP), "TIMESTAMP");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::DECIMAL), "DECIMAL");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::DATE), "DATE");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::ARRAY), "ARRAY");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::MAP), "MAP");
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(FieldType::STRUCT), "STRUCT");

    // Test UNKNOWN type
    auto unknown_type = static_cast<FieldType>(999);
    ASSERT_EQ(FieldTypeUtils::FieldTypeToString(unknown_type), "UNKNOWN, type id:999");
}

}  // namespace paimon::test
