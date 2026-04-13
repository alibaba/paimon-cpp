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

#include "paimon/core/bucket/hive_bucket_function.h"

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/bucket/hive_hasher.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class HiveBucketFunctionTest : public ::testing::Test {
 protected:
    /// Helper to create a BinaryRow with INT, STRING, BINARY, DECIMAL(10,4) fields.
    /// Matches the Java test: toBinaryRow(rowType, 7, "hello", {1,2,3}, Decimal("12.3400", 10, 4))
    BinaryRow CreateMixedRow(int32_t int_val, const std::string& str_val,
                             const std::vector<char>& binary_val, int64_t decimal_unscaled,
                             int32_t decimal_precision, int32_t decimal_scale) {
        auto pool = GetDefaultPool();
        BinaryRow row(4);
        BinaryRowWriter writer(&row, 0, pool.get());

        // Field 0: INT
        writer.WriteInt(0, int_val);

        // Field 1: STRING
        writer.WriteStringView(1, std::string_view(str_val));

        // Field 2: BINARY
        writer.WriteStringView(2, std::string_view(binary_val.data(), binary_val.size()));

        // Field 3: DECIMAL (compact, precision <= 18)
        writer.WriteDecimal(
            3, Decimal::FromUnscaledLong(decimal_unscaled, decimal_precision, decimal_scale),
            decimal_precision);

        writer.Complete();
        return row;
    }

    /// Helper to create a BinaryRow with all null fields.
    BinaryRow CreateNullRow(int32_t num_fields) {
        auto pool = GetDefaultPool();
        BinaryRow row(num_fields);
        BinaryRowWriter writer(&row, 0, pool.get());
        for (int32_t i = 0; i < num_fields; i++) {
            writer.SetNullAt(i);
        }
        writer.Complete();
        return row;
    }

    BinaryRow CreateIntRow(int32_t value) {
        auto pool = GetDefaultPool();
        return BinaryRowGenerator::GenerateRow({value}, pool.get());
    }

    BinaryRow CreateBooleanRow(bool value) {
        auto pool = GetDefaultPool();
        return BinaryRowGenerator::GenerateRow({value}, pool.get());
    }

    BinaryRow CreateLongRow(int64_t value) {
        auto pool = GetDefaultPool();
        return BinaryRowGenerator::GenerateRow({value}, pool.get());
    }

    BinaryRow CreateFloatRow(float value) {
        auto pool = GetDefaultPool();
        return BinaryRowGenerator::GenerateRow({value}, pool.get());
    }

    BinaryRow CreateDoubleRow(double value) {
        auto pool = GetDefaultPool();
        return BinaryRowGenerator::GenerateRow({value}, pool.get());
    }

    BinaryRow CreateStringRow(const std::string& value) {
        auto pool = GetDefaultPool();
        return BinaryRowGenerator::GenerateRow({value}, pool.get());
    }
};

/// Test matching Java: testHiveBucketFunction
/// RowType: INT, STRING, BYTES, DECIMAL(10,4)
/// Values: 7, "hello", {1,2,3}, Decimal("12.3400", 10, 4)
TEST_F(HiveBucketFunctionTest, TestHiveBucketFunction) {
    std::vector<HiveFieldInfo> field_infos = {
        HiveFieldInfo(FieldType::INT),
        HiveFieldInfo(FieldType::STRING),
        HiveFieldInfo(FieldType::BINARY),
        HiveFieldInfo(FieldType::DECIMAL, 10, 4),
    };
    ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_infos));

    // Decimal("12.3400", 10, 4) => unscaled = 123400
    BinaryRow row = CreateMixedRow(7, "hello", {1, 2, 3}, 123400, 10, 4);

    // Verify individual hash components:
    // HiveHasher.hashBytes("hello") = 99162322
    ASSERT_EQ(99162322, HiveHasher::HashBytes("hello", 5));
    // HiveHasher.hashBytes({1,2,3}) = 1026
    ASSERT_EQ(1026, HiveHasher::HashBytes("\x01\x02\x03", 3));
    // BigDecimal("12.34").hashCode() = 1234 * 31 + 2 = 38256
    // (After normalizing "12.3400" -> "12.34", unscaled=1234, scale=2)
    ASSERT_EQ(38256, HiveHasher::HashDecimal(Decimal::FromUnscaledLong(123400, 10, 4)));

    // expectedHash = 31*(31*(31*7 + 99162322) + 1026) + 38256 = 805989529 (with int32 overflow)
    // bucket = (805989529 & INT32_MAX) % 8 = 1
    ASSERT_EQ(1, func->Bucket(row, 8));
}

/// Test matching Java: testHiveBucketFunctionWithNulls
TEST_F(HiveBucketFunctionTest, TestHiveBucketFunctionWithNulls) {
    std::vector<FieldType> field_types = {FieldType::INT, FieldType::STRING};
    ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

    BinaryRow row = CreateNullRow(2);

    // All nulls => hash = 0, bucket = 0
    ASSERT_EQ(0, func->Bucket(row, 4));
}

/// Test unsupported type returns error on Create
TEST_F(HiveBucketFunctionTest, TestUnsupportedType) {
    // TIMESTAMP type should fail
    std::vector<FieldType> field_types = {FieldType::TIMESTAMP};
    auto result = HiveBucketFunction::Create(field_types);
    ASSERT_NOK_WITH_MSG(result.status(), "Unsupported type");
}

/// Test empty field types returns error
TEST_F(HiveBucketFunctionTest, TestEmptyFieldTypes) {
    std::vector<FieldType> field_types = {};
    auto result = HiveBucketFunction::Create(field_types);
    ASSERT_NOK_WITH_MSG(result.status(), "at least one field");
}

/// Test single INT field
TEST_F(HiveBucketFunctionTest, TestSingleIntField) {
    std::vector<FieldType> field_types = {FieldType::INT};
    ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

    // hash = 31*0 + 42 = 42, bucket = (42 & INT32_MAX) % 5 = 2
    ASSERT_EQ(2, func->Bucket(CreateIntRow(42), 5));
    // hash = 31*0 + 0 = 0, bucket = 0
    ASSERT_EQ(0, func->Bucket(CreateIntRow(0), 5));
}

/// Test BOOLEAN field
TEST_F(HiveBucketFunctionTest, TestBooleanField) {
    std::vector<FieldType> field_types = {FieldType::BOOLEAN};
    ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

    // true => hashInt(1) = 1, bucket = 1 % 4 = 1
    ASSERT_EQ(1, func->Bucket(CreateBooleanRow(true), 4));
    // false => hashInt(0) = 0, bucket = 0
    ASSERT_EQ(0, func->Bucket(CreateBooleanRow(false), 4));
}

/// Test BIGINT field
TEST_F(HiveBucketFunctionTest, TestBigintField) {
    std::vector<FieldType> field_types = {FieldType::BIGINT};
    ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

    // Java Long.hashCode(100L) = (int)(100 ^ (100 >>> 32)) = 100
    // bucket = 100 % 7 = 2
    ASSERT_EQ(2, func->Bucket(CreateLongRow(100L), 7));
}

/// Test FLOAT field with -0.0f
TEST_F(HiveBucketFunctionTest, TestFloatNegativeZero) {
    std::vector<FieldType> field_types = {FieldType::FLOAT};
    ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

    // -0.0f should be treated as 0 => hashInt(0) = 0
    ASSERT_EQ(func->Bucket(CreateFloatRow(0.0f), 5), func->Bucket(CreateFloatRow(-0.0f), 5));
}

/// Test DOUBLE field with -0.0
TEST_F(HiveBucketFunctionTest, TestDoubleNegativeZero) {
    std::vector<FieldType> field_types = {FieldType::DOUBLE};
    ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

    // -0.0 should be treated as 0L => hashLong(0) = 0
    ASSERT_EQ(func->Bucket(CreateDoubleRow(0.0), 5), func->Bucket(CreateDoubleRow(-0.0), 5));
}

/// Test STRING field
TEST_F(HiveBucketFunctionTest, TestStringField) {
    std::vector<FieldType> field_types = {FieldType::STRING};
    ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

    // hashBytes("hello") = 99162322
    // bucket = (99162322 & INT32_MAX) % 10 = 99162322 % 10 = 2
    ASSERT_EQ(2, func->Bucket(CreateStringRow("hello"), 10));
}

/// Test different num_buckets produce valid results
TEST_F(HiveBucketFunctionTest, TestDifferentNumBuckets) {
    std::vector<FieldType> field_types = {FieldType::INT};
    ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

    for (int32_t num_buckets = 1; num_buckets <= 20; num_buckets++) {
        int32_t bucket = func->Bucket(CreateIntRow(12345), num_buckets);
        ASSERT_GE(bucket, 0);
        ASSERT_LT(bucket, num_buckets);
    }
}

}  // namespace paimon::test
