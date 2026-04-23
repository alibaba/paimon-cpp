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

#include <limits>

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
        writer.WriteStringView(1, std::string_view{str_val});

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

/// Test compatibility with Java HiveBucketFunction across multiple data types.
/// Expected values are computed from the Java implementation:
///   hash = 0 (seed)
///   for each field: hash = 31 * hash + computeHash(field)
///   bucket = (hash & INT32_MAX) % numBuckets
///
/// Java computeHash per type:
///   BOOLEAN: hashInt(value ? 1 : 0)
///   INT/DATE: hashInt(value)  [identity]
///   BIGINT: hashLong(value) = (int)(value ^ (value >>> 32))
///   FLOAT: hashInt(Float.floatToIntBits(value)), -0.0f treated as 0
///   DOUBLE: hashLong(Double.doubleToLongBits(value)), -0.0 treated as 0L
///   STRING/BINARY: hashBytes(bytes)
///   DECIMAL: BigDecimal.hashCode() after normalization
TEST_F(HiveBucketFunctionTest, TestCompatibleWithJava) {
    auto pool = GetDefaultPool();
    const int32_t num_buckets = 128;

    // Case 1: Single INT field with various values
    // Java: hash = 31*0 + hashInt(v) = v
    // bucket = (v & INT32_MAX) % 128
    {
        std::vector<FieldType> field_types = {FieldType::INT};
        ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

        // hashInt(0) = 0, bucket = 0
        ASSERT_EQ(0, func->Bucket(CreateIntRow(0), num_buckets));
        // hashInt(1) = 1, bucket = 1
        ASSERT_EQ(1, func->Bucket(CreateIntRow(1), num_buckets));
        // hashInt(127) = 127, bucket = 127
        ASSERT_EQ(127, func->Bucket(CreateIntRow(127), num_buckets));
        // hashInt(128) = 128, bucket = 0
        ASSERT_EQ(0, func->Bucket(CreateIntRow(128), num_buckets));
        // hashInt(-1) = -1, (-1 & INT32_MAX) = 2147483647, 2147483647 % 128 = 127
        ASSERT_EQ(127, func->Bucket(CreateIntRow(-1), num_buckets));
        // hashInt(INT32_MIN) = -2147483648, (-2147483648 & INT32_MAX) = 0, bucket = 0
        ASSERT_EQ(0, func->Bucket(CreateIntRow(std::numeric_limits<int32_t>::min()), num_buckets));
        // hashInt(INT32_MAX) = 2147483647, (2147483647 & INT32_MAX) = 2147483647, % 128 = 127
        ASSERT_EQ(127,
                  func->Bucket(CreateIntRow(std::numeric_limits<int32_t>::max()), num_buckets));
    }

    // Case 2: Single BOOLEAN field
    // Java: hashInt(true ? 1 : 0)
    {
        std::vector<FieldType> field_types = {FieldType::BOOLEAN};
        ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

        // true => hashInt(1) = 1, bucket = 1 % 128 = 1
        ASSERT_EQ(1, func->Bucket(CreateBooleanRow(true), num_buckets));
        // false => hashInt(0) = 0, bucket = 0
        ASSERT_EQ(0, func->Bucket(CreateBooleanRow(false), num_buckets));
    }

    // Case 3: Single BIGINT field
    // Java: hashLong(v) = (int)(v ^ (v >>> 32))
    {
        std::vector<FieldType> field_types = {FieldType::BIGINT};
        ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

        // hashLong(0) = 0, bucket = 0
        ASSERT_EQ(0, func->Bucket(CreateLongRow(0L), num_buckets));
        // hashLong(100) = (int)(100 ^ 0) = 100, bucket = 100 % 128 = 100
        ASSERT_EQ(100, func->Bucket(CreateLongRow(100L), num_buckets));
        // hashLong(4294967296L) = (int)(4294967296 ^ 1) = 1, bucket = 1
        // 4294967296L = 0x100000000, >>> 32 = 1, xor = 0x100000001, (int) = 1
        ASSERT_EQ(1, func->Bucket(CreateLongRow(4294967296L), num_buckets));
        // hashLong(LONG_MAX) = (int)(0x7FFFFFFFFFFFFFFF ^ 0x7FFFFFFF) = (int)0x7FFFFF80000000
        // = (int)(0x7FFFFFFF80000000) => low 32 bits = 0x80000000 = -2147483648
        // Actually: 0x7FFFFFFFFFFFFFFF ^ (0x7FFFFFFFFFFFFFFF >>> 32)
        //         = 0x7FFFFFFFFFFFFFFF ^ 0x7FFFFFFF
        //         = 0x7FFFFFFF80000000
        // (int) = 0x80000000 = -2147483648
        // (-2147483648 & INT32_MAX) = 0, bucket = 0
        ASSERT_EQ(0, func->Bucket(CreateLongRow(std::numeric_limits<int64_t>::max()), num_buckets));
        // hashLong(-1) = (int)(-1 ^ (0xFFFFFFFFFFFFFFFF >>> 32))
        //              = (int)(-1 ^ 0xFFFFFFFF) = (int)(0) = 0
        ASSERT_EQ(0, func->Bucket(CreateLongRow(-1L), num_buckets));
    }

    // Case 4: Single FLOAT field
    // Java: hashInt(Float.floatToIntBits(v)), -0.0f => 0
    {
        std::vector<FieldType> field_types = {FieldType::FLOAT};
        ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

        // 0.0f => bits = 0, hashInt(0) = 0
        ASSERT_EQ(0, func->Bucket(CreateFloatRow(0.0f), num_buckets));
        // -0.0f => treated as 0, hashInt(0) = 0
        ASSERT_EQ(0, func->Bucket(CreateFloatRow(-0.0f), num_buckets));
        // 1.0f => Float.floatToIntBits(1.0f) = 0x3F800000 = 1065353216
        // 1065353216 & INT32_MAX = 1065353216, % 128 = 0
        ASSERT_EQ(0, func->Bucket(CreateFloatRow(1.0f), num_buckets));
        // -1.0f => Float.floatToIntBits(-1.0f) = 0xBF800000 = -1082130432
        // (-1082130432 & INT32_MAX) = 1065353216, % 128 = 0
        ASSERT_EQ(0, func->Bucket(CreateFloatRow(-1.0f), num_buckets));
    }

    // Case 5: Single DOUBLE field
    // Java: hashLong(Double.doubleToLongBits(v)), -0.0 => 0L
    {
        std::vector<FieldType> field_types = {FieldType::DOUBLE};
        ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

        // 0.0 => bits = 0L, hashLong(0) = 0
        ASSERT_EQ(0, func->Bucket(CreateDoubleRow(0.0), num_buckets));
        // -0.0 => treated as 0L, hashLong(0) = 0
        ASSERT_EQ(0, func->Bucket(CreateDoubleRow(-0.0), num_buckets));
        // 1.0 => Double.doubleToLongBits(1.0) = 0x3FF0000000000000 = 4607182418800017408
        // hashLong = (int)(4607182418800017408 ^ (4607182418800017408 >>> 32))
        //          = (int)(0x3FF0000000000000 ^ 0x3FF00000)
        //          = (int)(0x3FF000003FF00000)
        //          = (int)(0x3FF00000) = 1072693248
        // 1072693248 % 128 = 0
        ASSERT_EQ(0, func->Bucket(CreateDoubleRow(1.0), num_buckets));
    }

    // Case 6: Single STRING field
    // Java: hashBytes(bytes)
    {
        std::vector<FieldType> field_types = {FieldType::STRING};
        ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_types));

        // hashBytes("hello") = 99162322 (verified in TestHiveBucketFunction)
        // 99162322 & INT32_MAX = 99162322, % 128 = 82
        ASSERT_EQ(82, func->Bucket(CreateStringRow("hello"), num_buckets));
        // hashBytes("") = 0, bucket = 0
        ASSERT_EQ(0, func->Bucket(CreateStringRow(""), num_buckets));
        // hashBytes("a") = 97, bucket = 97
        ASSERT_EQ(97, func->Bucket(CreateStringRow("a"), num_buckets));
    }

    // Case 7: Single DATE field (same as INT)
    // Java: hashInt(daysSinceEpoch)
    {
        std::vector<HiveFieldInfo> field_infos = {HiveFieldInfo(FieldType::DATE)};
        ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_infos));

        // DATE is stored as int32 days since epoch, hashed same as INT
        // date = 2000 (days), hashInt(2000) = 2000, 2000 % 128 = 80
        ASSERT_EQ(80, func->Bucket(CreateIntRow(2000), num_buckets));
    }

    // Case 8: Multi-field row (INT, STRING, BINARY, DECIMAL)
    // This is the same as TestHiveBucketFunction but with num_buckets=128
    // Java step-by-step (all arithmetic in int32 with overflow):
    //   hash = 0
    //   hash = 31*0 + hashInt(7) = 7
    //   hash = 31*7 + hashBytes("hello") = 217 + 99162322 = 99162539
    //   hash = 31*99162539 + hashBytes({1,2,3}) = int32(-1220928587) + 1026 = -1220927561
    //   hash = 31*(-1220927561) + hashDecimal(12.3400) = int32(805951273) + 38256 = 805989529
    //   bucket = (805989529 & INT32_MAX) % 128 = 25
    {
        std::vector<HiveFieldInfo> field_infos = {
            HiveFieldInfo(FieldType::INT),
            HiveFieldInfo(FieldType::STRING),
            HiveFieldInfo(FieldType::BINARY),
            HiveFieldInfo(FieldType::DECIMAL, 10, 4),
        };
        ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_infos));

        BinaryRow row = CreateMixedRow(7, "hello", {1, 2, 3}, 123400, 10, 4);
        // Already verified: func->Bucket(row, 8) == 1
        ASSERT_EQ(1, func->Bucket(row, 8));
        // With 128 buckets: 805989529 % 128 = 25
        ASSERT_EQ(25, func->Bucket(row, num_buckets));
    }

    // Case 9: All-null row
    // Java: all nulls => hash = 0, bucket = 0
    {
        std::vector<HiveFieldInfo> field_infos = {
            HiveFieldInfo(FieldType::INT),
            HiveFieldInfo(FieldType::STRING),
            HiveFieldInfo(FieldType::BIGINT),
        };
        ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_infos));

        BinaryRow row = CreateNullRow(3);
        ASSERT_EQ(0, func->Bucket(row, num_buckets));
    }

    // Case 10: Multi-field row with BOOLEAN, INT, BIGINT, FLOAT, DOUBLE, STRING
    // Java step-by-step:
    //   field 0: BOOLEAN true => hashInt(1) = 1
    //   field 1: INT 42 => hashInt(42) = 42
    //   field 2: BIGINT 100 => hashLong(100) = 100
    //   field 3: FLOAT 0.0f => hashInt(0) = 0
    //   field 4: DOUBLE 0.0 => hashLong(0) = 0
    //   field 5: STRING "a" => hashBytes("a") = 97
    //
    //   hash = 0
    //   hash = 31*0 + 1 = 1
    //   hash = 31*1 + 42 = 73
    //   hash = 31*73 + 100 = 2363
    //   hash = 31*2363 + 0 = 73253
    //   hash = 31*73253 + 0 = 2270843
    //   hash = 31*2270843 + 97 = 70396230
    //   bucket = (70396230 & INT32_MAX) % 128 = 70396230 % 128 = 70
    {
        std::vector<HiveFieldInfo> field_infos = {
            HiveFieldInfo(FieldType::BOOLEAN), HiveFieldInfo(FieldType::INT),
            HiveFieldInfo(FieldType::BIGINT),  HiveFieldInfo(FieldType::FLOAT),
            HiveFieldInfo(FieldType::DOUBLE),  HiveFieldInfo(FieldType::STRING),
        };
        ASSERT_OK_AND_ASSIGN(auto func, HiveBucketFunction::Create(field_infos));

        BinaryRow row(6);
        BinaryRowWriter writer(&row, 0, pool.get());
        writer.WriteBoolean(0, true);
        writer.WriteInt(1, 42);
        writer.WriteLong(2, 100L);
        writer.WriteFloat(3, 0.0f);
        writer.WriteDouble(4, 0.0);
        writer.WriteStringView(5, std::string_view("a"));
        writer.Complete();

        ASSERT_EQ(70, func->Bucket(row, num_buckets));
    }
}

}  // namespace paimon::test
