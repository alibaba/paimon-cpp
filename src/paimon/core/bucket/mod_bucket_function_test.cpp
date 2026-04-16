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

#include "paimon/core/bucket/mod_bucket_function.h"

#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

BinaryRow CreateIntRow(int32_t value) {
    auto pool = GetDefaultPool();
    return BinaryRowGenerator::GenerateRow({value}, pool.get());
}

BinaryRow CreateLongRow(int64_t value) {
    auto pool = GetDefaultPool();
    return BinaryRowGenerator::GenerateRow({value}, pool.get());
}

}  // namespace

TEST(ModBucketFunctionTest, TestIntType) {
    ASSERT_OK_AND_ASSIGN(auto func, ModBucketFunction::Create(FieldType::INT));

    // 1 % 5 = 1
    ASSERT_EQ(1, func->Bucket(CreateIntRow(1), 5));
    // 7 % 5 = 2
    ASSERT_EQ(2, func->Bucket(CreateIntRow(7), 5));
    // -2 floorMod 5 = 3 (Java Math.floorMod(-2, 5) = 3)
    ASSERT_EQ(3, func->Bucket(CreateIntRow(-2), 5));
}

TEST(ModBucketFunctionTest, TestBigintType) {
    ASSERT_OK_AND_ASSIGN(auto func, ModBucketFunction::Create(FieldType::BIGINT));

    // 8 % 5 = 3
    ASSERT_EQ(3, func->Bucket(CreateLongRow(8), 5));
    // 0 % 5 = 0
    ASSERT_EQ(0, func->Bucket(CreateLongRow(0), 5));
    // -3 floorMod 5 = 2 (Java Math.floorMod(-3L, 5) = 2)
    ASSERT_EQ(2, func->Bucket(CreateLongRow(-3), 5));
}

TEST(ModBucketFunctionTest, TestUnsupportedTypes) {
    {
        // STRING type should fail
        auto result = ModBucketFunction::Create(FieldType::STRING);
        ASSERT_NOK_WITH_MSG(result.status(), "only supports INT or BIGINT");
    }
    {
        // FLOAT type should fail
        auto result = ModBucketFunction::Create(FieldType::FLOAT);
        ASSERT_NOK_WITH_MSG(result.status(), "only supports INT or BIGINT");
    }
    {
        // DOUBLE type should fail
        auto result = ModBucketFunction::Create(FieldType::DOUBLE);
        ASSERT_NOK_WITH_MSG(result.status(), "only supports INT or BIGINT");
    }
}

TEST(ModBucketFunctionTest, TestIntEdgeCases) {
    ASSERT_OK_AND_ASSIGN(auto func, ModBucketFunction::Create(FieldType::INT));

    // 0 % 5 = 0
    ASSERT_EQ(0, func->Bucket(CreateIntRow(0), 5));
    // 5 % 5 = 0
    ASSERT_EQ(0, func->Bucket(CreateIntRow(5), 5));
    // -5 floorMod 5 = 0
    ASSERT_EQ(0, func->Bucket(CreateIntRow(-5), 5));
    // 1 % 1 = 0
    ASSERT_EQ(0, func->Bucket(CreateIntRow(1), 1));
}

TEST(ModBucketFunctionTest, TestBigintEdgeCases) {
    ASSERT_OK_AND_ASSIGN(auto func, ModBucketFunction::Create(FieldType::BIGINT));

    // Large value
    ASSERT_EQ(3, func->Bucket(CreateLongRow(1000000003L), 5));
    // Negative large value: -1000000003 floorMod 5 = 2
    ASSERT_EQ(2, func->Bucket(CreateLongRow(-1000000003L), 5));
}

/// Large random compatibility test to ensure alignment with Java's Math.floorMod behavior.
/// The expected values are pre-computed using Java's Math.floorMod.
TEST(ModBucketFunctionTest, TestCompatibleWithJava) {
    ASSERT_OK_AND_ASSIGN(auto int_func, ModBucketFunction::Create(FieldType::INT));
    ASSERT_OK_AND_ASSIGN(auto long_func, ModBucketFunction::Create(FieldType::BIGINT));

    // Test INT type: pairs of (value, num_buckets) -> expected bucket (Java Math.floorMod)
    // These values cover positive, negative, zero, edge cases, and large values.
    struct IntTestCase {
        int32_t value;
        int32_t num_buckets;
        int32_t expected;
    };
    std::vector<IntTestCase> int_cases = {
        {0, 10, 0},
        {1, 10, 1},
        {-1, 10, 9},
        {10, 10, 0},
        {-10, 10, 0},
        {11, 10, 1},
        {-11, 10, 9},
        {2147483647, 100, 47},   // INT32_MAX
        {-2147483647, 100, 53},  // -(INT32_MAX)
        {2147483647, 7, 1},
        {-2147483647, 7, 6},
        {123456789, 1000, 789},
        {-123456789, 1000, 211},
        {999, 1, 0},
        {-999, 1, 0},
        {42, 3, 0},
        {-42, 3, 0},
        {43, 3, 1},
        {-43, 3, 2},
        {100, 7, 2},
        {-100, 7, 5},
    };
    for (const auto& tc : int_cases) {
        ASSERT_EQ(tc.expected, int_func->Bucket(CreateIntRow(tc.value), tc.num_buckets))
            << "INT floorMod(" << tc.value << ", " << tc.num_buckets << ")";
    }

    // Test BIGINT type: pairs of (value, num_buckets) -> expected bucket (Java Math.floorMod)
    struct LongTestCase {
        int64_t value;
        int32_t num_buckets;
        int32_t expected;
    };
    std::vector<LongTestCase> long_cases = {
        {0L, 10, 0},
        {1L, 10, 1},
        {-1L, 10, 9},
        {10L, 10, 0},
        {-10L, 10, 0},
        {9223372036854775807L, 100, 7},    // INT64_MAX
        {-9223372036854775807L, 100, 93},  // -(INT64_MAX)
        {9223372036854775807L, 7, 0},
        {-9223372036854775807L, 7, 0},
        {1234567890123456789L, 1000, 789},
        {-1234567890123456789L, 1000, 211},
        {100L, 7, 2},
        {-100L, 7, 5},
        {999999999999L, 13, 0},
        {-999999999999L, 13, 0},
    };
    for (const auto& tc : long_cases) {
        ASSERT_EQ(tc.expected, long_func->Bucket(CreateLongRow(tc.value), tc.num_buckets))
            << "BIGINT floorMod(" << tc.value << ", " << tc.num_buckets << ")";
    }

    // Verify that all bucket results are in valid range [0, num_buckets)
    for (int32_t num_buckets = 1; num_buckets <= 50; num_buckets++) {
        for (int32_t v = -100; v <= 100; v++) {
            int32_t bucket = int_func->Bucket(CreateIntRow(v), num_buckets);
            ASSERT_GE(bucket, 0);
            ASSERT_LT(bucket, num_buckets);
        }
    }
}

}  // namespace paimon::test
