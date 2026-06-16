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

#include "paimon/core/bucket/default_bucket_function.h"

#include <limits>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

void CheckDefaultBucket(const DefaultBucketFunction& func, const BinaryRow& row,
                        int32_t expected_hash, int32_t expected_bucket) {
    constexpr int32_t kNumBuckets = 1000;
    ASSERT_EQ(expected_hash, row.HashCode());
    ASSERT_EQ(expected_bucket, func.Bucket(row, kNumBuckets));
}

}  // namespace

TEST(DefaultBucketFunctionTest, TestBasicHashMod) {
    auto pool = GetDefaultPool();
    DefaultBucketFunction func;

    // Create a row with a single INT field
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool.get());
    writer.WriteInt(0, 42);
    writer.Complete();

    int32_t num_buckets = 5;
    int32_t bucket = func.Bucket(row, num_buckets);
    ASSERT_GE(bucket, 0);
    ASSERT_LT(bucket, num_buckets);

    // Verify it matches the expected formula: abs(hashCode % numBuckets)
    int32_t expected = std::abs(row.HashCode() % num_buckets);
    ASSERT_EQ(expected, bucket);
}

TEST(DefaultBucketFunctionTest, TestDifferentNumBuckets) {
    auto pool = GetDefaultPool();
    DefaultBucketFunction func;

    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool.get());
    writer.WriteInt(0, 100);
    writer.Complete();

    for (int32_t num_buckets = 1; num_buckets <= 10; num_buckets++) {
        int32_t bucket = func.Bucket(row, num_buckets);
        ASSERT_GE(bucket, 0);
        ASSERT_LT(bucket, num_buckets);
        ASSERT_EQ(std::abs(row.HashCode() % num_buckets), bucket);
    }
}

TEST(DefaultBucketFunctionTest, TestMultiFieldRow) {
    auto pool = GetDefaultPool();
    DefaultBucketFunction func;

    BinaryRow row(3);
    BinaryRowWriter writer(&row, 0, pool.get());
    writer.WriteInt(0, 1);
    writer.WriteLong(1, 2);
    writer.WriteInt(2, 3);
    writer.Complete();

    int32_t num_buckets = 7;
    int32_t bucket = func.Bucket(row, num_buckets);
    ASSERT_GE(bucket, 0);
    ASSERT_LT(bucket, num_buckets);
    ASSERT_EQ(std::abs(row.HashCode() % num_buckets), bucket);
}

TEST(DefaultBucketFunctionTest, TestFloatSpecialValuesCompatibleWithJava) {
    auto pool = GetDefaultPool();
    DefaultBucketFunction func;

    // Verified with Java DefaultBucketFunction and NUM_BUCKETS = 1000.
    CheckDefaultBucket(
        func,
        BinaryRowGenerator::GenerateRow({std::numeric_limits<float>::quiet_NaN()}, pool.get()),
        -2039172089, 89);
    CheckDefaultBucket(
        func, BinaryRowGenerator::GenerateRow({std::numeric_limits<float>::infinity()}, pool.get()),
        2139216202, 202);
    CheckDefaultBucket(
        func,
        BinaryRowGenerator::GenerateRow({-std::numeric_limits<float>::infinity()}, pool.get()),
        -106221671, 671);
    CheckDefaultBucket(func, BinaryRowGenerator::GenerateRow({0.0f}, pool.get()), -300363099, 99);
    CheckDefaultBucket(func, BinaryRowGenerator::GenerateRow({-0.0f}, pool.get()), 916225219, 219);
}

TEST(DefaultBucketFunctionTest, TestDoubleSpecialValuesCompatibleWithJava) {
    auto pool = GetDefaultPool();
    DefaultBucketFunction func;

    // Verified with Java DefaultBucketFunction and NUM_BUCKETS = 1000.
    CheckDefaultBucket(
        func,
        BinaryRowGenerator::GenerateRow({std::numeric_limits<double>::quiet_NaN()}, pool.get()),
        -1323214697, 697);
    CheckDefaultBucket(
        func,
        BinaryRowGenerator::GenerateRow({std::numeric_limits<double>::infinity()}, pool.get()),
        -1556713404, 404);
    CheckDefaultBucket(
        func,
        BinaryRowGenerator::GenerateRow({-std::numeric_limits<double>::infinity()}, pool.get()),
        -2079171840, 840);
    CheckDefaultBucket(func, BinaryRowGenerator::GenerateRow({0.0}, pool.get()), -300363099, 99);
    CheckDefaultBucket(func, BinaryRowGenerator::GenerateRow({-0.0}, pool.get()), 302122119, 119);
}

}  // namespace paimon::test
