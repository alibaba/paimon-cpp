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

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

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

}  // namespace paimon::test
