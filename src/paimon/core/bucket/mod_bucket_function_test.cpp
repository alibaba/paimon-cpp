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
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

BinaryRow CreateIntRow(int32_t value) {
    auto pool = GetDefaultPool();
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool.get());
    writer.WriteInt(0, value);
    writer.Complete();
    return row;
}

BinaryRow CreateLongRow(int64_t value) {
    auto pool = GetDefaultPool();
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool.get());
    writer.WriteLong(0, value);
    writer.Complete();
    return row;
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

TEST(ModBucketFunctionTest, TestUnsupportedType) {
    // STRING type should fail
    auto result = ModBucketFunction::Create(FieldType::STRING);
    ASSERT_NOK(result.status());
}

TEST(ModBucketFunctionTest, TestUnsupportedFloatType) {
    // FLOAT type should fail
    auto result = ModBucketFunction::Create(FieldType::FLOAT);
    ASSERT_NOK(result.status());
}

TEST(ModBucketFunctionTest, TestUnsupportedDoubleType) {
    // DOUBLE type should fail
    auto result = ModBucketFunction::Create(FieldType::DOUBLE);
    ASSERT_NOK(result.status());
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

}  // namespace paimon::test
