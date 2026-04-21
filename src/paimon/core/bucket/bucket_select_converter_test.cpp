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

#include "paimon/core/bucket/bucket_select_converter.h"

#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/core/bucket/default_bucket_function.h"
#include "paimon/core/bucket/mod_bucket_function.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BucketSelectConverterTest : public ::testing::Test {
 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(BucketSelectConverterTest, SingleIntEqualDefault) {
    int32_t num_buckets = 10;
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    ASSERT_OK_AND_ASSIGN(auto result, BucketSelectConverter::Convert(
                                          predicate, {"id"}, {arrow::int32()},
                                          BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
    ASSERT_TRUE(result.has_value());

    // Verify by computing the expected bucket manually
    auto row = BinaryRowGenerator::GenerateRow({static_cast<int32_t>(42)}, pool_.get());
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value());
}

TEST_F(BucketSelectConverterTest, SingleStringEqualDefault) {
    int32_t num_buckets = 8;
    std::string val = "hello_world";
    Literal lit(FieldType::STRING, val.c_str(), val.size());
    auto predicate = PredicateBuilder::Equal(0, "name", FieldType::STRING, lit);

    ASSERT_OK_AND_ASSIGN(auto result, BucketSelectConverter::Convert(
                                          predicate, {"name"}, {arrow::utf8()},
                                          BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
    ASSERT_TRUE(result.has_value());

    // Verify
    auto row = BinaryRowGenerator::GenerateRow({val}, pool_.get());
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value());
}

TEST_F(BucketSelectConverterTest, MultiKeyAndPredicate) {
    int32_t num_buckets = 5;
    Literal lit_id(static_cast<int32_t>(100));
    Literal lit_name(FieldType::STRING, "test", 4);
    auto pred_id = PredicateBuilder::Equal(0, "id", FieldType::INT, lit_id);
    auto pred_name = PredicateBuilder::Equal(1, "name", FieldType::STRING, lit_name);
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({pred_id, pred_name}));

    ASSERT_OK_AND_ASSIGN(
        auto result,
        BucketSelectConverter::Convert(predicate, {"id", "name"}, {arrow::int32(), arrow::utf8()},
                                       BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
    ASSERT_TRUE(result.has_value());

    // Verify
    auto row = BinaryRowGenerator::GenerateRow({static_cast<int32_t>(100), std::string("test")},
                                               pool_.get());
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value());
}

TEST_F(BucketSelectConverterTest, MissingBucketKeyReturnsNullopt) {
    int32_t num_buckets = 5;
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    ASSERT_OK_AND_ASSIGN(
        auto result,
        BucketSelectConverter::Convert(predicate, {"id", "name"}, {arrow::int32(), arrow::utf8()},
                                       BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
    ASSERT_FALSE(result.has_value());
}

TEST_F(BucketSelectConverterTest, NonEqualPredicateReturnsNullopt) {
    int32_t num_buckets = 5;
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::GreaterThan(0, "id", FieldType::INT, lit);

    ASSERT_OK_AND_ASSIGN(auto result, BucketSelectConverter::Convert(
                                          predicate, {"id"}, {arrow::int32()},
                                          BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
    ASSERT_FALSE(result.has_value());
}

TEST_F(BucketSelectConverterTest, OrPredicateReturnsNullopt) {
    int32_t num_buckets = 5;
    Literal lit1(static_cast<int32_t>(1));
    Literal lit2(static_cast<int32_t>(2));
    auto pred1 = PredicateBuilder::Equal(0, "id", FieldType::INT, lit1);
    auto pred2 = PredicateBuilder::Equal(0, "id", FieldType::INT, lit2);
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({pred1, pred2}));

    ASSERT_OK_AND_ASSIGN(auto result, BucketSelectConverter::Convert(
                                          predicate, {"id"}, {arrow::int32()},
                                          BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
    ASSERT_FALSE(result.has_value());
}

TEST_F(BucketSelectConverterTest, ModBucketFunction) {
    int32_t num_buckets = 7;
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    ASSERT_OK_AND_ASSIGN(auto result, BucketSelectConverter::Convert(
                                          predicate, {"id"}, {arrow::int32()},
                                          BucketFunctionType::MOD, num_buckets, pool_.get()));
    ASSERT_TRUE(result.has_value());

    // Verify: MOD function uses floorMod
    auto row = BinaryRowGenerator::GenerateRow({static_cast<int32_t>(42)}, pool_.get());
    ASSERT_OK_AND_ASSIGN(auto mod_func, ModBucketFunction::Create(FieldType::INT));
    ASSERT_EQ(mod_func->Bucket(row, num_buckets), result.value());
}

TEST_F(BucketSelectConverterTest, NullLiteralReturnsNullopt) {
    int32_t num_buckets = 5;
    Literal lit(FieldType::INT);  // null literal
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    ASSERT_OK_AND_ASSIGN(auto result, BucketSelectConverter::Convert(
                                          predicate, {"id"}, {arrow::int32()},
                                          BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
    ASSERT_FALSE(result.has_value());
}

TEST_F(BucketSelectConverterTest, DynamicBucketModeReturnsNullopt) {
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    ASSERT_OK_AND_ASSIGN(
        auto result, BucketSelectConverter::Convert(predicate, {"id"}, {arrow::int32()},
                                                    BucketFunctionType::DEFAULT, -1, pool_.get()));
    ASSERT_FALSE(result.has_value());
}

TEST_F(BucketSelectConverterTest, NullPredicateReturnsNullopt) {
    ASSERT_OK_AND_ASSIGN(
        auto result, BucketSelectConverter::Convert(nullptr, {"id"}, {arrow::int32()},
                                                    BucketFunctionType::DEFAULT, 5, pool_.get()));
    ASSERT_FALSE(result.has_value());
}

TEST_F(BucketSelectConverterTest, BigintKeyDefault) {
    int32_t num_buckets = 16;
    Literal lit(static_cast<int64_t>(123456789L));
    auto predicate = PredicateBuilder::Equal(0, "user_id", FieldType::BIGINT, lit);

    ASSERT_OK_AND_ASSIGN(auto result, BucketSelectConverter::Convert(
                                          predicate, {"user_id"}, {arrow::int64()},
                                          BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
    ASSERT_TRUE(result.has_value());

    // Verify
    auto row = BinaryRowGenerator::GenerateRow({static_cast<int64_t>(123456789L)}, pool_.get());
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value());
}

TEST_F(BucketSelectConverterTest, AndWithExtraPredicateStillWorks) {
    // AND(EQUAL(id, 42), GREATER_THAN(value, 100))
    // Only id is bucket key, value is not — should still derive bucket from id
    int32_t num_buckets = 5;
    Literal lit_id(static_cast<int32_t>(42));
    Literal lit_val(static_cast<int32_t>(100));
    auto pred_id = PredicateBuilder::Equal(0, "id", FieldType::INT, lit_id);
    auto pred_val = PredicateBuilder::GreaterThan(1, "value", FieldType::INT, lit_val);
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({pred_id, pred_val}));

    ASSERT_OK_AND_ASSIGN(auto result, BucketSelectConverter::Convert(
                                          predicate, {"id"}, {arrow::int32()},
                                          BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
    ASSERT_TRUE(result.has_value());

    auto row = BinaryRowGenerator::GenerateRow({static_cast<int32_t>(42)}, pool_.get());
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value());
}

TEST_F(BucketSelectConverterTest, TimestampMillisPrecision) {
    // TIMESTAMP with millisecond precision (compact storage, precision=3)
    int32_t num_buckets = 10;
    Timestamp ts = Timestamp::FromEpochMillis(1700000000000L);
    Literal lit(ts);
    auto predicate = PredicateBuilder::Equal(0, "ts", FieldType::TIMESTAMP, lit);

    auto arrow_type = arrow::timestamp(arrow::TimeUnit::MILLI);
    ASSERT_OK_AND_ASSIGN(auto result, BucketSelectConverter::Convert(
                                          predicate, {"ts"}, {arrow_type},
                                          BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
    ASSERT_TRUE(result.has_value());

    // Verify: precision=3 uses compact WriteTimestamp
    auto row = BinaryRowGenerator::GenerateRow({TimestampType(ts, 3)}, pool_.get());
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value());
}

TEST_F(BucketSelectConverterTest, TimestampMicrosPrecision) {
    // TIMESTAMP with microsecond precision (non-compact storage, precision=6)
    int32_t num_buckets = 10;
    Timestamp ts(1700000000000L, 123456);
    Literal lit(ts);
    auto predicate = PredicateBuilder::Equal(0, "ts", FieldType::TIMESTAMP, lit);

    auto arrow_type = arrow::timestamp(arrow::TimeUnit::MICRO);
    ASSERT_OK_AND_ASSIGN(auto result, BucketSelectConverter::Convert(
                                          predicate, {"ts"}, {arrow_type},
                                          BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
    ASSERT_TRUE(result.has_value());

    // Verify: precision=6 uses non-compact WriteTimestamp (different layout than precision=3)
    auto row = BinaryRowGenerator::GenerateRow({TimestampType(ts, 6)}, pool_.get());
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value());
}

TEST_F(BucketSelectConverterTest, DecimalKey) {
    int32_t num_buckets = 10;
    Decimal dec = Decimal::FromUnscaledLong(12345L, 10, 2);
    Literal lit(dec);
    auto predicate = PredicateBuilder::Equal(0, "amount", FieldType::DECIMAL, lit);

    auto arrow_type = arrow::decimal128(10, 2);
    ASSERT_OK_AND_ASSIGN(auto result, BucketSelectConverter::Convert(
                                          predicate, {"amount"}, {arrow_type},
                                          BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
    ASSERT_TRUE(result.has_value());

    // Verify
    auto row = BinaryRowGenerator::GenerateRow({dec}, pool_.get());
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value());
}

}  // namespace paimon::test
