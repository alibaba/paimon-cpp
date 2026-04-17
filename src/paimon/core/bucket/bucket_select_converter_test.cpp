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
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/bucket/default_bucket_function.h"
#include "paimon/core/bucket/mod_bucket_function.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
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

    auto result =
        BucketSelectConverter::Convert(predicate, {"id"}, {FieldType::INT}, {arrow::int32()},
                                       BucketFunctionType::DEFAULT, num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());

    // Verify by computing the expected bucket manually
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool_.get());
    writer.WriteInt(0, 42);
    writer.Complete();
    DefaultBucketFunction func;
    int32_t expected_bucket = func.Bucket(row, num_buckets);
    ASSERT_EQ(expected_bucket, result.value().value());
}

TEST_F(BucketSelectConverterTest, SingleStringEqualDefault) {
    int32_t num_buckets = 8;
    std::string val = "hello_world";
    Literal lit(FieldType::STRING, val.c_str(), val.size());
    auto predicate = PredicateBuilder::Equal(0, "name", FieldType::STRING, lit);

    auto result =
        BucketSelectConverter::Convert(predicate, {"name"}, {FieldType::STRING}, {arrow::utf8()},
                                       BucketFunctionType::DEFAULT, num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());

    // Verify
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool_.get());
    writer.WriteStringView(0, std::string_view(val));
    writer.Complete();
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value().value());
}

TEST_F(BucketSelectConverterTest, MultiKeyAndPredicate) {
    int32_t num_buckets = 5;
    Literal lit_id(static_cast<int32_t>(100));
    Literal lit_name(FieldType::STRING, "test", 4);
    auto pred_id = PredicateBuilder::Equal(0, "id", FieldType::INT, lit_id);
    auto pred_name = PredicateBuilder::Equal(1, "name", FieldType::STRING, lit_name);
    auto predicate = PredicateBuilder::And({pred_id, pred_name});
    ASSERT_TRUE(predicate.ok());

    auto result = BucketSelectConverter::Convert(
        predicate.value(), {"id", "name"}, {FieldType::INT, FieldType::STRING},
        {arrow::int32(), arrow::utf8()}, BucketFunctionType::DEFAULT, num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());

    // Verify
    BinaryRow row(2);
    BinaryRowWriter writer(&row, 0, pool_.get());
    writer.WriteInt(0, 100);
    writer.WriteStringView(1, std::string_view("test", 4));
    writer.Complete();
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value().value());
}

TEST_F(BucketSelectConverterTest, MissingBucketKeyReturnsNullopt) {
    int32_t num_buckets = 5;
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    auto result = BucketSelectConverter::Convert(
        predicate, {"id", "name"}, {FieldType::INT, FieldType::STRING},
        {arrow::int32(), arrow::utf8()}, BucketFunctionType::DEFAULT, num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(BucketSelectConverterTest, NonEqualPredicateReturnsNullopt) {
    int32_t num_buckets = 5;
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::GreaterThan(0, "id", FieldType::INT, lit);

    auto result =
        BucketSelectConverter::Convert(predicate, {"id"}, {FieldType::INT}, {arrow::int32()},
                                       BucketFunctionType::DEFAULT, num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(BucketSelectConverterTest, OrPredicateReturnsNullopt) {
    int32_t num_buckets = 5;
    Literal lit1(static_cast<int32_t>(1));
    Literal lit2(static_cast<int32_t>(2));
    auto pred1 = PredicateBuilder::Equal(0, "id", FieldType::INT, lit1);
    auto pred2 = PredicateBuilder::Equal(0, "id", FieldType::INT, lit2);
    auto predicate = PredicateBuilder::Or({pred1, pred2});
    ASSERT_TRUE(predicate.ok());

    auto result = BucketSelectConverter::Convert(predicate.value(), {"id"}, {FieldType::INT},
                                                 {arrow::int32()}, BucketFunctionType::DEFAULT,
                                                 num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(BucketSelectConverterTest, ModBucketFunction) {
    int32_t num_buckets = 7;
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    auto result =
        BucketSelectConverter::Convert(predicate, {"id"}, {FieldType::INT}, {arrow::int32()},
                                       BucketFunctionType::MOD, num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());

    // Verify: MOD function uses floorMod
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool_.get());
    writer.WriteInt(0, 42);
    writer.Complete();
    auto mod_func = ModBucketFunction::Create(FieldType::INT);
    ASSERT_TRUE(mod_func.ok());
    ASSERT_EQ(mod_func.value()->Bucket(row, num_buckets), result.value().value());
}

TEST_F(BucketSelectConverterTest, NullLiteralReturnsNullopt) {
    int32_t num_buckets = 5;
    Literal lit(FieldType::INT);  // null literal
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    auto result =
        BucketSelectConverter::Convert(predicate, {"id"}, {FieldType::INT}, {arrow::int32()},
                                       BucketFunctionType::DEFAULT, num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(BucketSelectConverterTest, DynamicBucketModeReturnsNullopt) {
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    auto result =
        BucketSelectConverter::Convert(predicate, {"id"}, {FieldType::INT}, {arrow::int32()},
                                       BucketFunctionType::DEFAULT, -1, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(BucketSelectConverterTest, NullPredicateReturnsNullopt) {
    auto result =
        BucketSelectConverter::Convert(nullptr, {"id"}, {FieldType::INT}, {arrow::int32()},
                                       BucketFunctionType::DEFAULT, 5, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(BucketSelectConverterTest, BigintKeyDefault) {
    int32_t num_buckets = 16;
    Literal lit(static_cast<int64_t>(123456789L));
    auto predicate = PredicateBuilder::Equal(0, "user_id", FieldType::BIGINT, lit);

    auto result = BucketSelectConverter::Convert(predicate, {"user_id"}, {FieldType::BIGINT},
                                                 {arrow::int64()}, BucketFunctionType::DEFAULT,
                                                 num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());

    // Verify
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool_.get());
    writer.WriteLong(0, 123456789L);
    writer.Complete();
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value().value());
}

TEST_F(BucketSelectConverterTest, AndWithExtraPredicateStillWorks) {
    // AND(EQUAL(id, 42), GREATER_THAN(value, 100))
    // Only id is bucket key, value is not — should still derive bucket from id
    int32_t num_buckets = 5;
    Literal lit_id(static_cast<int32_t>(42));
    Literal lit_val(static_cast<int32_t>(100));
    auto pred_id = PredicateBuilder::Equal(0, "id", FieldType::INT, lit_id);
    auto pred_val = PredicateBuilder::GreaterThan(1, "value", FieldType::INT, lit_val);
    auto predicate = PredicateBuilder::And({pred_id, pred_val});
    ASSERT_TRUE(predicate.ok());

    auto result = BucketSelectConverter::Convert(predicate.value(), {"id"}, {FieldType::INT},
                                                 {arrow::int32()}, BucketFunctionType::DEFAULT,
                                                 num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());

    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool_.get());
    writer.WriteInt(0, 42);
    writer.Complete();
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value().value());
}

TEST_F(BucketSelectConverterTest, TimestampMillisPrecision) {
    // TIMESTAMP with millisecond precision (compact storage, precision=3)
    int32_t num_buckets = 10;
    Timestamp ts = Timestamp::FromEpochMillis(1700000000000L);
    Literal lit(ts);
    auto predicate = PredicateBuilder::Equal(0, "ts", FieldType::TIMESTAMP, lit);

    auto arrow_type = arrow::timestamp(arrow::TimeUnit::MILLI);
    auto result =
        BucketSelectConverter::Convert(predicate, {"ts"}, {FieldType::TIMESTAMP}, {arrow_type},
                                       BucketFunctionType::DEFAULT, num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());

    // Verify: precision=3 uses compact WriteTimestamp
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool_.get());
    writer.WriteTimestamp(0, ts, /*precision=*/3);
    writer.Complete();
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value().value());
}

TEST_F(BucketSelectConverterTest, TimestampMicrosPrecision) {
    // TIMESTAMP with microsecond precision (non-compact storage, precision=6)
    int32_t num_buckets = 10;
    Timestamp ts(1700000000000L, 123456);
    Literal lit(ts);
    auto predicate = PredicateBuilder::Equal(0, "ts", FieldType::TIMESTAMP, lit);

    auto arrow_type = arrow::timestamp(arrow::TimeUnit::MICRO);
    auto result =
        BucketSelectConverter::Convert(predicate, {"ts"}, {FieldType::TIMESTAMP}, {arrow_type},
                                       BucketFunctionType::DEFAULT, num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());

    // Verify: precision=6 uses non-compact WriteTimestamp (different layout than precision=3)
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool_.get());
    writer.WriteTimestamp(0, ts, /*precision=*/6);
    writer.Complete();
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value().value());
}

TEST_F(BucketSelectConverterTest, TimestampDifferentPrecisionProducesDifferentBucket) {
    // Verify that precision=3 and precision=6 produce different BinaryRow layouts
    // and thus potentially different bucket assignments
    Timestamp ts(1700000000000L, 123456);

    BinaryRow row_compact(1);
    BinaryRowWriter writer_compact(&row_compact, 0, pool_.get());
    writer_compact.WriteTimestamp(0, ts, /*precision=*/3);
    writer_compact.Complete();

    BinaryRow row_non_compact(1);
    BinaryRowWriter writer_non_compact(&row_non_compact, 0, pool_.get());
    writer_non_compact.WriteTimestamp(0, ts, /*precision=*/6);
    writer_non_compact.Complete();

    // The two rows have different memory layouts, so hash codes should differ
    ASSERT_NE(row_compact.HashCode(), row_non_compact.HashCode());
}

TEST_F(BucketSelectConverterTest, DecimalKey) {
    int32_t num_buckets = 10;
    Decimal dec = Decimal::FromUnscaledLong(12345L, 10, 2);
    Literal lit(dec);
    auto predicate = PredicateBuilder::Equal(0, "amount", FieldType::DECIMAL, lit);

    auto arrow_type = arrow::decimal128(10, 2);
    auto result =
        BucketSelectConverter::Convert(predicate, {"amount"}, {FieldType::DECIMAL}, {arrow_type},
                                       BucketFunctionType::DEFAULT, num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());

    // Verify
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool_.get());
    writer.WriteDecimal(0, dec, 10);
    writer.Complete();
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value().value());
}

}  // namespace paimon::test
