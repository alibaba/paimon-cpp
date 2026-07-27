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

#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/core/bucket/default_bucket_function.h"
#include "paimon/core/bucket/hive_bucket_function.h"
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
 public:
    void AssertDefaultBucket(FieldType field_type, const Literal& literal,
                             const std::shared_ptr<arrow::DataType>& arrow_type,
                             const BinaryRowGenerator::ValueType& values,
                             int32_t num_buckets = 17) const {
        auto predicate = PredicateBuilder::Equal(0, "key", field_type, literal);

        ASSERT_OK_AND_ASSIGN(
            std::optional<int32_t> selected_bucket,
            BucketSelectConverter::Convert(predicate, {"key"}, {arrow_type},
                                           BucketFunctionType::DEFAULT, num_buckets, pool_.get()));
        ASSERT_TRUE(selected_bucket.has_value());

        BinaryRow row = BinaryRowGenerator::GenerateRow(values, pool_.get());
        DefaultBucketFunction function;
        ASSERT_EQ(function.Bucket(row, num_buckets), selected_bucket.value());
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(BucketSelectConverterTest, SingleStringEqualDefault) {
    std::string value = "hello_world";
    AssertDefaultBucket(FieldType::STRING, Literal(FieldType::STRING, value.c_str(), value.size()),
                        arrow::utf8(), {value}, 8);
}

TEST_F(BucketSelectConverterTest, PrimitiveKeyTypes) {
    AssertDefaultBucket(FieldType::BOOLEAN, Literal(true), arrow::boolean(), {true});
    AssertDefaultBucket(FieldType::TINYINT, Literal(static_cast<int8_t>(-12)), arrow::int8(),
                        {static_cast<int8_t>(-12)});
    AssertDefaultBucket(FieldType::SMALLINT, Literal(static_cast<int16_t>(1234)), arrow::int16(),
                        {static_cast<int16_t>(1234)});
    AssertDefaultBucket(FieldType::INT, Literal(static_cast<int32_t>(42)), arrow::int32(),
                        {static_cast<int32_t>(42)}, 10);
    AssertDefaultBucket(FieldType::BIGINT, Literal(static_cast<int64_t>(123456789L)),
                        arrow::int64(), {static_cast<int64_t>(123456789L)}, 16);
    AssertDefaultBucket(FieldType::FLOAT, Literal(1.25F), arrow::float32(), {1.25F});
    AssertDefaultBucket(FieldType::DOUBLE, Literal(-123.5), arrow::float64(), {-123.5});
}

TEST_F(BucketSelectConverterTest, TimestampMillisPrecision) {
    // TIMESTAMP with millisecond precision (compact storage, precision=3)
    Timestamp ts = Timestamp::FromEpochMillis(1700000000000L);
    AssertDefaultBucket(FieldType::TIMESTAMP, Literal(ts), arrow::timestamp(arrow::TimeUnit::MILLI),
                        {TimestampType(ts, 3)}, 10);
}

TEST_F(BucketSelectConverterTest, TimestampMicrosPrecision) {
    // TIMESTAMP with microsecond precision (non-compact storage, precision=6)
    Timestamp ts(1700000000000L, 123456);
    AssertDefaultBucket(FieldType::TIMESTAMP, Literal(ts), arrow::timestamp(arrow::TimeUnit::MICRO),
                        {TimestampType(ts, 6)}, 10);
}

TEST_F(BucketSelectConverterTest, DecimalKey) {
    Decimal decimal = Decimal::FromUnscaledLong(12345L, 10, 2);
    AssertDefaultBucket(FieldType::DECIMAL, Literal(decimal), arrow::decimal128(10, 2), {decimal},
                        10);
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

TEST_F(BucketSelectConverterTest, HiveBucketFunctionWithDecimal) {
    constexpr int32_t NUM_BUCKETS = 11;
    Decimal decimal = Decimal::FromUnscaledLong(12345L, 10, 2);
    auto int_predicate =
        PredicateBuilder::Equal(0, "id", FieldType::INT, Literal(static_cast<int32_t>(7)));
    auto decimal_predicate =
        PredicateBuilder::Equal(1, "amount", FieldType::DECIMAL, Literal(decimal));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({int_predicate, decimal_predicate}));

    ASSERT_OK_AND_ASSIGN(
        std::optional<int32_t> selected_bucket,
        BucketSelectConverter::Convert(predicate, {"id", "amount"},
                                       {arrow::int32(), arrow::decimal128(10, 2)},
                                       BucketFunctionType::HIVE, NUM_BUCKETS, pool_.get()));
    ASSERT_TRUE(selected_bucket.has_value());

    BinaryRow row =
        BinaryRowGenerator::GenerateRow({static_cast<int32_t>(7), decimal}, pool_.get());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HiveBucketFunction> function,
                         HiveBucketFunction::Create({HiveFieldInfo(FieldType::INT),
                                                     HiveFieldInfo(FieldType::DECIMAL, 10, 2)}));
    ASSERT_EQ(function->Bucket(row, NUM_BUCKETS), selected_bucket.value());
}

TEST_F(BucketSelectConverterTest, UnsupportedFieldTypeReturnsError) {
    auto predicate =
        PredicateBuilder::Equal(0, "items", FieldType::ARRAY, Literal(static_cast<int32_t>(42)));

    Result<std::optional<int32_t>> result =
        BucketSelectConverter::Convert(predicate, {"items"}, {arrow::list(arrow::int32())},
                                       BucketFunctionType::DEFAULT, 5, pool_.get());
    ASSERT_NOK_WITH_MSG(result.status(), "unsupported field type");
}

TEST_F(BucketSelectConverterTest, ModBucketFunctionWithMultipleKeysReturnsError) {
    auto id_predicate =
        PredicateBuilder::Equal(0, "id", FieldType::INT, Literal(static_cast<int32_t>(42)));
    auto region_predicate =
        PredicateBuilder::Equal(1, "region", FieldType::INT, Literal(static_cast<int32_t>(1)));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({id_predicate, region_predicate}));

    Result<std::optional<int32_t>> result = BucketSelectConverter::Convert(
        predicate, {"id", "region"}, {arrow::int32(), arrow::int32()}, BucketFunctionType::MOD, 5,
        pool_.get());
    ASSERT_NOK_WITH_MSG(result.status(),
                        "MOD bucket function requires exactly one bucket key field");
}

}  // namespace paimon::test
