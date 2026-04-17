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

#include <cmath>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/bucket/default_bucket_function.h"
#include "paimon/core/bucket/mod_bucket_function.h"
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
    // EQUAL predicate on single INT bucket key with DEFAULT bucket function
    int32_t num_buckets = 10;
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    auto result = BucketSelectConverter::Convert(predicate, {"id"}, {FieldType::INT},
                                                  BucketFunctionType::DEFAULT, num_buckets,
                                                  pool_.get());
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
    // EQUAL predicate on single STRING bucket key
    int32_t num_buckets = 8;
    std::string val = "hello_world";
    Literal lit(FieldType::STRING, val.c_str(), val.size());
    auto predicate = PredicateBuilder::Equal(0, "name", FieldType::STRING, lit);

    auto result = BucketSelectConverter::Convert(predicate, {"name"}, {FieldType::STRING},
                                                  BucketFunctionType::DEFAULT, num_buckets,
                                                  pool_.get());
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
    // AND of two EQUAL predicates on two bucket key fields
    int32_t num_buckets = 5;
    Literal lit_id(static_cast<int32_t>(100));
    Literal lit_name(FieldType::STRING, "test", 4);
    auto pred_id = PredicateBuilder::Equal(0, "id", FieldType::INT, lit_id);
    auto pred_name = PredicateBuilder::Equal(1, "name", FieldType::STRING, lit_name);
    auto predicate = PredicateBuilder::And({pred_id, pred_name});
    ASSERT_TRUE(predicate.ok());

    auto result = BucketSelectConverter::Convert(
        predicate.value(), {"id", "name"}, {FieldType::INT, FieldType::STRING},
        BucketFunctionType::DEFAULT, num_buckets, pool_.get());
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
    // Only one bucket key has EQUAL, but table has two bucket keys
    int32_t num_buckets = 5;
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    auto result = BucketSelectConverter::Convert(
        predicate, {"id", "name"}, {FieldType::INT, FieldType::STRING},
        BucketFunctionType::DEFAULT, num_buckets, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(BucketSelectConverterTest, NonEqualPredicateReturnsNullopt) {
    // GREATER_THAN predicate cannot derive bucket
    int32_t num_buckets = 5;
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::GreaterThan(0, "id", FieldType::INT, lit);

    auto result = BucketSelectConverter::Convert(predicate, {"id"}, {FieldType::INT},
                                                  BucketFunctionType::DEFAULT, num_buckets,
                                                  pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(BucketSelectConverterTest, OrPredicateReturnsNullopt) {
    // OR at top level cannot derive bucket
    int32_t num_buckets = 5;
    Literal lit1(static_cast<int32_t>(1));
    Literal lit2(static_cast<int32_t>(2));
    auto pred1 = PredicateBuilder::Equal(0, "id", FieldType::INT, lit1);
    auto pred2 = PredicateBuilder::Equal(0, "id", FieldType::INT, lit2);
    auto predicate = PredicateBuilder::Or({pred1, pred2});
    ASSERT_TRUE(predicate.ok());

    auto result = BucketSelectConverter::Convert(predicate.value(), {"id"}, {FieldType::INT},
                                                  BucketFunctionType::DEFAULT, num_buckets,
                                                  pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(BucketSelectConverterTest, ModBucketFunction) {
    // MOD bucket function with single INT key
    int32_t num_buckets = 7;
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    auto result = BucketSelectConverter::Convert(predicate, {"id"}, {FieldType::INT},
                                                  BucketFunctionType::MOD, num_buckets,
                                                  pool_.get());
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
    // Null literal cannot derive bucket
    int32_t num_buckets = 5;
    Literal lit(FieldType::INT);  // null literal
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    auto result = BucketSelectConverter::Convert(predicate, {"id"}, {FieldType::INT},
                                                  BucketFunctionType::DEFAULT, num_buckets,
                                                  pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(BucketSelectConverterTest, DynamicBucketModeReturnsNullopt) {
    // num_buckets <= 0 means dynamic bucket mode, cannot derive
    Literal lit(static_cast<int32_t>(42));
    auto predicate = PredicateBuilder::Equal(0, "id", FieldType::INT, lit);

    auto result = BucketSelectConverter::Convert(predicate, {"id"}, {FieldType::INT},
                                                  BucketFunctionType::DEFAULT, -1, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(BucketSelectConverterTest, NullPredicateReturnsNullopt) {
    auto result = BucketSelectConverter::Convert(nullptr, {"id"}, {FieldType::INT},
                                                  BucketFunctionType::DEFAULT, 5, pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(BucketSelectConverterTest, BigintKeyDefault) {
    int32_t num_buckets = 16;
    Literal lit(static_cast<int64_t>(123456789L));
    auto predicate = PredicateBuilder::Equal(0, "user_id", FieldType::BIGINT, lit);

    auto result = BucketSelectConverter::Convert(predicate, {"user_id"}, {FieldType::BIGINT},
                                                  BucketFunctionType::DEFAULT, num_buckets,
                                                  pool_.get());
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
    // Only id is bucket key, value is not - should still derive bucket from id
    int32_t num_buckets = 5;
    Literal lit_id(static_cast<int32_t>(42));
    Literal lit_val(static_cast<int32_t>(100));
    auto pred_id = PredicateBuilder::Equal(0, "id", FieldType::INT, lit_id);
    auto pred_val = PredicateBuilder::GreaterThan(1, "value", FieldType::INT, lit_val);
    auto predicate = PredicateBuilder::And({pred_id, pred_val});
    ASSERT_TRUE(predicate.ok());

    auto result = BucketSelectConverter::Convert(predicate.value(), {"id"}, {FieldType::INT},
                                                  BucketFunctionType::DEFAULT, num_buckets,
                                                  pool_.get());
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());

    BinaryRow row(1);
    BinaryRowWriter writer(&row, 0, pool_.get());
    writer.WriteInt(0, 42);
    writer.Complete();
    DefaultBucketFunction func;
    ASSERT_EQ(func.Bucket(row, num_buckets), result.value().value());
}

}  // namespace paimon::test
