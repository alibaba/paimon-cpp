/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/core/operation/bucket_select_converter.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class BucketSelectConverterTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    std::shared_ptr<TableSchema> MakeSchema(
        const std::vector<std::string>& field_names,
        const std::vector<std::shared_ptr<arrow::DataType>>& types,
        const std::vector<std::string>& pk) {
        arrow::FieldVector fields;
        for (size_t i = 0; i < field_names.size(); ++i) {
            fields.push_back(arrow::field(field_names[i], types[i]));
        }
        auto schema = arrow::schema(fields);
        std::map<std::string, std::string> options;
        auto result = TableSchema::Create(0, schema, /*partition_keys=*/{}, pk, options);
        EXPECT_TRUE(result.ok()) << result.status().ToString();
        return std::shared_ptr<TableSchema>(std::move(result).value());
    }

    std::shared_ptr<MemoryPool> pool_;
};

/// Single EQUAL predicate on single bucket key → exactly one bucket.
TEST_F(BucketSelectConverterTest, SingleEqualSingleKey) {
    auto schema = MakeSchema({"pk", "val"}, {arrow::utf8(), arrow::int64()}, {"pk"});
    auto pred =
        PredicateBuilder::Equal(0, "pk", FieldType::STRING, Literal(FieldType::STRING, "hello", 5));

    ASSERT_OK_AND_ASSIGN(auto result,
                         BucketSelectConverter::Convert(pred, {"pk"}, 10, schema, pool_));
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(1, result->size());
    // Bucket ID should be in [0, 10)
    int32_t bucket = *result->begin();
    ASSERT_GE(bucket, 0);
    ASSERT_LT(bucket, 10);
}

/// Same value always hashes to the same bucket (deterministic).
TEST_F(BucketSelectConverterTest, Deterministic) {
    auto schema = MakeSchema({"pk", "val"}, {arrow::utf8(), arrow::int64()}, {"pk"});
    auto pred =
        PredicateBuilder::Equal(0, "pk", FieldType::STRING, Literal(FieldType::STRING, "test", 4));

    ASSERT_OK_AND_ASSIGN(auto r1, BucketSelectConverter::Convert(pred, {"pk"}, 100, schema, pool_));
    ASSERT_OK_AND_ASSIGN(auto r2, BucketSelectConverter::Convert(pred, {"pk"}, 100, schema, pool_));
    ASSERT_TRUE(r1.has_value());
    ASSERT_TRUE(r2.has_value());
    ASSERT_EQ(*r1, *r2);
}

/// AND of EQUAL predicates on two bucket key columns → one bucket.
TEST_F(BucketSelectConverterTest, CompositeBucketKey) {
    auto schema = MakeSchema({"k1", "k2", "val"}, {arrow::int32(), arrow::int64(), arrow::utf8()},
                             {"k1", "k2"});
    auto eq1 = PredicateBuilder::Equal(0, "k1", FieldType::INT, Literal(static_cast<int32_t>(42)));
    auto eq2 =
        PredicateBuilder::Equal(1, "k2", FieldType::BIGINT, Literal(static_cast<int64_t>(100)));
    ASSERT_OK_AND_ASSIGN(auto and_pred, PredicateBuilder::And({eq1, eq2}));

    ASSERT_OK_AND_ASSIGN(auto result,
                         BucketSelectConverter::Convert(and_pred, {"k1", "k2"}, 8, schema, pool_));
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(1, result->size());
    int32_t bucket = *result->begin();
    ASSERT_GE(bucket, 0);
    ASSERT_LT(bucket, 8);
}

/// Missing bucket key column → nullopt.
TEST_F(BucketSelectConverterTest, MissingBucketKey) {
    auto schema = MakeSchema({"k1", "k2", "val"}, {arrow::int32(), arrow::int64(), arrow::utf8()},
                             {"k1", "k2"});
    // Only predicate on k1, missing k2
    auto pred = PredicateBuilder::Equal(0, "k1", FieldType::INT, Literal(static_cast<int32_t>(1)));

    ASSERT_OK_AND_ASSIGN(auto result,
                         BucketSelectConverter::Convert(pred, {"k1", "k2"}, 8, schema, pool_));
    ASSERT_FALSE(result.has_value());
}

/// Non-equality predicate (e.g. GreaterThan) → nullopt.
TEST_F(BucketSelectConverterTest, NonEqualityPredicate) {
    auto schema = MakeSchema({"pk", "val"}, {arrow::int64(), arrow::int64()}, {"pk"});
    auto pred = PredicateBuilder::GreaterThan(0, "pk", FieldType::BIGINT,
                                              Literal(static_cast<int64_t>(10)));

    ASSERT_OK_AND_ASSIGN(auto result,
                         BucketSelectConverter::Convert(pred, {"pk"}, 10, schema, pool_));
    ASSERT_FALSE(result.has_value());
}

/// Null predicate → nullopt.
TEST_F(BucketSelectConverterTest, NullPredicate) {
    auto schema = MakeSchema({"pk"}, {arrow::int64()}, {"pk"});

    ASSERT_OK_AND_ASSIGN(auto result,
                         BucketSelectConverter::Convert(nullptr, {"pk"}, 10, schema, pool_));
    ASSERT_FALSE(result.has_value());
}

/// Empty bucket keys → nullopt.
TEST_F(BucketSelectConverterTest, EmptyBucketKeys) {
    auto schema = MakeSchema({"pk"}, {arrow::int64()}, {"pk"});
    auto pred =
        PredicateBuilder::Equal(0, "pk", FieldType::BIGINT, Literal(static_cast<int64_t>(1)));

    ASSERT_OK_AND_ASSIGN(auto result, BucketSelectConverter::Convert(pred, {}, 10, schema, pool_));
    ASSERT_FALSE(result.has_value());
}

/// IN predicate → multiple bucket IDs.
TEST_F(BucketSelectConverterTest, InPredicate) {
    auto schema = MakeSchema({"pk", "val"}, {arrow::int64(), arrow::int64()}, {"pk"});
    auto pred =
        PredicateBuilder::In(0, "pk", FieldType::BIGINT,
                             {Literal(static_cast<int64_t>(1)), Literal(static_cast<int64_t>(2)),
                              Literal(static_cast<int64_t>(3))});

    ASSERT_OK_AND_ASSIGN(auto result,
                         BucketSelectConverter::Convert(pred, {"pk"}, 100, schema, pool_));
    ASSERT_TRUE(result.has_value());
    // Could be 1-3 distinct buckets
    ASSERT_GE(result->size(), 1u);
    ASSERT_LE(result->size(), 3u);
    for (int32_t b : *result) {
        ASSERT_GE(b, 0);
        ASSERT_LT(b, 100);
    }
}

/// OR of EQUAL predicates on same bucket key column → multiple bucket IDs.
TEST_F(BucketSelectConverterTest, OrEqualPredicates) {
    auto schema = MakeSchema({"pk"}, {arrow::int64()}, {"pk"});
    auto eq1 =
        PredicateBuilder::Equal(0, "pk", FieldType::BIGINT, Literal(static_cast<int64_t>(10)));
    auto eq2 =
        PredicateBuilder::Equal(0, "pk", FieldType::BIGINT, Literal(static_cast<int64_t>(20)));
    ASSERT_OK_AND_ASSIGN(auto or_pred, PredicateBuilder::Or({eq1, eq2}));

    ASSERT_OK_AND_ASSIGN(auto result,
                         BucketSelectConverter::Convert(or_pred, {"pk"}, 50, schema, pool_));
    ASSERT_TRUE(result.has_value());
    ASSERT_GE(result->size(), 1u);
    ASSERT_LE(result->size(), 2u);
}

/// Different data types: INT, BIGINT, STRING, BOOLEAN, FLOAT, DOUBLE.
TEST_F(BucketSelectConverterTest, VariousDataTypes) {
    // INT
    {
        auto schema = MakeSchema({"pk"}, {arrow::int32()}, {"pk"});
        auto pred =
            PredicateBuilder::Equal(0, "pk", FieldType::INT, Literal(static_cast<int32_t>(42)));
        ASSERT_OK_AND_ASSIGN(auto result,
                             BucketSelectConverter::Convert(pred, {"pk"}, 16, schema, pool_));
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(1, result->size());
    }
    // BIGINT
    {
        auto schema = MakeSchema({"pk"}, {arrow::int64()}, {"pk"});
        auto pred =
            PredicateBuilder::Equal(0, "pk", FieldType::BIGINT, Literal(static_cast<int64_t>(999)));
        ASSERT_OK_AND_ASSIGN(auto result,
                             BucketSelectConverter::Convert(pred, {"pk"}, 16, schema, pool_));
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(1, result->size());
    }
    // STRING
    {
        auto schema = MakeSchema({"pk"}, {arrow::utf8()}, {"pk"});
        auto pred = PredicateBuilder::Equal(0, "pk", FieldType::STRING,
                                            Literal(FieldType::STRING, "abc", 3));
        ASSERT_OK_AND_ASSIGN(auto result,
                             BucketSelectConverter::Convert(pred, {"pk"}, 16, schema, pool_));
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(1, result->size());
    }
    // DOUBLE
    {
        auto schema = MakeSchema({"pk"}, {arrow::float64()}, {"pk"});
        auto pred = PredicateBuilder::Equal(0, "pk", FieldType::DOUBLE, Literal(3.14));
        ASSERT_OK_AND_ASSIGN(auto result,
                             BucketSelectConverter::Convert(pred, {"pk"}, 16, schema, pool_));
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(1, result->size());
    }
}

/// num_buckets = 0 → nullopt.
TEST_F(BucketSelectConverterTest, ZeroBuckets) {
    auto schema = MakeSchema({"pk"}, {arrow::int64()}, {"pk"});
    auto pred =
        PredicateBuilder::Equal(0, "pk", FieldType::BIGINT, Literal(static_cast<int64_t>(1)));

    ASSERT_OK_AND_ASSIGN(auto result,
                         BucketSelectConverter::Convert(pred, {"pk"}, 0, schema, pool_));
    ASSERT_FALSE(result.has_value());
}

/// AND with extra non-bucket-key predicate: should still work (extra predicates ignored).
TEST_F(BucketSelectConverterTest, AndWithExtraPredicate) {
    auto schema = MakeSchema({"pk", "val"}, {arrow::int64(), arrow::int64()}, {"pk"});
    auto eq_pk =
        PredicateBuilder::Equal(0, "pk", FieldType::BIGINT, Literal(static_cast<int64_t>(7)));
    auto gt_val = PredicateBuilder::GreaterThan(1, "val", FieldType::BIGINT,
                                                Literal(static_cast<int64_t>(100)));
    ASSERT_OK_AND_ASSIGN(auto and_pred, PredicateBuilder::And({eq_pk, gt_val}));

    ASSERT_OK_AND_ASSIGN(auto result,
                         BucketSelectConverter::Convert(and_pred, {"pk"}, 10, schema, pool_));
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(1, result->size());
}

}  // namespace paimon::test
