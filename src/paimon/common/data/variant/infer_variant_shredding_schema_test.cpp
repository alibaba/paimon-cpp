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

#include "paimon/common/data/variant/infer_variant_shredding_schema.h"

#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/variant/variant_builder.h"
#include "paimon/common/data/variant/variant_shredding_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class InferVariantShreddingSchemaTest : public ::testing::Test {
 public:
    std::vector<std::shared_ptr<GenericVariant>> Samples(const std::vector<const char*>& jsons) {
        std::vector<std::shared_ptr<GenericVariant>> samples;
        for (const char* json : jsons) {
            if (json == nullptr) {
                samples.push_back(nullptr);
                continue;
            }
            auto variant = GenericVariant::FromJson(json, pool_);
            EXPECT_TRUE(variant.ok()) << variant.status().ToString();
            samples.push_back(variant.value());
        }
        return samples;
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    InferVariantShreddingSchema infer_{/*max_schema_width=*/300, /*max_schema_depth=*/50,
                                       /*min_field_cardinality_ratio=*/0.1};
};

TEST_F(InferVariantShreddingSchemaTest, InferObjectSchema) {
    auto samples = Samples({
        R"({"age": 35, "city": "Hangzhou"})",
        R"({"age": 20, "city": "Beijing", "tags": [1, 2]})",
        nullptr,
        R"({"age": 120000000000, "city": "Shanghai"})",
    });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         infer_.InferColumnShreddingType(samples));
    ASSERT_NE(inferred, nullptr);
    // Integers widen to int64, strings stay, arrays of small ints infer as list<int64>.
    auto expected =
        arrow::struct_({arrow::field("age", arrow::int64()), arrow::field("city", arrow::utf8()),
                        arrow::field("tags", arrow::list(arrow::int64()))});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
    ASSERT_OK(VariantShreddingUtils::VariantShreddingSchema(inferred));
}

TEST_F(InferVariantShreddingSchemaTest, MixedTypesFallToVariant) {
    auto samples = Samples({
        R"({"x": 1, "y": 1.5e0})",
        R"({"x": "string now", "y": 2.5e0})",
    });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         infer_.InferColumnShreddingType(samples));
    ASSERT_NE(inferred, nullptr);
    // x saw both int and string: untyped variant leaf; y stays double (exponent notation
    // parses as double, plain decimals parse as DECIMAL).
    auto expected =
        arrow::struct_({arrow::field("x", arrow::null()), arrow::field("y", arrow::float64())});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
    ASSERT_OK(VariantShreddingUtils::VariantShreddingSchema(inferred));
}

TEST_F(InferVariantShreddingSchemaTest, RareFieldsDropped) {
    std::vector<const char*> jsons;
    for (int i = 0; i < 19; ++i) {
        jsons.push_back("{\"common\": 1}");
    }
    jsons.push_back(R"({"common": 2, "rare": true})");
    auto samples = Samples(jsons);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         infer_.InferColumnShreddingType(samples));
    ASSERT_NE(inferred, nullptr);
    // "rare" appears in 1/20 rows (< 0.1 ratio): dropped from the typed schema.
    auto expected = arrow::struct_({arrow::field("common", arrow::int64())});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, DecimalMerging) {
    auto samples = Samples({
        "{\"d\": 100.99}",
        "{\"d\": 1.5}",
        "{\"d\": 42}",
    });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         infer_.InferColumnShreddingType(samples));
    ASSERT_NE(inferred, nullptr);
    // Decimals merge to a widened decimal (scale 2, enough integer digits), capped at 18 digits.
    auto expected = arrow::struct_({arrow::field("d", arrow::decimal128(18, 2))});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, NoUsefulSchema) {
    auto scalar_samples = Samples({"1", "2"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> scalar_inferred,
                         infer_.InferColumnShreddingType(scalar_samples));
    ASSERT_NE(scalar_inferred, nullptr);
    ASSERT_TRUE(scalar_inferred->Equals(*arrow::int64())) << scalar_inferred->ToString();

    // Conflicting top-level types stay unshredded.
    auto mixed_samples = Samples({"1", "\"a string\""});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> mixed_inferred,
                         infer_.InferColumnShreddingType(mixed_samples));
    ASSERT_EQ(mixed_inferred, nullptr);

    // All-null columns stay unshredded.
    auto null_samples = Samples({nullptr, nullptr});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> null_inferred,
                         infer_.InferColumnShreddingType(null_samples));
    ASSERT_EQ(null_inferred, nullptr);
}

TEST_F(InferVariantShreddingSchemaTest, MaxSchemaWidthLimit) {
    InferVariantShreddingSchema narrow_infer{/*max_schema_width=*/3, /*max_schema_depth=*/50,
                                             /*min_field_cardinality_ratio=*/0.1};
    auto samples = Samples({R"({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         narrow_infer.InferColumnShreddingType(samples));
    if (inferred != nullptr) {
        ASSERT_EQ(inferred->id(), arrow::Type::STRUCT);
        ASSERT_LT(inferred->num_fields(), 5);
    }
}

TEST_F(InferVariantShreddingSchemaTest, MaxSchemaDepthLimit) {
    InferVariantShreddingSchema shallow_infer{/*max_schema_width=*/300, /*max_schema_depth=*/1,
                                              /*min_field_cardinality_ratio=*/0.1};
    auto samples = Samples({R"({"outer": {"inner": 1}})"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         shallow_infer.InferColumnShreddingType(samples));
    ASSERT_NE(inferred, nullptr);
    // Depth 1: the nested object stays an untyped variant leaf.
    auto expected = arrow::struct_({arrow::field("outer", arrow::null())});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, TrailingZeroDecimalNormalized) {
    // GetDecimal strips 100.00 to 1E+2 (scale -2); the inferred type must carry a non-negative
    // scale or reassembling the shredded file would be rejected. After normalization the value
    // is an integral decimal, which finalization widens to int64.
    auto samples = Samples({"100.00"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         infer_.InferColumnShreddingType(samples));
    ASSERT_NE(inferred, nullptr);
    ASSERT_TRUE(inferred->Equals(*arrow::int64())) << inferred->ToString();

    auto mixed = Samples({"100.00", "1.5"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> mixed_inferred,
                         infer_.InferColumnShreddingType(mixed));
    ASSERT_NE(mixed_inferred, nullptr);
    ASSERT_TRUE(mixed_inferred->Equals(*arrow::decimal128(18, 1))) << mixed_inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, TemporalValuesStayUnshredded) {
    // The shredding schema builder rejects temporal leaf types, so inferring them would abort
    // the write; date/timestamp samples must leave the column unshredded.
    VariantBuilder date_builder(/*allow_duplicate_keys=*/false);
    ASSERT_OK(date_builder.AppendDate(19000));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> date_variant, date_builder.Build(pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> date_inferred,
                         infer_.InferColumnShreddingType({date_variant}));
    ASSERT_EQ(date_inferred, nullptr);

    VariantBuilder ts_builder(/*allow_duplicate_keys=*/false);
    ASSERT_OK(ts_builder.AppendTimestamp(1700000000000000));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> ts_variant, ts_builder.Build(pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> ts_inferred,
                         infer_.InferColumnShreddingType({ts_variant}));
    ASSERT_EQ(ts_inferred, nullptr);
}

}  // namespace paimon::test
