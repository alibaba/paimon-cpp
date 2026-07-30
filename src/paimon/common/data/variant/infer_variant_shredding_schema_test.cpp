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

#include <functional>
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
    // Infers one column with a fresh shared-width budget.
    static Result<std::shared_ptr<arrow::DataType>> InferColumn(
        const InferVariantShreddingSchema& infer,
        const std::vector<std::shared_ptr<GenericVariant>>& samples) {
        InferVariantShreddingSchema::MaxFields max_fields = infer.CreateMaxFieldsBudget();
        return infer.InferColumnShreddingType(samples, &max_fields);
    }

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

    // Builds a single variant using the direct append API, which can encode types (float, binary,
    // uuid, decimals with a specific scale) that JSON parsing never produces.
    std::shared_ptr<GenericVariant> BuildVariant(
        const std::function<Status(VariantBuilder&)>& append) {
        VariantBuilder builder(/*allow_duplicate_keys=*/false);
        Status st = append(builder);
        EXPECT_TRUE(st.ok()) << st.ToString();
        auto result = builder.Build(pool_);
        EXPECT_TRUE(result.ok()) << result.status().ToString();
        return result.value();
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<arrow::Schema> empty_logical_schema_ = arrow::schema(arrow::FieldVector{});
    InferVariantShreddingSchema infer_{empty_logical_schema_, pool_,
                                       /*max_schema_width=*/300, /*max_schema_depth=*/50,
                                       /*min_field_cardinality_ratio=*/0.1};
};

TEST_F(InferVariantShreddingSchemaTest, InferObjectSchema) {
    auto samples = Samples({
        R"({"age": 35, "city": "Hangzhou"})",
        R"({"age": 20, "city": "Beijing", "tags": [1, 2]})",
        nullptr,
        R"({"age": 120000000000, "city": "Shanghai"})",
    });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
    ASSERT_NE(inferred, nullptr);
    // Integers widen to int64, strings stay, arrays of small ints infer as list<int64>.
    auto expected =
        arrow::struct_({arrow::field("age", arrow::int64()), arrow::field("city", arrow::utf8()),
                        arrow::field("tags", arrow::list(arrow::int64()))});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
    ASSERT_OK(VariantShreddingUtils::VariantShreddingSchema(inferred));
}

TEST_F(InferVariantShreddingSchemaTest, NullObjectFieldMergesWithTypedValue) {
    auto samples = Samples({
        R"({"a": 1, "b": null})",
        R"({"a": 2, "b": 3})",
    });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
    auto expected =
        arrow::struct_({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, MixedTypesFallToVariant) {
    auto samples = Samples({
        R"({"x": 1, "y": 1.5e0})",
        R"({"x": "string now", "y": 2.5e0})",
    });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
    ASSERT_NE(inferred, nullptr);
    // "rare" appears in 1/20 rows (< 0.1 ratio): dropped from the typed schema.
    auto expected = arrow::struct_({arrow::field("common", arrow::int64())});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, FieldCardinalityAdmissionThreshold) {
    std::vector<const char*> jsons = {
        R"({"common": 1, "rare": 99})",
        R"({"common": 2, "rare": 88})",
        R"({"common": 3})",
        R"({"common": 4})",
        R"({"common": 5})",
        R"({"common": 6})",
        R"({"common": 7})",
        R"({"common": 8})",
        R"({"common": 9})",
        R"({"common": 10})",
    };
    auto samples = Samples(jsons);

    InferVariantShreddingSchema permissive{empty_logical_schema_, pool_,
                                           /*max_schema_width=*/300,
                                           /*max_schema_depth=*/50,
                                           /*min_field_cardinality_ratio=*/0.1};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> admitted,
                         InferColumn(permissive, samples));
    auto expected_admitted = arrow::struct_(
        {arrow::field("common", arrow::int64()), arrow::field("rare", arrow::int64())});
    ASSERT_TRUE(admitted->Equals(*expected_admitted)) << admitted->ToString();

    InferVariantShreddingSchema strict{empty_logical_schema_, pool_,
                                       /*max_schema_width=*/300,
                                       /*max_schema_depth=*/50,
                                       /*min_field_cardinality_ratio=*/0.25};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> rejected, InferColumn(strict, samples));
    auto expected_rejected = arrow::struct_({arrow::field("common", arrow::int64())});
    ASSERT_TRUE(rejected->Equals(*expected_rejected)) << rejected->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, DecimalMerging) {
    auto samples = Samples({
        "{\"d\": 100.99}",
        "{\"d\": 1.5}",
        "{\"d\": 42}",
    });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
    ASSERT_NE(inferred, nullptr);
    // Decimals merge to a widened decimal (scale 2, enough integer digits), capped at 18 digits.
    auto expected = arrow::struct_({arrow::field("d", arrow::decimal128(18, 2))});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, AllPrimitiveTypes) {
    auto samples = Samples({R"({
        "string": "test",
        "long": 123456789,
        "double": 3.14159,
        "boolean": true,
        "null": null
    })"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
    auto expected = arrow::struct_({
        arrow::field("boolean", arrow::boolean()),
        arrow::field("double", arrow::decimal128(18, 5)),
        arrow::field("long", arrow::int64()),
        arrow::field("null", arrow::null()),
        arrow::field("string", arrow::utf8()),
    });
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, MixedArrayElementTypesFallToVariant) {
    auto samples = Samples({
        R"({"arr": [1, 2, 3]})",
        R"({"arr": ["a", "b", "c"]})",
        R"({"arr": [true, false, true]})",
        R"({"arr": [1, "mixed", true]})",
    });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
    auto expected = arrow::struct_({arrow::field("arr", arrow::list(arrow::null()))});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, NullInNestedArrays) {
    auto samples = Samples({
        R"({"arr": [1, 2, 3, null, 5]})",
        R"({"arr": [null, null, null]})",
        R"({"arr": [10, null, 20, null, 30]})",
        R"({"arr": null})",
    });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
    auto expected = arrow::struct_({arrow::field("arr", arrow::list(arrow::null()))});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, LargeDatasetWithManyFields) {
    std::vector<std::string> documents;
    documents.reserve(500);
    for (int32_t row = 0; row < 500; ++row) {
        std::string json = "{";
        for (int32_t field = 0; field < 50; ++field) {
            if (field > 0) {
                json += ",";
            }
            json += "\"field" + std::to_string(field) +
                    "\":" + std::to_string((row * 50 + field) % 1000);
        }
        json += "}";
        documents.push_back(std::move(json));
    }
    std::vector<const char*> jsons;
    jsons.reserve(documents.size());
    for (const std::string& document : documents) {
        jsons.push_back(document.c_str());
    }

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         InferColumn(infer_, Samples(jsons)));
    ASSERT_EQ(arrow::Type::STRUCT, inferred->id());
    const auto& struct_type = static_cast<const arrow::StructType&>(*inferred);
    ASSERT_EQ(50, struct_type.num_fields());
    for (int32_t field = 0; field < 50; ++field) {
        auto inferred_field = struct_type.GetFieldByName("field" + std::to_string(field));
        ASSERT_NE(nullptr, inferred_field);
        ASSERT_TRUE(inferred_field->type()->Equals(*arrow::int64()));
    }
}

TEST_F(InferVariantShreddingSchemaTest, NoUsefulSchema) {
    auto scalar_samples = Samples({"1", "2"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> scalar_inferred,
                         InferColumn(infer_, scalar_samples));
    ASSERT_NE(scalar_inferred, nullptr);
    ASSERT_TRUE(scalar_inferred->Equals(*arrow::int64())) << scalar_inferred->ToString();

    // Conflicting top-level types stay unshredded.
    auto mixed_samples = Samples({"1", "\"a string\""});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> mixed_inferred,
                         InferColumn(infer_, mixed_samples));
    ASSERT_EQ(mixed_inferred, nullptr);

    // All-null columns stay unshredded.
    auto null_samples = Samples({nullptr, nullptr});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> null_inferred,
                         InferColumn(infer_, null_samples));
    ASSERT_EQ(null_inferred, nullptr);
}

TEST_F(InferVariantShreddingSchemaTest, MaxSchemaWidthLimit) {
    InferVariantShreddingSchema narrow_infer{empty_logical_schema_, pool_,
                                             /*max_schema_width=*/3, /*max_schema_depth=*/50,
                                             /*min_field_cardinality_ratio=*/0.1};
    auto samples = Samples({R"({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         InferColumn(narrow_infer, samples));
    ASSERT_NE(inferred, nullptr);
    // Budget of 3: the root object costs 1, "a" costs 2 (value + typed_value); the remaining
    // fields exceed the budget and are dropped from the typed schema.
    auto expected = arrow::struct_({arrow::field("a", arrow::int64())});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();

    // One budget serves all variant columns of a schema: after the first column consumes it, a
    // second column cannot shred anymore.
    InferVariantShreddingSchema::MaxFields max_fields = narrow_infer.CreateMaxFieldsBudget();
    auto first_samples = Samples({R"({"a": 1, "b": 2})"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> first,
                         narrow_infer.InferColumnShreddingType(first_samples, &max_fields));
    ASSERT_NE(first, nullptr);
    ASSERT_TRUE(first->Equals(*expected)) << first->ToString();
    auto second_samples = Samples({R"({"c": 1})"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> second,
                         narrow_infer.InferColumnShreddingType(second_samples, &max_fields));
    ASSERT_EQ(second, nullptr);
}

TEST_F(InferVariantShreddingSchemaTest, MaxSchemaDepthLimit) {
    InferVariantShreddingSchema shallow_infer{empty_logical_schema_, pool_,
                                              /*max_schema_width=*/300, /*max_schema_depth=*/1,
                                              /*min_field_cardinality_ratio=*/0.1};
    auto samples = Samples({R"({"outer": {"inner": 1}})"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         InferColumn(shallow_infer, samples));
    ASSERT_NE(inferred, nullptr);
    // Depth 1: the nested object stays an untyped variant leaf.
    auto expected = arrow::struct_({arrow::field("outer", arrow::null())});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, DeepNestedObjectSchema) {
    auto samples = Samples({R"({"level1":{"level2":{"level3":{"value":42}}}})"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
    auto expected = arrow::struct_({arrow::field(
        "level1",
        arrow::struct_({arrow::field(
            "level2",
            arrow::struct_({arrow::field(
                "level3", arrow::struct_({arrow::field("value", arrow::int64())}))}))}))});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, AdaptivePreviousSelectionHonorsSharedWidthBudget) {
    InferVariantShreddingSchema narrow_infer{empty_logical_schema_, pool_,
                                             /*max_schema_width=*/6, /*max_schema_depth=*/50,
                                             /*min_field_cardinality_ratio=*/0.1};
    // A scalar-to-object transition can leave the combined evidence untyped while selecting the
    // current object schema for writing.
    InferVariantShreddingSchema::ColumnEvidence previous_evidence;
    previous_evidence.root_value_count = 1;
    auto previous_selected =
        arrow::struct_({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())});
    InferVariantShreddingSchema::MaxFields max_fields = narrow_infer.CreateMaxFieldsBudget();
    // In the preceding file, an earlier Variant column used one slot, leaving five slots for this
    // column's root, a and b. In this file that earlier column expanded to use three slots, so only
    // three remain and the previous selection must be trimmed.
    max_fields.remaining = 3;

    ASSERT_OK_AND_ASSIGN(
        InferVariantShreddingSchema::AdaptiveColumnResult result,
        narrow_infer.InferAdaptiveColumn(
            previous_evidence, previous_selected, /*samples=*/{}, /*effective_sample_size=*/10,
            /*admission_ratio=*/0.1, /*retention_ratio=*/0.05, &max_fields));
    auto expected = arrow::struct_({arrow::field("a", arrow::int64())});
    ASSERT_TRUE(result.selected_schema->Equals(*expected)) << result.selected_schema->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, TrailingZeroDecimalNormalized) {
    // GetDecimal strips 100.00 to 1E+2 (scale -2); the inferred type must carry a non-negative
    // scale or reassembling the shredded file would be rejected. After normalization the value
    // is an integral decimal, which finalization widens to int64.
    auto samples = Samples({"100.00"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
    ASSERT_NE(inferred, nullptr);
    ASSERT_TRUE(inferred->Equals(*arrow::int64())) << inferred->ToString();

    auto mixed = Samples({"100.00", "1.5"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> mixed_inferred,
                         InferColumn(infer_, mixed));
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
                         InferColumn(infer_, {date_variant}));
    ASSERT_EQ(date_inferred, nullptr);

    VariantBuilder ts_builder(/*allow_duplicate_keys=*/false);
    ASSERT_OK(ts_builder.AppendTimestamp(1700000000000000));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> ts_variant, ts_builder.Build(pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> ts_inferred,
                         InferColumn(infer_, {ts_variant}));
    ASSERT_EQ(ts_inferred, nullptr);
}

TEST_F(InferVariantShreddingSchemaTest, MergeObjectsWithDisjointFields) {
    // Objects whose keys interleave exercise both single-side merge branches (field only in the
    // first object, and field only in the second).
    auto samples = Samples({R"({"a": 1, "c": 3})", R"({"b": 2, "c": 4})"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
    ASSERT_NE(inferred, nullptr);
    auto expected =
        arrow::struct_({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
                        arrow::field("c", arrow::int64())});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, VariantNullSamplesAndFields) {
    // A top-level variant null (JSON `null`, not a missing sample) merges away, leaving the other
    // sample's inferred type.
    auto scalar_and_null = Samples({"1", "null"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> merged,
                         InferColumn(infer_, scalar_and_null));
    ASSERT_NE(merged, nullptr);
    ASSERT_TRUE(merged->Equals(*arrow::int64())) << merged->ToString();

    // An object field that is always variant-null becomes an untyped variant leaf.
    auto object_with_null = Samples({R"({"a": null, "b": 1})"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         InferColumn(infer_, object_with_null));
    ASSERT_NE(inferred, nullptr);
    auto expected =
        arrow::struct_({arrow::field("a", arrow::null()), arrow::field("b", arrow::int64())});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, ArraysMerge) {
    // Two arrays merge element-wise into a single typed element schema.
    auto samples = Samples({"[1, 2]", "[3, 4]"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
    ASSERT_NE(inferred, nullptr);
    ASSERT_TRUE(inferred->Equals(*arrow::list(arrow::int64()))) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, ArrayBeyondDepthLimitStaysVariant) {
    InferVariantShreddingSchema shallow_infer{empty_logical_schema_, pool_,
                                              /*max_schema_width=*/300, /*max_schema_depth=*/1,
                                              /*min_field_cardinality_ratio=*/0.1};
    auto samples = Samples({R"({"arr": [1, 2]})"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         InferColumn(shallow_infer, samples));
    ASSERT_NE(inferred, nullptr);
    // Depth 1: the nested array is beyond the limit and stays an untyped variant leaf.
    auto expected = arrow::struct_({arrow::field("arr", arrow::null())});
    ASSERT_TRUE(inferred->Equals(*expected)) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, ScalarLeafTypes) {
    // Float and binary leaves shred to their arrow types.
    std::shared_ptr<GenericVariant> float_variant =
        BuildVariant([](VariantBuilder& b) { return b.AppendFloat(1.5f); });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> float_inferred,
                         InferColumn(infer_, {float_variant}));
    ASSERT_NE(float_inferred, nullptr);
    ASSERT_TRUE(float_inferred->Equals(*arrow::float32())) << float_inferred->ToString();

    std::shared_ptr<GenericVariant> binary_variant = BuildVariant(
        [](VariantBuilder& b) { return b.AppendBinary(std::string_view("\x01\x02", 2)); });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> binary_inferred,
                         InferColumn(infer_, {binary_variant}));
    ASSERT_NE(binary_inferred, nullptr);
    ASSERT_TRUE(binary_inferred->Equals(*arrow::binary())) << binary_inferred->ToString();

    // A UUID has no shredding type, so the column stays unshredded.
    std::string uuid_bytes(16, '\0');
    std::shared_ptr<GenericVariant> uuid_variant =
        BuildVariant([&](VariantBuilder& b) { return b.AppendUuid(uuid_bytes); });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> uuid_inferred,
                         InferColumn(infer_, {uuid_variant}));
    ASSERT_EQ(uuid_inferred, nullptr);
}

TEST_F(InferVariantShreddingSchemaTest, LargeIntegerAndDecimalMerging) {
    // A 19-digit long exceeds decimal(18) precision, so it stays a genuine int64 leaf.
    auto big_long = Samples({"1000000000000000000"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> long_inferred,
                         InferColumn(infer_, big_long));
    ASSERT_NE(long_inferred, nullptr);
    ASSERT_TRUE(long_inferred->Equals(*arrow::int64())) << long_inferred->ToString();

    // A long (int64) merged with a fractional decimal widens via MergeDecimalWithLong.
    auto long_then_decimal = Samples({"1000000000000000000", "1.5"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> ld,
                         InferColumn(infer_, long_then_decimal));
    ASSERT_NE(ld, nullptr);
    ASSERT_TRUE(ld->Equals(*arrow::decimal128(38, 1))) << ld->ToString();

    // The reversed order (decimal first, then long) hits the mirrored branch.
    auto decimal_then_long = Samples({"1.5", "1000000000000000000"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> dl,
                         InferColumn(infer_, decimal_then_long));
    ASSERT_NE(dl, nullptr);
    ASSERT_TRUE(dl->Equals(*arrow::decimal128(38, 1))) << dl->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, LongMergedWithIntegralDecimalStaysLong) {
    // A scale-0 decimal that fits in 18 digits merges with a long back to int64.
    std::shared_ptr<GenericVariant> integral_decimal =
        BuildVariant([](VariantBuilder& b) { return b.AppendDecimal(VariantDecimal{123, 0}); });
    std::shared_ptr<GenericVariant> big_long = Samples({"1000000000000000000"})[0];
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         InferColumn(infer_, {integral_decimal, big_long}));
    ASSERT_NE(inferred, nullptr);
    ASSERT_TRUE(inferred->Equals(*arrow::int64())) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, SmallFractionalDecimalPrecisionAdjusted) {
    // 0.0015 has more fractional digits than significant digits; precision widens up to the scale.
    auto samples = Samples({"0.0015"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred, InferColumn(infer_, samples));
    ASSERT_NE(inferred, nullptr);
    ASSERT_TRUE(inferred->Equals(*arrow::decimal128(18, 4))) << inferred->ToString();
}

TEST_F(InferVariantShreddingSchemaTest, DecimalMergeOverflowFallsToVariant) {
    // A 38-digit integral decimal merged with a high-scale decimal would need precision > 38,
    // which decimal cannot represent, so the column stays unshredded.
    __int128 wide_unscaled = 0;
    for (int i = 0; i < 38; ++i) {
        wide_unscaled = wide_unscaled * 10 + 1;  // 38 ones, no trailing zeros
    }
    std::shared_ptr<GenericVariant> wide_decimal = BuildVariant(
        [&](VariantBuilder& b) { return b.AppendDecimal(VariantDecimal{wide_unscaled, 0}); });
    std::shared_ptr<GenericVariant> high_scale_decimal =
        BuildVariant([](VariantBuilder& b) { return b.AppendDecimal(VariantDecimal{15, 20}); });
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         InferColumn(infer_, {wide_decimal, high_scale_decimal}));
    ASSERT_EQ(inferred, nullptr);
}

TEST_F(InferVariantShreddingSchemaTest, ObjectWithAllRareFieldsStaysUnshredded) {
    InferVariantShreddingSchema strict_infer{empty_logical_schema_, pool_,
                                             /*max_schema_width=*/300, /*max_schema_depth=*/50,
                                             /*min_field_cardinality_ratio=*/0.6};
    // Two objects with disjoint single-occurrence keys: with a 0.6 ratio every field is below the
    // cardinality threshold, so the object contributes no typed field and the column is dropped.
    auto samples = Samples({R"({"a": 1})", R"({"b": 2})"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> inferred,
                         InferColumn(strict_infer, samples));
    ASSERT_EQ(inferred, nullptr);
}

}  // namespace paimon::test
