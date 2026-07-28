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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_builder.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/data/variant/variant_reassembler.h"
#include "paimon/common/data/variant/variant_schema.h"
#include "paimon/common/data/variant/variant_shredding_utils.h"
#include "paimon/common/data/variant/variant_shredding_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class VariantShreddingTest : public ::testing::Test {
 public:
    // Shreds the given JSON documents (nullptr = null variant) with the given shredding type,
    // asserts the reassembled variants render back to the same JSON, and returns the shredded
    // array for structural checks.
    std::shared_ptr<arrow::StructArray> RoundTrip(
        const std::shared_ptr<arrow::DataType>& shredding_type,
        const std::vector<const char*>& jsons) {
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> physical,
                             VariantShreddingUtils::VariantShreddingSchema(shredding_type));
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<VariantSchema> schema,
                             VariantShreddingUtils::BuildVariantSchema(physical));

        EXPECT_OK_AND_ASSIGN(
            std::unique_ptr<VariantShreddedColumnWriter> writer,
            VariantShreddedColumnWriter::Create(schema, physical, arrow::default_memory_pool()));
        std::vector<std::string> expected_jsons;
        for (const char* json : jsons) {
            if (json == nullptr) {
                EXPECT_OK(writer->AppendNull());
                expected_jsons.emplace_back();
                continue;
            }
            EXPECT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> variant,
                                 GenericVariant::FromJson(json, pool_));
            EXPECT_OK_AND_ASSIGN(std::string expected_json, variant->ToJson());
            expected_jsons.push_back(std::move(expected_json));
            EXPECT_OK(writer->Append(*variant));
        }
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> shredded_array, writer->Finish());
        auto shredded = std::static_pointer_cast<arrow::StructArray>(shredded_array);

        EXPECT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> assembled_array,
                             VariantReassembler::AssembleVariantArray(
                                 shredded, schema, pool_, arrow::default_memory_pool()));
        auto assembled = std::static_pointer_cast<arrow::StructArray>(assembled_array);
        EXPECT_EQ(assembled->length(), static_cast<int64_t>(jsons.size()));
        auto value_column = std::static_pointer_cast<arrow::BinaryArray>(assembled->field(0));
        auto metadata_column = std::static_pointer_cast<arrow::BinaryArray>(assembled->field(1));
        for (size_t i = 0; i < jsons.size(); ++i) {
            SCOPED_TRACE("row " + std::to_string(i));
            if (jsons[i] == nullptr) {
                EXPECT_TRUE(assembled->IsNull(i));
                continue;
            }
            EXPECT_FALSE(assembled->IsNull(i));
            EXPECT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> variant,
                                 GenericVariant::Create(value_column->GetView(i),
                                                        metadata_column->GetView(i), pool_));
            EXPECT_OK_AND_ASSIGN(std::string actual_json, variant->ToJson());
            EXPECT_EQ(actual_json, expected_jsons[i]);
        }
        return shredded;
    }

 protected:
    // The physical shredded arrow type for a logical shredding type.
    std::shared_ptr<arrow::DataType> Physical(const std::shared_ptr<arrow::DataType>& logical) {
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> physical,
                             VariantShreddingUtils::VariantShreddingSchema(logical));
        return physical;
    }

    std::shared_ptr<GenericVariant> Json(const char* json) {
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> variant,
                             GenericVariant::FromJson(json, pool_));
        return variant;
    }

    // Builds a single variant using the direct append API, which can encode types (float, binary,
    // date, timestamp) that JSON parsing never produces.
    std::shared_ptr<GenericVariant> BuildVariant(
        const std::function<Status(VariantBuilder&)>& append) {
        VariantBuilder builder(/*allow_duplicate_keys=*/false);
        Status st = append(builder);
        EXPECT_TRUE(st.ok()) << st.ToString();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> variant, builder.Build(pool_));
        return variant;
    }

    // Shreds the given variants (nullptr = null row) against a physical shredded type, reassembles
    // them, asserts each reassembled variant renders back to the same JSON, and returns the
    // shredded array. Unlike `RoundTrip`, the physical type is provided directly so that typed
    // columns unsupported by `VariantShreddingSchema` (date/timestamp) can be exercised.
    std::shared_ptr<arrow::StructArray> ShredAndCheck(
        const std::shared_ptr<arrow::DataType>& physical,
        const std::vector<std::shared_ptr<GenericVariant>>& variants) {
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<VariantSchema> schema,
                             VariantShreddingUtils::BuildVariantSchema(physical));
        EXPECT_OK_AND_ASSIGN(
            std::unique_ptr<VariantShreddedColumnWriter> writer,
            VariantShreddedColumnWriter::Create(schema, physical, arrow::default_memory_pool()));
        std::vector<std::string> expected_jsons;
        for (const auto& variant : variants) {
            if (variant == nullptr) {
                EXPECT_OK(writer->AppendNull());
                expected_jsons.emplace_back();
                continue;
            }
            EXPECT_OK_AND_ASSIGN(std::string expected_json, variant->ToJson());
            expected_jsons.push_back(std::move(expected_json));
            EXPECT_OK(writer->Append(*variant));
        }
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> shredded_array, writer->Finish());
        auto shredded = std::static_pointer_cast<arrow::StructArray>(shredded_array);

        EXPECT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> assembled_array,
                             VariantReassembler::AssembleVariantArray(
                                 shredded, schema, pool_, arrow::default_memory_pool()));
        auto assembled = std::static_pointer_cast<arrow::StructArray>(assembled_array);
        auto value_column = std::static_pointer_cast<arrow::BinaryArray>(assembled->field(0));
        auto metadata_column = std::static_pointer_cast<arrow::BinaryArray>(assembled->field(1));
        for (size_t i = 0; i < variants.size(); ++i) {
            SCOPED_TRACE("row " + std::to_string(i));
            if (variants[i] == nullptr) {
                EXPECT_TRUE(assembled->IsNull(i));
                continue;
            }
            EXPECT_FALSE(assembled->IsNull(i));
            EXPECT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> variant,
                                 GenericVariant::Create(value_column->GetView(i),
                                                        metadata_column->GetView(i), pool_));
            EXPECT_OK_AND_ASSIGN(std::string actual_json, variant->ToJson());
            EXPECT_EQ(actual_json, expected_jsons[i]);
        }
        return shredded;
    }

    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(VariantShreddingTest, ShreddingSchemaShape) {
    auto shredding_type =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> physical,
                         VariantShreddingUtils::VariantShreddingSchema(shredding_type));
    // struct{metadata: binary not null, value: binary, typed_value: struct{a:
    // struct{value, typed_value} not null, b: ... not null}}
    auto expected = arrow::struct_(
        {arrow::field("metadata", arrow::binary(), false),
         arrow::field("value", arrow::binary(), true),
         arrow::field(
             "typed_value",
             arrow::struct_(
                 {arrow::field("a",
                               arrow::struct_({arrow::field("value", arrow::binary(), true),
                                               arrow::field("typed_value", arrow::int32(), true)}),
                               false),
                  arrow::field("b",
                               arrow::struct_({arrow::field("value", arrow::binary(), true),
                                               arrow::field("typed_value", arrow::utf8(), true)}),
                               false)}),
             true)});
    ASSERT_TRUE(physical->Equals(*expected)) << physical->ToString();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<VariantSchema> schema,
                         VariantShreddingUtils::BuildVariantSchema(physical));
    ASSERT_EQ(schema->top_level_metadata_idx, 0);
    ASSERT_EQ(schema->variant_idx, 1);
    ASSERT_EQ(schema->typed_idx, 2);
    ASSERT_TRUE(schema->has_object_schema);
    ASSERT_EQ(schema->object_schema.size(), 2);
    ASSERT_FALSE(schema->IsUnshredded());
    ASSERT_TRUE(VariantShreddingUtils::IsShreddedFileType(physical));
    ASSERT_FALSE(VariantShreddingUtils::IsShreddedFileType(
        arrow::struct_({arrow::field("value", arrow::binary(), false),
                        arrow::field("metadata", arrow::binary(), false)})));

    // Invalid shredding types are rejected.
    ASSERT_NOK(VariantShreddingUtils::VariantShreddingSchema(arrow::date32()));
    ASSERT_NOK(
        VariantShreddingUtils::VariantShreddingSchema(arrow::map(arrow::utf8(), arrow::int32())));
}

TEST_F(VariantShreddingTest, ShredObject) {
    // Mirrors the Java GenericVariantTest#testShredding scenarios.
    auto variant_json = R"({"a": 1, "b": "hello"})";
    // Happy path: all fields shredded, no residual value.
    {
        auto shredded = RoundTrip(
            arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())}),
            {variant_json});
        ASSERT_TRUE(shredded->field(1)->IsNull(0));   // top-level value (residual) is null
        ASSERT_FALSE(shredded->field(2)->IsNull(0));  // typed_value is present
    }
    // Missing field "c" in the data: present in schema, both children null.
    {
        auto shredded = RoundTrip(
            arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("c", arrow::utf8()),
                            arrow::field("b", arrow::utf8())}),
            {variant_json});
        auto typed = std::static_pointer_cast<arrow::StructArray>(shredded->field(2));
        auto c_group = std::static_pointer_cast<arrow::StructArray>(typed->field(1));
        ASSERT_FALSE(c_group->IsNull(0));
        ASSERT_TRUE(c_group->field(0)->IsNull(0));
        ASSERT_TRUE(c_group->field(1)->IsNull(0));
    }
    // "a" is not present in the shredding schema: it goes to the residual value.
    {
        auto shredded = RoundTrip(
            arrow::struct_({arrow::field("b", arrow::utf8()), arrow::field("c", arrow::utf8())}),
            {variant_json});
        auto value_column = std::static_pointer_cast<arrow::BinaryArray>(shredded->field(1));
        ASSERT_FALSE(value_column->IsNull(0));
        // The residual must equal the standalone encoding of {"a": 1}.
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> residual_expected,
                             GenericVariant::FromJson("{\"a\": 1}", pool_));
        ASSERT_OK_AND_ASSIGN(std::string_view residual_value, residual_expected->Value());
        ASSERT_EQ(value_column->GetView(0), residual_value);
    }
}

TEST_F(VariantShreddingTest, ShredAllTypes) {
    // Mirrors the Java GenericVariantTest#testShreddingAllTypes.
    const char* json =
        "{\n"
        "  \"c1\": \"Hello, World!\",\n"
        "  \"c2\": 12345678901234,\n"
        "  \"c3\": 1.0123456789012345678901234567890123456789,\n"
        "  \"c4\": 100.99,\n"
        "  \"c5\": true,\n"
        "  \"c6\": null,\n"
        "  \"c7\": {\"street\" : \"Main St\",\"city\" : \"Hangzhou\"},\n"
        "  \"c8\": [1, 2]\n"
        "}\n";
    auto shredding_type = arrow::struct_(
        {arrow::field("c1", arrow::utf8()), arrow::field("c2", arrow::int64()),
         arrow::field("c3", arrow::float64()), arrow::field("c4", arrow::decimal128(5, 2)),
         arrow::field("c5", arrow::boolean()), arrow::field("c6", arrow::utf8()),
         arrow::field("c7", arrow::struct_({arrow::field("street", arrow::utf8()),
                                            arrow::field("city", arrow::utf8())})),
         arrow::field("c8", arrow::list(arrow::int32()))});
    auto shredded = RoundTrip(shredding_type, {json, nullptr, json});

    // c6 is a variant null: it stays in the field's value column ("00"), typed_value is null.
    auto typed = std::static_pointer_cast<arrow::StructArray>(shredded->field(2));
    auto c6_group = std::static_pointer_cast<arrow::StructArray>(typed->field(5));
    ASSERT_FALSE(c6_group->IsNull(0));
    auto c6_value = std::static_pointer_cast<arrow::BinaryArray>(c6_group->field(0));
    ASSERT_FALSE(c6_value->IsNull(0));
    ASSERT_EQ(c6_value->GetView(0), std::string_view("\x00", 1));
    ASSERT_TRUE(c6_group->field(1)->IsNull(0));

    // Nothing was left over at the top level.
    ASSERT_TRUE(shredded->field(1)->IsNull(0));

    // No shredding at all: everything stays in the top-level value.
    auto no_match_type = arrow::struct_({arrow::field("other", arrow::utf8())});
    auto unshredded = RoundTrip(no_match_type, {json});
    auto value_column = std::static_pointer_cast<arrow::BinaryArray>(unshredded->field(1));
    ASSERT_FALSE(value_column->IsNull(0));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> expected,
                         GenericVariant::FromJson(json, pool_));
    ASSERT_OK_AND_ASSIGN(std::string_view expected_value, expected->Value());
    ASSERT_EQ(value_column->GetView(0), expected_value);
}

TEST_F(VariantShreddingTest, ShredScalarsAndMismatches) {
    // Top-level scalar shredding with type mismatches falling back to the value column.
    auto long_type = arrow::struct_({arrow::field("x", arrow::int64())});
    RoundTrip(long_type,
              {"{\"x\": 5}", R"({"x": "not a number"})", "{\"x\": 3.25}", "{\"x\": [1]}", "{}"});
    // Decimal rescale: 100.99 fits decimal(9, 4) exactly (allowNumericScaleChanges).
    auto decimal_type = arrow::struct_({arrow::field("x", arrow::decimal128(9, 4))});
    RoundTrip(decimal_type,
              {"{\"x\": 100.99}", "{\"x\": 42}", "{\"x\": 0.123456789}", "{\"x\": 100.00}"});
    // A 38-digit unscaled value cannot rescale to scale 1 without overflowing the 128-bit
    // decimal; it must fall back to the value column instead of being written corrupted.
    auto wide_decimal_type = arrow::struct_({arrow::field("x", arrow::decimal128(38, 1))});
    RoundTrip(wide_decimal_type,
              {"{\"x\": 99999999999999999999999999999999999999}", "{\"x\": 1.5}"});
    // Integer target from decimal that is numerically integral.
    auto int_type = arrow::struct_({arrow::field("x", arrow::int32())});
    RoundTrip(int_type, {"{\"x\": 5.0}", "{\"x\": 5.5}", "{\"x\": 123456789012345678}"});
    auto array_type = arrow::struct_(
        {arrow::field("arr", arrow::list(arrow::struct_({arrow::field("k", arrow::utf8())})))});
    RoundTrip(array_type, {R"({"arr": [{"k": "v1"}, {"k": "v2", "extra": 1}, {"other": 2}]})",
                           "{\"arr\": [1, 2]}", R"({"arr": {"k": "v"}})"});
}

TEST_F(VariantShreddingTest, ScalarShreddingIntegerWidths) {
    // int8/int16 targets: in-range longs shred into the narrow typed column, while out-of-range
    // values fall back to the residual value column. Decimal-integral inputs take the
    // decimal->integer shredding path.
    ShredAndCheck(Physical(arrow::struct_({arrow::field("x", arrow::int8())})),
                  {Json(R"({"x": 5})"), Json(R"({"x": 200})"), Json(R"({"x": 5.0})")});
    ShredAndCheck(Physical(arrow::struct_({arrow::field("x", arrow::int16())})),
                  {Json(R"({"x": 5})"), Json(R"({"x": 40000})"), Json(R"({"x": 5.0})")});
}

TEST_F(VariantShreddingTest, ScalarShreddingFloatAndBinary) {
    // Float and binary variants are not producible from JSON, so build them directly. They shred
    // into their typed columns and round-trip back to the same value.
    ShredAndCheck(Physical(arrow::float32()),
                  {BuildVariant([](VariantBuilder& b) { return b.AppendFloat(1.5f); }), nullptr});
    ShredAndCheck(Physical(arrow::binary()), {BuildVariant([](VariantBuilder& b) {
                      return b.AppendBinary(std::string_view("\x01\x02\x03", 3));
                  })});
}

TEST_F(VariantShreddingTest, ScalarShreddingDate) {
    // date typed columns are produced by external engines; `VariantShreddingSchema` itself never
    // emits them, so build the physical shredded type directly.
    auto physical = arrow::struct_({arrow::field("metadata", arrow::binary(), false),
                                    arrow::field("value", arrow::binary(), true),
                                    arrow::field("typed_value", arrow::date32(), true)});
    ShredAndCheck(physical,
                  {BuildVariant([](VariantBuilder& b) { return b.AppendDate(19000); }), nullptr});
}

TEST_F(VariantShreddingTest, TimestampSchemaParsing) {
    auto make_physical = [](const std::shared_ptr<arrow::DataType>& ts) {
        return arrow::struct_({arrow::field("metadata", arrow::binary(), false),
                               arrow::field("value", arrow::binary(), true),
                               arrow::field("typed_value", ts, true)});
    };
    // A microsecond timestamp with a timezone parses as TIMESTAMP_LTZ; without, TIMESTAMP_NTZ.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<VariantSchema> ltz,
                         VariantShreddingUtils::BuildVariantSchema(
                             make_physical(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))));
    ASSERT_TRUE(ltz->scalar_schema.has_value());
    ASSERT_EQ(ltz->scalar_schema->kind, VariantSchema::ScalarKind::kTimestampLtz);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<VariantSchema> ntz,
                         VariantShreddingUtils::BuildVariantSchema(
                             make_physical(arrow::timestamp(arrow::TimeUnit::MICRO))));
    ASSERT_TRUE(ntz->scalar_schema.has_value());
    ASSERT_EQ(ntz->scalar_schema->kind, VariantSchema::ScalarKind::kTimestampNtz);
    // Non-microsecond timestamps cannot represent the variant's microsecond values, so they are
    // rejected as an invalid shredding schema.
    ASSERT_NOK(VariantShreddingUtils::BuildVariantSchema(
        make_physical(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"))));
}

TEST_F(VariantShreddingTest, TimestampReassembly) {
    // The writer never produces timestamp typed columns, but the reassembler must handle files
    // written by engines that do. Build a shredded array with a populated timestamp typed_value
    // and verify it reassembles into the same variant.
    auto reassemble_one = [&](const std::shared_ptr<arrow::DataType>& ts_type,
                              const std::shared_ptr<GenericVariant>& reference, int64_t micros) {
        auto physical = arrow::struct_({arrow::field("metadata", arrow::binary(), false),
                                        arrow::field("value", arrow::binary(), true),
                                        arrow::field("typed_value", ts_type, true)});
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<VariantSchema> schema,
                             VariantShreddingUtils::BuildVariantSchema(physical));
        std::string metadata(reference->Metadata());
        ASSERT_OK_AND_ASSIGN(std::string expected_json, reference->ToJson());

        arrow::BinaryBuilder meta_builder;
        ASSERT_TRUE(meta_builder.Append(metadata).ok());
        std::shared_ptr<arrow::Array> meta_array;
        ASSERT_TRUE(meta_builder.Finish(&meta_array).ok());
        arrow::BinaryBuilder value_builder;
        ASSERT_TRUE(value_builder.AppendNull().ok());
        std::shared_ptr<arrow::Array> value_array;
        ASSERT_TRUE(value_builder.Finish(&value_array).ok());
        arrow::TimestampBuilder ts_builder(ts_type, arrow::default_memory_pool());
        ASSERT_TRUE(ts_builder.Append(micros).ok());
        std::shared_ptr<arrow::Array> ts_array;
        ASSERT_TRUE(ts_builder.Finish(&ts_array).ok());
        auto made = arrow::StructArray::Make({meta_array, value_array, ts_array},
                                             {"metadata", "value", "typed_value"});
        ASSERT_TRUE(made.ok()) << made.status().ToString();
        std::shared_ptr<arrow::StructArray> shredded = made.ValueOrDie();

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> assembled_array,
                             VariantReassembler::AssembleVariantArray(
                                 shredded, schema, pool_, arrow::default_memory_pool()));
        auto assembled = std::static_pointer_cast<arrow::StructArray>(assembled_array);
        auto value_column = std::static_pointer_cast<arrow::BinaryArray>(assembled->field(0));
        auto metadata_column = std::static_pointer_cast<arrow::BinaryArray>(assembled->field(1));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<GenericVariant> variant,
            GenericVariant::Create(value_column->GetView(0), metadata_column->GetView(0), pool_));
        ASSERT_OK_AND_ASSIGN(std::string actual_json, variant->ToJson());
        ASSERT_EQ(actual_json, expected_json);
    };

    int64_t micros = 1700000000000000;
    reassemble_one(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                   BuildVariant([&](VariantBuilder& b) { return b.AppendTimestamp(micros); }),
                   micros);
    reassemble_one(arrow::timestamp(arrow::TimeUnit::MICRO),
                   BuildVariant([&](VariantBuilder& b) { return b.AppendTimestampNtz(micros); }),
                   micros);
}

TEST_F(VariantShreddingTest, ScalarSchemaToArrowType) {
    using SK = VariantSchema::ScalarKind;
    auto check = [](VariantSchema::ScalarType scalar,
                    const std::shared_ptr<arrow::DataType>& expected) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> type,
                             VariantShreddingUtils::ScalarSchemaToArrowType(scalar));
        ASSERT_TRUE(type->Equals(*expected)) << type->ToString();
    };
    check({SK::kBoolean}, arrow::boolean());
    check({SK::kByte}, arrow::int8());
    check({SK::kShort}, arrow::int16());
    check({SK::kInt}, arrow::int32());
    check({SK::kLong}, arrow::int64());
    check({SK::kFloat}, arrow::float32());
    check({SK::kDouble}, arrow::float64());
    check({SK::kString}, arrow::utf8());
    check({SK::kBinary}, arrow::binary());
    check({SK::kDecimal, 10, 2}, arrow::decimal128(10, 2));
    check({SK::kDate}, arrow::date32());
    check({SK::kTimestampLtz}, arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"));
    check({SK::kTimestampNtz}, arrow::timestamp(arrow::TimeUnit::MICRO));
    // kUuid has no shredded arrow representation.
    ASSERT_NOK(VariantShreddingUtils::ScalarSchemaToArrowType({SK::kUuid}));
}

TEST_F(VariantShreddingTest, InvalidShreddingSchemas) {
    // Not a struct.
    ASSERT_NOK(VariantShreddingUtils::BuildVariantSchema(arrow::int32()));
    // Empty struct.
    ASSERT_NOK(VariantShreddingUtils::BuildVariantSchema(arrow::struct_({})));
    // The "value" column must be binary.
    ASSERT_NOK(VariantShreddingUtils::BuildVariantSchema(
        arrow::struct_({arrow::field("value", arrow::int32())})));
    // Unknown field name.
    ASSERT_NOK(VariantShreddingUtils::BuildVariantSchema(
        arrow::struct_({arrow::field("bogus", arrow::binary())})));
    // A top-level schema must carry a metadata column.
    ASSERT_NOK(VariantShreddingUtils::BuildVariantSchema(
        arrow::struct_({arrow::field("value", arrow::binary())})));
    // Unsupported typed_value type.
    ASSERT_NOK(VariantShreddingUtils::BuildVariantSchema(arrow::struct_(
        {arrow::field("metadata", arrow::binary()), arrow::field("value", arrow::binary()),
         arrow::field("typed_value", arrow::map(arrow::utf8(), arrow::int32()))})));
}

}  // namespace paimon::test
