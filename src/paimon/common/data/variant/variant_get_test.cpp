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

#include "paimon/common/data/variant/variant_get.h"

#include <functional>
#include <string>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/variant/variant_builder.h"
#include "paimon/common/data/variant/variant_path_segment.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class VariantGetTest : public ::testing::Test {
 public:
    void SetUp() override {
        // The same document as the Java GenericVariantTest#testVariantGet.
        std::string json =
            "{\n"
            "  \"object\": {\n"
            "    \"name\": \"Apache Paimon\",\n"
            "    \"age\": 2,\n"
            "    \"address\": {\n"
            "      \"street\": \"Main St\",\n"
            "      \"city\": \"Hangzhou\"\n"
            "    }\n"
            "  },\n"
            "  \"array\": [1, 2, 3, 4, 5],\n"
            "  \"string\": \"Hello, World!\",\n"
            "  \"long\": 12345678901234,\n"
            "  \"double\": 1.0123456789012345678901234567890123456789,\n"
            "  \"decimal\": 100.99,\n"
            "  \"boolean1\": true,\n"
            "  \"boolean2\": false,\n"
            "  \"nullField\": null\n"
            "}\n";
        ASSERT_OK_AND_ASSIGN(variant_, GenericVariant::FromJson(json, pool_));
        cast_args_.fail_on_error = false;
    }

    std::optional<Literal> Get(const std::string& path,
                               const std::shared_ptr<arrow::DataType>& target) {
        auto result = VariantGetExecutor::Get(variant_, path, target, cast_args_);
        EXPECT_TRUE(result.ok()) << result.status().ToString();
        return result.value();
    }

    std::string GetString(const std::string& path, const std::shared_ptr<arrow::DataType>& target) {
        std::optional<Literal> literal = Get(path, target);
        EXPECT_TRUE(literal.has_value());
        return literal->ToString();
    }

    Result<std::shared_ptr<arrow::Array>> GetAsArrow(
        const std::string& path, const std::shared_ptr<arrow::Field>& target_field) {
        return VariantGetExecutor::GetAsArrow(variant_, path, target_field, cast_args_, pool_,
                                              arrow_pool_);
    }

    // Builds a single-scalar variant using the direct append API, which can encode types that
    // JSON parsing never produces (uuid/date/float/binary/timestamp).
    std::shared_ptr<GenericVariant> BuildScalar(
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
    std::shared_ptr<arrow::MemoryPool> arrow_pool_ = GetArrowPool(pool_);
    std::shared_ptr<GenericVariant> variant_;
    VariantCastArgs cast_args_;
};

TEST_F(VariantGetTest, PathSegmentParse) {
    ASSERT_OK_AND_ASSIGN(auto segments,
                         VariantPathSegment::Parse("$[\"object\"]['address'].city[3]"));
    ASSERT_EQ(segments.size(), 4);
    ASSERT_EQ(segments[0].kind, VariantPathSegment::Kind::kObjectExtraction);
    ASSERT_EQ(segments[0].key, "object");
    ASSERT_EQ(segments[1].kind, VariantPathSegment::Kind::kObjectExtraction);
    ASSERT_EQ(segments[1].key, "address");
    ASSERT_EQ(segments[2].kind, VariantPathSegment::Kind::kObjectExtraction);
    ASSERT_EQ(segments[2].key, "city");
    ASSERT_EQ(segments[3].kind, VariantPathSegment::Kind::kArrayExtraction);
    ASSERT_EQ(segments[3].index, 3);

    ASSERT_OK_AND_ASSIGN(auto root_only, VariantPathSegment::Parse("$"));
    ASSERT_TRUE(root_only.empty());

    // Java parity: the root `$` is located anywhere in the path (Java uses Matcher#find), so a
    // prefix before the first `$` is tolerated and ignored.
    ASSERT_OK_AND_ASSIGN(auto prefixed, VariantPathSegment::Parse("abc$.x"));
    ASSERT_EQ(prefixed.size(), 1);
    ASSERT_EQ(prefixed[0].kind, VariantPathSegment::Kind::kObjectExtraction);
    ASSERT_EQ(prefixed[0].key, "x");

    ASSERT_NOK(VariantPathSegment::Parse(""));
    ASSERT_NOK(VariantPathSegment::Parse("no_root"));
    ASSERT_NOK(VariantPathSegment::Parse("$."));
    ASSERT_NOK(VariantPathSegment::Parse("$[abc]"));
    ASSERT_NOK(VariantPathSegment::Parse("$['unterminated]"));
}

TEST_F(VariantGetTest, ScalarTargets) {
    ASSERT_EQ(GetString("$.string", arrow::utf8()), "Hello, World!");
    auto long_value = Get("$.long", arrow::int64());
    ASSERT_TRUE(long_value.has_value());
    ASSERT_EQ(long_value->GetValue<int64_t>(), 12345678901234LL);
    ASSERT_EQ(GetString("$.long", arrow::utf8()), "12345678901234");
    auto double_value = Get("$.double", arrow::float64());
    ASSERT_TRUE(double_value.has_value());
    ASSERT_DOUBLE_EQ(double_value->GetValue<double>(), 1.0123456789012346);
    auto decimal_value = Get("$.decimal", arrow::decimal128(5, 2));
    ASSERT_TRUE(decimal_value.has_value());
    ASSERT_EQ(decimal_value->GetValue<Decimal>().ToUnscaledLong(), 10099);
    ASSERT_EQ(GetString("$.decimal", arrow::utf8()), "100.99");
    auto bool1 = Get("$.boolean1", arrow::boolean());
    ASSERT_TRUE(bool1.has_value());
    ASSERT_TRUE(bool1->GetValue<bool>());
    auto bool2 = Get("$.boolean2", arrow::boolean());
    ASSERT_TRUE(bool2.has_value());
    ASSERT_FALSE(bool2->GetValue<bool>());
    // Variant null maps to SQL NULL.
    ASSERT_FALSE(Get("$.nullField", arrow::boolean()).has_value());
    auto elem = Get("$.array[3]", arrow::int64());
    ASSERT_TRUE(elem.has_value());
    ASSERT_EQ(elem->GetValue<int64_t>(), 4);
}

TEST_F(VariantGetTest, ContainerToJsonString) {
    ASSERT_EQ(GetString("$.object", arrow::utf8()),
              "{\"address\":{\"city\":\"Hangzhou\",\"street\":\"Main St\"},\"age\":2,\"name\":"
              "\"Apache Paimon\"}");
    ASSERT_EQ(GetString("$.object.name", arrow::utf8()), "Apache Paimon");
    ASSERT_EQ(GetString("$.object.address.street", arrow::utf8()), "Main St");
    ASSERT_EQ(GetString("$[\"object\"]['address'].city", arrow::utf8()), "Hangzhou");
    ASSERT_EQ(GetString("$.array", arrow::utf8()), "[1,2,3,4,5]");
}

TEST_F(VariantGetTest, UnmatchedPathAndInvalidCast) {
    // A path that does not exist yields SQL NULL.
    ASSERT_FALSE(Get("$.missing", arrow::utf8()).has_value());
    ASSERT_FALSE(Get("$.string[0]", arrow::utf8()).has_value());
    ASSERT_FALSE(Get("$.array.key", arrow::utf8()).has_value());
    // An invalid cast yields SQL NULL when fail_on_error is false.
    ASSERT_FALSE(Get("$.object", arrow::int64()).has_value());
    // ... and an error when fail_on_error is true.
    cast_args_.fail_on_error = true;
    auto result = VariantGetExecutor::Get(variant_, "$.object", arrow::int64(), cast_args_);
    ASSERT_NOK(result);
}

TEST_F(VariantGetTest, NestedTargetsNotImplemented) {
    auto result =
        VariantGetExecutor::Get(variant_, "$.array", arrow::list(arrow::int32()), cast_args_);
    ASSERT_TRUE(result.status().IsNotImplemented());
}

TEST_F(VariantGetTest, BinaryAndTimestampSources) {
    // Java-encoded data can carry binary/timestamp scalars that JSON parsing never produces.
    {
        VariantBuilder builder(/*allow_duplicate_keys=*/false);
        ASSERT_OK(builder.AppendBinary(std::string_view("\x01\x02\x03", 3)));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> variant, builder.Build(pool_));
        ASSERT_OK_AND_ASSIGN(std::optional<Literal> literal,
                             VariantGetExecutor::Get(variant, "$", arrow::binary(), cast_args_));
        ASSERT_TRUE(literal.has_value());
        ASSERT_EQ(literal->GetValue<std::string>(), std::string("\x01\x02\x03", 3));
    }
    {
        VariantBuilder builder(/*allow_duplicate_keys=*/false);
        ASSERT_OK(builder.AppendTimestamp(1700000000123456));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> variant, builder.Build(pool_));
        ASSERT_OK_AND_ASSIGN(
            std::optional<Literal> literal,
            VariantGetExecutor::Get(variant, "$", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                                    cast_args_));
        ASSERT_TRUE(literal.has_value());
        ASSERT_EQ(literal->GetValue<Timestamp>().ToMicrosecond(), 1700000000123456);
    }
    {
        VariantBuilder builder(/*allow_duplicate_keys=*/false);
        ASSERT_OK(builder.AppendTimestampNtz(-1001));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> variant, builder.Build(pool_));
        ASSERT_OK_AND_ASSIGN(
            std::optional<Literal> literal,
            VariantGetExecutor::Get(variant, "$", arrow::timestamp(arrow::TimeUnit::MICRO),
                                    cast_args_));
        ASSERT_TRUE(literal.has_value());
        // Negative epochs must floor, not truncate toward zero.
        ASSERT_EQ(literal->GetValue<Timestamp>().ToMicrosecond(), -1001);
    }
}

TEST_F(VariantGetTest, ExtractByPath) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> address,
                         VariantGetExecutor::ExtractByPath(variant_, "$.object.address"));
    ASSERT_NE(address, nullptr);
    ASSERT_OK_AND_ASSIGN(std::string json, address->ToJson());
    ASSERT_EQ(json, "{\"city\":\"Hangzhou\",\"street\":\"Main St\"}");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> missing,
                         VariantGetExecutor::ExtractByPath(variant_, "$.object.missing"));
    ASSERT_EQ(missing, nullptr);
}

TEST_F(VariantGetTest, CastToStructTarget) {
    auto target = arrow::field(
        "r", arrow::struct_(
                 {arrow::field("name", arrow::utf8()), arrow::field("age", arrow::int64()),
                  arrow::field("address", arrow::struct_({arrow::field("city", arrow::utf8())})),
                  arrow::field("missing", arrow::utf8())}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, GetAsArrow("$.object", target));
    ASSERT_EQ(array->length(), 1);
    const auto& row = static_cast<const arrow::StructArray&>(*array);
    ASSERT_FALSE(row.IsNull(0));
    ASSERT_EQ(static_cast<const arrow::StringArray&>(*row.field(0)).GetString(0), "Apache Paimon");
    ASSERT_EQ(static_cast<const arrow::Int64Array&>(*row.field(1)).Value(0), 2);
    const auto& address = static_cast<const arrow::StructArray&>(*row.field(2));
    ASSERT_EQ(static_cast<const arrow::StringArray&>(*address.field(0)).GetString(0), "Hangzhou");
    // A target field absent from the variant object is null.
    ASSERT_TRUE(row.field(3)->IsNull(0));

    // With fail_on_error == false a child that cannot cast becomes null while the parent row
    // stays non-null.
    auto mixed_target = arrow::field("r", arrow::struct_({arrow::field("string", arrow::int64()),
                                                          arrow::field("long", arrow::int64())}));
    ASSERT_OK_AND_ASSIGN(array, GetAsArrow("$", mixed_target));
    const auto& mixed_row = static_cast<const arrow::StructArray&>(*array);
    ASSERT_FALSE(mixed_row.IsNull(0));
    // "string" holds "Hello, World!", which cannot cast to int64.
    ASSERT_TRUE(mixed_row.field(0)->IsNull(0));
    ASSERT_FALSE(mixed_row.field(1)->IsNull(0));
    ASSERT_EQ(static_cast<const arrow::Int64Array&>(*mixed_row.field(1)).Value(0), 12345678901234);
}

TEST_F(VariantGetTest, CastToStructFromNonObject) {
    auto target = arrow::field("r", arrow::struct_({arrow::field("a", arrow::int64())}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, GetAsArrow("$.array", target));
    ASSERT_TRUE(array->IsNull(0));
    cast_args_.fail_on_error = true;
    auto result =
        VariantGetExecutor::GetAsArrow(variant_, "$.array", target, cast_args_, pool_, arrow_pool_);
    ASSERT_NOK(result);
}

TEST_F(VariantGetTest, CastToMapTarget) {
    auto target = arrow::field("m", arrow::map(arrow::utf8(), arrow::utf8()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array,
                         GetAsArrow("$.object.address", target));
    const auto& map = static_cast<const arrow::MapArray&>(*array);
    ASSERT_EQ(map.value_length(0), 2);
    const auto& keys = static_cast<const arrow::StringArray&>(*map.keys());
    const auto& items = static_cast<const arrow::StringArray&>(*map.items());
    ASSERT_EQ(keys.GetString(0), "city");
    ASSERT_EQ(items.GetString(0), "Hangzhou");
    ASSERT_EQ(keys.GetString(1), "street");
    ASSERT_EQ(items.GetString(1), "Main St");
    // A map with a non-string key type is an invalid cast.
    auto bad_target = arrow::field("m", arrow::map(arrow::int64(), arrow::utf8()));
    ASSERT_OK_AND_ASSIGN(array, GetAsArrow("$.object.address", bad_target));
    ASSERT_TRUE(array->IsNull(0));
}

TEST_F(VariantGetTest, CastToListTarget) {
    auto target = arrow::field("l", arrow::list(arrow::int64()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, GetAsArrow("$.array", target));
    const auto& list = static_cast<const arrow::ListArray&>(*array);
    ASSERT_EQ(list.value_length(0), 5);
    const auto& values = static_cast<const arrow::Int64Array&>(*list.values());
    for (int64_t i = 0; i < 5; ++i) {
        ASSERT_EQ(values.Value(i), i + 1);
    }
    auto list_of_structs =
        arrow::field("l", arrow::list(arrow::struct_({arrow::field("a", arrow::int64())})));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> rows,
                         GenericVariant::FromJson("[{\"a\": 1}, {\"a\": 2}]", pool_));
    ASSERT_OK_AND_ASSIGN(array, VariantGetExecutor::GetAsArrow(rows, "$", list_of_structs,
                                                               cast_args_, pool_, arrow_pool_));
    const auto& struct_list = static_cast<const arrow::ListArray&>(*array);
    ASSERT_EQ(struct_list.value_length(0), 2);
    const auto& elements = static_cast<const arrow::StructArray&>(*struct_list.values());
    ASSERT_EQ(static_cast<const arrow::Int64Array&>(*elements.field(0)).Value(0), 1);
    ASSERT_EQ(static_cast<const arrow::Int64Array&>(*elements.field(0)).Value(1), 2);
}

TEST_F(VariantGetTest, CastToVariantTarget) {
    // Re-encodes the extracted sub-variant against a fresh metadata dictionary.
    auto target = VariantTypeUtils::ToArrowField("v", /*nullable=*/true, {});
    auto read_variant_json = [&](const arrow::Array& array) {
        const auto& row = static_cast<const arrow::StructArray&>(array);
        EXPECT_FALSE(row.IsNull(0));
        std::string_view value = static_cast<const arrow::BinaryArray&>(*row.field(0)).GetView(0);
        std::string_view metadata =
            static_cast<const arrow::BinaryArray&>(*row.field(1)).GetView(0);
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> copied,
                             GenericVariant::Create(value, metadata, pool_));
        EXPECT_OK_AND_ASSIGN(std::string json, copied->ToJson());
        return json;
    };
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array,
                         GetAsArrow("$.object.address", target));
    ASSERT_EQ(read_variant_json(*array), "{\"city\":\"Hangzhou\",\"street\":\"Main St\"}");

    // A variant null cast to a VARIANT target stays an encoded variant null (a non-null row
    // whose value renders as JSON null); only scalar targets turn a variant null into SQL NULL.
    ASSERT_OK_AND_ASSIGN(array, GetAsArrow("$.nullField", target));
    ASSERT_EQ(read_variant_json(*array), "null");
    // An unmatched path is SQL NULL by contrast.
    ASSERT_OK_AND_ASSIGN(array, GetAsArrow("$.missing", target));
    ASSERT_TRUE(array->IsNull(0));
}

TEST_F(VariantGetTest, NestedTargetNullSemantics) {
    auto target = arrow::field("r", arrow::struct_({arrow::field("a", arrow::int64())}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, GetAsArrow("$.missing", target));
    ASSERT_TRUE(array->IsNull(0));
    // A variant null yields SQL NULL even with fail_on_error == true.
    cast_args_.fail_on_error = true;
    ASSERT_OK_AND_ASSIGN(array, GetAsArrow("$.nullField", target));
    ASSERT_TRUE(array->IsNull(0));
}

TEST_F(VariantGetTest, UuidSourceCastsToStringOnly) {
    std::string uuid_bytes(16, '\0');
    for (int i = 0; i < 16; ++i) {
        uuid_bytes[i] = static_cast<char>(i);
    }
    std::shared_ptr<GenericVariant> variant =
        BuildScalar([&](VariantBuilder& b) { return b.AppendUuid(uuid_bytes); });
    // A UUID has no Paimon type, so it can only be rendered as its canonical string.
    ASSERT_OK_AND_ASSIGN(std::optional<Literal> as_string,
                         VariantGetExecutor::Get(variant, "$", arrow::utf8(), cast_args_));
    ASSERT_TRUE(as_string.has_value());
    ASSERT_EQ(as_string->GetValue<std::string>(), "00010203-0405-0607-0809-0a0b0c0d0e0f");
    // Any non-string target is an invalid cast, which is SQL NULL when fail_on_error is false.
    ASSERT_OK_AND_ASSIGN(std::optional<Literal> as_long,
                         VariantGetExecutor::Get(variant, "$", arrow::int64(), cast_args_));
    ASSERT_FALSE(as_long.has_value());
    cast_args_.fail_on_error = true;
    ASSERT_NOK(VariantGetExecutor::Get(variant, "$", arrow::int64(), cast_args_));
}

TEST_F(VariantGetTest, FloatingPointToStringMatchesJava) {
    // Doubles and floats are stringified via the Java formatting, not the arrow cast.
    ASSERT_EQ(GetString("$.double", arrow::utf8()), "1.0123456789012346");
    std::shared_ptr<GenericVariant> f =
        BuildScalar([](VariantBuilder& b) { return b.AppendFloat(1.5f); });
    ASSERT_OK_AND_ASSIGN(std::optional<Literal> as_string,
                         VariantGetExecutor::Get(f, "$", arrow::utf8(), cast_args_));
    ASSERT_TRUE(as_string.has_value());
    ASSERT_EQ(as_string->GetValue<std::string>(), "1.5");
    ASSERT_OK_AND_ASSIGN(std::optional<Literal> as_float,
                         VariantGetExecutor::Get(f, "$", arrow::float32(), cast_args_));
    ASSERT_TRUE(as_float.has_value());
    ASSERT_EQ(as_float->GetValue<float>(), 1.5f);
}

TEST_F(VariantGetTest, DateSource) {
    std::shared_ptr<GenericVariant> d =
        BuildScalar([](VariantBuilder& b) { return b.AppendDate(19000); });
    ASSERT_OK_AND_ASSIGN(std::optional<Literal> as_date,
                         VariantGetExecutor::Get(d, "$", arrow::date32(), cast_args_));
    ASSERT_TRUE(as_date.has_value());
    ASSERT_EQ(as_date->GetValue<int32_t>(), 19000);
}

TEST_F(VariantGetTest, MissingCastExecutorYieldsNull) {
    // No binary->int64 cast executor exists, so the cast is treated as invalid (SQL NULL).
    std::shared_ptr<GenericVariant> bin = BuildScalar(
        [](VariantBuilder& b) { return b.AppendBinary(std::string_view("\x01\x02", 2)); });
    ASSERT_OK_AND_ASSIGN(std::optional<Literal> as_long,
                         VariantGetExecutor::Get(bin, "$", arrow::int64(), cast_args_));
    ASSERT_FALSE(as_long.has_value());
}

TEST_F(VariantGetTest, ScalarArrowBuilders) {
    auto build_array = [&](const std::shared_ptr<GenericVariant>& variant, const std::string& path,
                           const std::shared_ptr<arrow::DataType>& type) {
        auto result = VariantGetExecutor::GetAsArrow(variant, path, arrow::field("x", type),
                                                     cast_args_, pool_, arrow_pool_);
        EXPECT_TRUE(result.ok()) << result.status().ToString();
        return result.value();
    };

    std::shared_ptr<arrow::Array> boolean = build_array(variant_, "$.boolean1", arrow::boolean());
    ASSERT_TRUE(static_cast<const arrow::BooleanArray&>(*boolean).Value(0));

    // INT8/INT16/INT32 each require a narrowing cast from the int64 source.
    std::shared_ptr<arrow::Array> int8 = build_array(variant_, "$.object.age", arrow::int8());
    ASSERT_EQ(static_cast<const arrow::Int8Array&>(*int8).Value(0), 2);
    std::shared_ptr<arrow::Array> int16 = build_array(variant_, "$.object.age", arrow::int16());
    ASSERT_EQ(static_cast<const arrow::Int16Array&>(*int16).Value(0), 2);
    std::shared_ptr<arrow::Array> int32 = build_array(variant_, "$.object.age", arrow::int32());
    ASSERT_EQ(static_cast<const arrow::Int32Array&>(*int32).Value(0), 2);

    std::shared_ptr<arrow::Array> float64 = build_array(variant_, "$.double", arrow::float64());
    ASSERT_DOUBLE_EQ(static_cast<const arrow::DoubleArray&>(*float64).Value(0), 1.0123456789012346);

    std::shared_ptr<GenericVariant> f =
        BuildScalar([](VariantBuilder& b) { return b.AppendFloat(1.5f); });
    std::shared_ptr<arrow::Array> float32 = build_array(f, "$", arrow::float32());
    ASSERT_EQ(static_cast<const arrow::FloatArray&>(*float32).Value(0), 1.5f);

    std::shared_ptr<arrow::Array> decimal =
        build_array(variant_, "$.decimal", arrow::decimal128(5, 2));
    ASSERT_EQ(static_cast<const arrow::Decimal128Array&>(*decimal).FormatValue(0), "100.99");

    std::shared_ptr<GenericVariant> bin = BuildScalar(
        [](VariantBuilder& b) { return b.AppendBinary(std::string_view("\x01\x02\x03", 3)); });
    std::shared_ptr<arrow::Array> binary = build_array(bin, "$", arrow::binary());
    ASSERT_EQ(static_cast<const arrow::BinaryArray&>(*binary).GetView(0),
              std::string_view("\x01\x02\x03", 3));

    std::shared_ptr<GenericVariant> d =
        BuildScalar([](VariantBuilder& b) { return b.AppendDate(19000); });
    std::shared_ptr<arrow::Array> date = build_array(d, "$", arrow::date32());
    ASSERT_EQ(static_cast<const arrow::Date32Array&>(*date).Value(0), 19000);

    // Only the microsecond unit matches the variant's internal timestamp representation; other
    // units require a timestamp->timestamp literal cast that is intentionally unsupported.
    std::shared_ptr<GenericVariant> ts =
        BuildScalar([](VariantBuilder& b) { return b.AppendTimestamp(1700000000123456); });
    std::shared_ptr<arrow::Array> timestamp =
        build_array(ts, "$", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"));
    ASSERT_EQ(static_cast<const arrow::TimestampArray&>(*timestamp).Value(0), 1700000000123456);
}

TEST_F(VariantGetTest, ListFromNonArrayIsNull) {
    auto target = arrow::field("l", arrow::list(arrow::int64()));
    // An object cannot cast to a list: SQL NULL when fail_on_error is false ...
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, GetAsArrow("$.object", target));
    ASSERT_TRUE(array->IsNull(0));
    // ... and an error when fail_on_error is true.
    cast_args_.fail_on_error = true;
    auto result = VariantGetExecutor::GetAsArrow(variant_, "$.object", target, cast_args_, pool_,
                                                 arrow_pool_);
    ASSERT_NOK(result);
}

}  // namespace paimon::test
