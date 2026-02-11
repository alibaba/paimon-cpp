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

#include <memory>
#include <string>

#include "arrow/api.h"
#include "arrow/ipc/api.h"
#include "avro/Compiler.hh"
#include "avro/Decoder.hh"
#include "avro/Encoder.hh"
#include "avro/Stream.hh"
#include "avro/ValidSchema.hh"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/avro/avro_direct_decoder.h"
#include "paimon/format/avro/avro_direct_encoder.h"
#include "paimon/format/avro/avro_schema_converter.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::avro::test {

class AvroDirectEncoderDecoderTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    Result<std::unique_ptr<::avro::OutputStream>> EncodeData(
        const ::avro::NodePtr& avro_node, const std::shared_ptr<arrow::Array>& input_array) {
        auto output_stream = ::avro::memoryOutputStream();
        auto encoder = ::avro::binaryEncoder();
        encoder->init(*output_stream);

        for (int64_t i = 0; i < input_array->length(); ++i) {
            PAIMON_RETURN_NOT_OK(AvroDirectEncoder::EncodeArrowToAvro(avro_node, *input_array, i,
                                                                      encoder.get(), &encode_ctx_));
        }
        return output_stream;
    }

    Result<std::shared_ptr<arrow::Array>> DecodeWithEncodedData(
        const ::avro::NodePtr& avro_node, std::unique_ptr<::avro::OutputStream>&& encoded_data,
        const std::optional<std::set<size_t>>& projection, int32_t expected_count,
        arrow::ArrayBuilder* builder) {
        auto input_stream = ::avro::memoryInputStream(*encoded_data);
        auto decoder = ::avro::binaryDecoder();
        decoder->init(*input_stream);

        for (int32_t i = 0; i < expected_count; ++i) {
            PAIMON_RETURN_NOT_OK(AvroDirectDecoder::DecodeAvroToBuilder(
                avro_node, projection, decoder.get(), builder, &decode_ctx_));
        }

        std::shared_ptr<arrow::Array> decoded_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Finish(&decoded_array));
        EXPECT_EQ(decoded_array->length(), expected_count);
        return decoded_array;
    }

    void CheckResult(const std::string& schema_json,
                     const std::shared_ptr<arrow::Array>& input_array,
                     arrow::ArrayBuilder* builder) {
        auto avro_schema = ::avro::compileJsonSchemaFromString(schema_json);

        ASSERT_OK_AND_ASSIGN(auto encoded_data, EncodeData(avro_schema.root(), input_array));
        ASSERT_OK_AND_ASSIGN(
            auto decoded_array,
            DecodeWithEncodedData(avro_schema.root(), std::move(encoded_data),
                                  /*projection=*/std::nullopt, input_array->length(), builder));
        ASSERT_TRUE(decoded_array->Equals(*input_array));
    }

    Result<std::shared_ptr<arrow::Array>> GetProjectedArray(
        const std::shared_ptr<arrow::StructArray>& input_array,
        const std::set<size_t>& projection) {
        auto struct_type = input_array->struct_type();
        arrow::FieldVector projected_fields;
        projected_fields.reserve(projection.size());
        arrow::ArrayVector projected_field_arrays;
        projected_field_arrays.reserve(projection.size());
        for (size_t index : projection) {
            if (index >= static_cast<size_t>(struct_type->num_fields())) {
                return Status::Invalid(
                    fmt::format("Projection index {} out of range for struct with {} fields", index,
                                struct_type->num_fields()));
            }
            projected_fields.push_back(struct_type->field(index));
            projected_field_arrays.push_back(input_array->field(index));
        }
        auto projected_struct_type = arrow::struct_(projected_fields);

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto projected_array,
            arrow::StructArray::Make(projected_field_arrays, projected_fields));
        return projected_array;
    }

    void CheckResultWithProjection(const std::shared_ptr<arrow::Array>& src_array,
                                   const std::set<size_t>& projection) {
        auto src_struct_array = std::dynamic_pointer_cast<arrow::StructArray>(src_array);
        ASSERT_OK_AND_ASSIGN(auto avro_schema,
                             AvroSchemaConverter::ArrowSchemaToAvroSchema(
                                 arrow::schema(src_struct_array->struct_type()->fields())));
        ASSERT_OK_AND_ASSIGN(auto encoded_data, EncodeData(avro_schema.root(), src_array));

        ASSERT_OK_AND_ASSIGN(auto projected_array, GetProjectedArray(src_struct_array, projection));
        auto decoded_array_builder = arrow::MakeBuilder(projected_array->type()).ValueOrDie();
        ASSERT_OK_AND_ASSIGN(
            auto decoded_array,
            DecodeWithEncodedData(avro_schema.root(), std::move(encoded_data), projection,
                                  src_array->length(), decoded_array_builder.get()));
        ASSERT_TRUE(decoded_array->Equals(*projected_array));
    }

 protected:
    AvroDirectEncoder::EncodeContext encode_ctx_;
    AvroDirectDecoder::DecodeContext decode_ctx_;
};

TEST_F(AvroDirectEncoderDecoderTest, TestBooleanType) {
    std::string schema_json = R"({"type": "boolean"})";
    arrow::BooleanBuilder builder;
    ASSERT_TRUE(builder.Append(true).ok());
    ASSERT_TRUE(builder.Append(false).ok());
    ASSERT_TRUE(builder.Append(true).ok());
    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(builder.Finish(&input_array).ok());
    CheckResult(schema_json, input_array, &builder);
}

TEST_F(AvroDirectEncoderDecoderTest, TestIntegerTypes) {
    // Test INT8
    {
        std::string schema_json = R"({"type": "int"})";
        arrow::Int8Builder builder;
        ASSERT_TRUE(builder.Append(1).ok());
        ASSERT_TRUE(builder.Append(-128).ok());
        ASSERT_TRUE(builder.Append(127).ok());
        std::shared_ptr<arrow::Array> input_array;
        ASSERT_TRUE(builder.Finish(&input_array).ok());
        CheckResult(schema_json, input_array, &builder);
    }

    // Test INT32
    {
        std::string schema_json = R"({"type": "int"})";
        arrow::Int32Builder builder;
        ASSERT_TRUE(builder.Append(42).ok());
        ASSERT_TRUE(builder.Append(-2147483648).ok());
        ASSERT_TRUE(builder.Append(2147483647).ok());
        std::shared_ptr<arrow::Array> input_array;
        ASSERT_TRUE(builder.Finish(&input_array).ok());
        CheckResult(schema_json, input_array, &builder);
    }

    // Test INT64
    {
        std::string schema_json = R"({"type": "long"})";
        arrow::Int64Builder builder;
        ASSERT_TRUE(builder.Append(123456789L).ok());
        ASSERT_TRUE(builder.Append(-9223372036854775807L).ok());
        ASSERT_TRUE(builder.Append(9223372036854775807L).ok());
        std::shared_ptr<arrow::Array> input_array;
        ASSERT_TRUE(builder.Finish(&input_array).ok());
        CheckResult(schema_json, input_array, &builder);
    }
}

TEST_F(AvroDirectEncoderDecoderTest, TestFloatingPointTypes) {
    // Test FLOAT
    {
        std::string schema_json = R"({"type": "float"})";
        arrow::FloatBuilder builder;
        ASSERT_TRUE(builder.Append(3.14f).ok());
        ASSERT_TRUE(builder.Append(-2.71f).ok());
        ASSERT_TRUE(builder.Append(0.0f).ok());
        std::shared_ptr<arrow::Array> input_array;
        ASSERT_TRUE(builder.Finish(&input_array).ok());
        CheckResult(schema_json, input_array, &builder);
    }

    // Test DOUBLE
    {
        std::string schema_json = R"({"type": "double"})";
        arrow::DoubleBuilder builder;
        ASSERT_TRUE(builder.Append(3.141592653589793).ok());
        ASSERT_TRUE(builder.Append(-2.718281828459045).ok());
        ASSERT_TRUE(builder.Append(0.0).ok());
        std::shared_ptr<arrow::Array> input_array;
        ASSERT_TRUE(builder.Finish(&input_array).ok());
        CheckResult(schema_json, input_array, &builder);
    }
}

TEST_F(AvroDirectEncoderDecoderTest, TestStringType) {
    std::string schema_json = R"({"type": "string"})";
    arrow::StringBuilder builder;
    ASSERT_TRUE(builder.Append("hello").ok());
    ASSERT_TRUE(builder.Append("world").ok());
    ASSERT_TRUE(builder.Append("").ok());
    ASSERT_TRUE(builder.Append("测试中文").ok());
    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(builder.Finish(&input_array).ok());
    CheckResult(schema_json, input_array, &builder);
}

TEST_F(AvroDirectEncoderDecoderTest, TestBytesType) {
    std::string schema_json = R"({"type": "bytes"})";
    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append("binary_data").ok());
    ASSERT_TRUE(builder.Append(std::string("\x00\x01\x02\x03", 4)).ok());
    ASSERT_TRUE(builder.Append("").ok());
    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(builder.Finish(&input_array).ok());
    CheckResult(schema_json, input_array, &builder);
}

TEST_F(AvroDirectEncoderDecoderTest, TestDate32Type) {
    std::string schema_json = R"({"type": "int", "logicalType": "date"})";
    arrow::Date32Builder builder;
    ASSERT_TRUE(builder.Append(18628).ok());  // 2021-01-01
    ASSERT_TRUE(builder.Append(0).ok());      // 1970-01-01
    ASSERT_TRUE(builder.Append(-1).ok());     // 1969-12-31
    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(builder.Finish(&input_array).ok());
    CheckResult(schema_json, input_array, &builder);
}

TEST_F(AvroDirectEncoderDecoderTest, TestTimestampType) {
    // Test timestamp-millis
    {
        std::string schema_json = R"({"type": "long", "logicalType": "timestamp-millis"})";
        arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MILLI),
                                        arrow::default_memory_pool());
        ASSERT_TRUE(builder.Append(1609459200123L).ok());  // 2021-01-01 00:00:00.123
        ASSERT_TRUE(builder.Append(0L).ok());              // 1970-01-01 00:00:00
        std::shared_ptr<arrow::Array> input_array;
        ASSERT_TRUE(builder.Finish(&input_array).ok());
        CheckResult(schema_json, input_array, &builder);
    }

    // Test timestamp-micros
    {
        std::string schema_json = R"({"type": "long", "logicalType": "timestamp-micros"})";
        arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MICRO),
                                        arrow::default_memory_pool());
        ASSERT_TRUE(builder.Append(1609459200123123L).ok());  // 2021-01-01 00:00:00.123123
        ASSERT_TRUE(builder.Append(0L).ok());                 // 1970-01-01 00:00:00
        std::shared_ptr<arrow::Array> input_array;
        ASSERT_TRUE(builder.Finish(&input_array).ok());
        CheckResult(schema_json, input_array, &builder);
    }
}

TEST_F(AvroDirectEncoderDecoderTest, TestUnionType) {
    // Test nullable int (union of null and int)
    std::string schema_json = R"(["null", "int"])";
    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.Append(42).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Append(100).ok());
    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(builder.Finish(&input_array).ok());
    CheckResult(schema_json, input_array, &builder);
}

TEST_F(AvroDirectEncoderDecoderTest, TestRecordType) {
    std::string schema_json = R"({
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "active", "type": "boolean"}
        ]
    })";

    // Create struct array
    auto int_field = arrow::field("id", arrow::int32());
    auto string_field = arrow::field("name", arrow::utf8());
    auto bool_field = arrow::field("active", arrow::boolean());
    auto struct_type = arrow::struct_({int_field, string_field, bool_field});

    arrow::StructBuilder struct_builder(
        struct_type, arrow::default_memory_pool(),
        {std::make_shared<arrow::Int32Builder>(), std::make_shared<arrow::StringBuilder>(),
         std::make_shared<arrow::BooleanBuilder>()});

    auto int_builder = static_cast<arrow::Int32Builder*>(struct_builder.field_builder(0));
    auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(1));
    auto bool_builder = static_cast<arrow::BooleanBuilder*>(struct_builder.field_builder(2));

    // Add first record
    ASSERT_TRUE(struct_builder.Append().ok());
    ASSERT_TRUE(int_builder->Append(1).ok());
    ASSERT_TRUE(string_builder->Append("Alice").ok());
    ASSERT_TRUE(bool_builder->Append(true).ok());

    // Add second record
    ASSERT_TRUE(struct_builder.Append().ok());
    ASSERT_TRUE(int_builder->Append(2).ok());
    ASSERT_TRUE(string_builder->Append("Bob").ok());
    ASSERT_TRUE(bool_builder->Append(false).ok());

    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(struct_builder.Finish(&input_array).ok());
    CheckResult(schema_json, input_array, &struct_builder);
}

TEST_F(AvroDirectEncoderDecoderTest, TestDecodeWithProjection) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),
        arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),
        arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),
        arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),
        arrow::field("f8", arrow::binary()),
        arrow::field("f9", arrow::map(arrow::float64(), arrow::float64())),
        arrow::field("f10", arrow::map(arrow::utf8(), arrow::utf8())),
        arrow::field("f11", arrow::list(arrow::float32())),
        arrow::field("f12", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                            arrow::field("f1", arrow::int64())})),
        arrow::field("f13", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("f14", arrow::date32()),
        arrow::field("f15", arrow::decimal128(2, 2)),
        arrow::field("f16", arrow::decimal128(10, 10)),
        arrow::field("f17", arrow::decimal128(19, 19))};

    std::shared_ptr<arrow::Array> src_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [true, 127, 32767, 2147483647, 9999999999999, 1234.56, 1234567890.0987654321, "aa", "qq", [[1.1,10.1],[2.2,20.2]], [["key1","val1"],["key2","val2"]], [0.1, 0.2], [true, null], "1970-01-01 00:02:03.123123", 2456, "0.22", "0.1234567890", "0.1234567890987654321"],
        [false, -128, -32768, -2147483648, -9999999999999, -1234.56, -1234567890.0987654321, null, "ww", [[1.11,10.11],[2.22,20.22]], [["key11","val11"],["key22","val22"]], [-0.1, -0.2, null, 0.3, 0.4], [null, 2], "1970-01-01 00:16:39.999999", null, "-0.22", "-0.1234567890", null],
        [null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]
    ])")
            .ValueOrDie();

    // no skip
    CheckResultWithProjection(src_array,
                              {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17});
    // skip bool
    CheckResultWithProjection(src_array,
                              {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17});
    // skip int
    CheckResultWithProjection(src_array, {0, 1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17});
    // skip long
    CheckResultWithProjection(src_array,
                              {0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17});
    // skip float
    CheckResultWithProjection(src_array,
                              {0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17});
    // skip double
    CheckResultWithProjection(src_array,
                              {0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17});
    // skip string
    CheckResultWithProjection(src_array,
                              {0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17});
    // skip binary
    CheckResultWithProjection(src_array,
                              {0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17});
    // skip map
    CheckResultWithProjection(src_array,
                              {0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17});
    // skip array-based map
    CheckResultWithProjection(src_array,
                              {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17});
    // skip list
    CheckResultWithProjection(src_array,
                              {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17});
    // skip struct
    CheckResultWithProjection(src_array,
                              {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17});
    // skip others
    CheckResultWithProjection(src_array, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
    // skip null and union is already tested in above test cases
}

TEST_F(AvroDirectEncoderDecoderTest, TestArrayType) {
    std::string schema_json = R"({
        "type": "array",
        "items": "int"
    })";

    // Create list array
    arrow::ListBuilder list_builder(arrow::default_memory_pool(),
                                    std::make_shared<arrow::Int32Builder>());
    auto int_builder = static_cast<arrow::Int32Builder*>(list_builder.value_builder());

    // First list: [1, 2, 3]
    ASSERT_TRUE(list_builder.Append().ok());
    ASSERT_TRUE(int_builder->Append(1).ok());
    ASSERT_TRUE(int_builder->Append(2).ok());
    ASSERT_TRUE(int_builder->Append(3).ok());

    // Second list: []
    ASSERT_TRUE(list_builder.Append().ok());

    // Third list: [42]
    ASSERT_TRUE(list_builder.Append().ok());
    ASSERT_TRUE(int_builder->Append(42).ok());

    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(list_builder.Finish(&input_array).ok());
    CheckResult(schema_json, input_array, &list_builder);
}

TEST_F(AvroDirectEncoderDecoderTest, TestMapType) {
    std::string schema_json = R"({
        "type": "map",
        "values": "string"
    })";

    // Create map array
    arrow::MapBuilder map_builder(arrow::default_memory_pool(),
                                  std::make_shared<arrow::StringBuilder>(),
                                  std::make_shared<arrow::StringBuilder>());
    auto key_builder = static_cast<arrow::StringBuilder*>(map_builder.key_builder());
    auto value_builder = static_cast<arrow::StringBuilder*>(map_builder.item_builder());

    // First map: {"key1": "value1", "key2": "value2"}
    ASSERT_TRUE(map_builder.Append().ok());
    ASSERT_TRUE(key_builder->Append("key1").ok());
    ASSERT_TRUE(value_builder->Append("value1").ok());
    ASSERT_TRUE(key_builder->Append("key2").ok());
    ASSERT_TRUE(value_builder->Append("value2").ok());

    // Second map: {}
    ASSERT_TRUE(map_builder.Append().ok());

    // Third map: {"single": "entry"}
    ASSERT_TRUE(map_builder.Append().ok());
    ASSERT_TRUE(key_builder->Append("single").ok());
    ASSERT_TRUE(value_builder->Append("entry").ok());

    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(map_builder.Finish(&input_array).ok());
    CheckResult(schema_json, input_array, &map_builder);
}

TEST_F(AvroDirectEncoderDecoderTest, TestArrayBasedMapType) {
    std::string schema_json = R"({
        "type" : "array",
        "items" : {
            "type" : "record",
            "name" : "record_f1",
            "fields" : [ {
                "name" : "key",
                "type" : "int"
            }, {
                "name" : "value",
                "type" : "string"
            } ]
        },
        "logicalType" : "map"
    })";

    // Create map array
    arrow::MapBuilder map_builder(arrow::default_memory_pool(),
                                  std::make_shared<arrow::Int32Builder>(),
                                  std::make_shared<arrow::StringBuilder>());
    auto key_builder = static_cast<arrow::Int32Builder*>(map_builder.key_builder());
    auto value_builder = static_cast<arrow::StringBuilder*>(map_builder.item_builder());

    // First map: {111: "value1", 222: "value2"}
    ASSERT_TRUE(map_builder.Append().ok());
    ASSERT_TRUE(key_builder->Append(111).ok());
    ASSERT_TRUE(value_builder->Append("value1").ok());
    ASSERT_TRUE(key_builder->Append(222).ok());
    ASSERT_TRUE(value_builder->Append("value2").ok());

    // Second map: {}
    ASSERT_TRUE(map_builder.Append().ok());

    // Third map: {333: "entry"}
    ASSERT_TRUE(map_builder.Append().ok());
    ASSERT_TRUE(key_builder->Append(333).ok());
    ASSERT_TRUE(value_builder->Append("entry").ok());

    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(map_builder.Finish(&input_array).ok());
    CheckResult(schema_json, input_array, &map_builder);
}

TEST_F(AvroDirectEncoderDecoderTest, TestDecimalType) {
    std::string schema_json = R"({
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 10,
        "scale": 2
    })";

    // Create decimal array
    auto decimal_type = arrow::decimal128(10, 2);
    arrow::Decimal128Builder builder(decimal_type);

    ASSERT_TRUE(builder.Append(arrow::Decimal128("123.45")).ok());
    ASSERT_TRUE(builder.Append(arrow::Decimal128("-67.89")).ok());
    ASSERT_TRUE(builder.Append(arrow::Decimal128("0.00")).ok());

    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(builder.Finish(&input_array).ok());
    CheckResult(schema_json, input_array, &builder);
}

TEST_F(AvroDirectEncoderDecoderTest, TestEncoderErrorCases) {
    std::string schema_json = R"({"type": "int"})";
    auto avro_schema = ::avro::compileJsonSchemaFromString(schema_json);
    auto output_stream = ::avro::memoryOutputStream();
    auto encoder = ::avro::binaryEncoder();
    encoder->init(*output_stream);

    {
        // Test out of bounds row index
        arrow::Int32Builder builder;
        ASSERT_TRUE(builder.Append(42).ok());
        std::shared_ptr<arrow::Array> input_array;
        ASSERT_TRUE(builder.Finish(&input_array).ok());

        ASSERT_NOK_WITH_MSG(AvroDirectEncoder::EncodeArrowToAvro(avro_schema.root(), *input_array,
                                                                 -1, encoder.get(), &encode_ctx_),
                            "Row index -1 out of bounds 1");
        ASSERT_NOK_WITH_MSG(AvroDirectEncoder::EncodeArrowToAvro(avro_schema.root(), *input_array,
                                                                 1, encoder.get(), &encode_ctx_),
                            "Row index 1 out of bounds 1");
    }
    {
        // Test null value in non-nullable field
        arrow::Int32Builder nullable_builder;
        ASSERT_TRUE(nullable_builder.AppendNull().ok());
        std::shared_ptr<arrow::Array> nullable_array;
        ASSERT_TRUE(nullable_builder.Finish(&nullable_array).ok());

        ASSERT_NOK_WITH_MSG(
            AvroDirectEncoder::EncodeArrowToAvro(avro_schema.root(), *nullable_array, 0,
                                                 encoder.get(), &encode_ctx_),
            "Null value in non-nullable field");
    }
}

TEST_F(AvroDirectEncoderDecoderTest, TestDecoderErrorCases) {
    std::string schema_json = R"(["null", "int"])";
    auto avro_schema = ::avro::compileJsonSchemaFromString(schema_json);

    // Test with invalid union branch index, branch index 2, but union only has 2 branches (0,1)
    std::vector<uint8_t> invalid_data = {0x04};
    auto input_stream = ::avro::memoryInputStream(invalid_data.data(), invalid_data.size());
    auto decoder = ::avro::binaryDecoder();
    decoder->init(*input_stream);

    arrow::Int32Builder builder;
    ASSERT_NOK_WITH_MSG(
        AvroDirectDecoder::DecodeAvroToBuilder(avro_schema.root(), std::nullopt, decoder.get(),
                                               &builder, &decode_ctx_),
        "Union branch index 2 out of range [0, 2)");
}

TEST_F(AvroDirectEncoderDecoderTest, TestInvalidUnionType) {
    auto run = [&](const std::string& schema_json, const std::string& error_msg) {
        auto avro_schema = ::avro::compileJsonSchemaFromString(schema_json);
        auto output_stream = ::avro::memoryOutputStream();
        auto encoder = ::avro::binaryEncoder();
        encoder->init(*output_stream);

        arrow::Int32Builder builder;
        ASSERT_TRUE(builder.Append(42).ok());
        std::shared_ptr<arrow::Array> input_array;
        ASSERT_TRUE(builder.Finish(&input_array).ok());

        ASSERT_NOK_WITH_MSG(AvroDirectEncoder::EncodeArrowToAvro(avro_schema.root(), *input_array,
                                                                 0, encoder.get(), &encode_ctx_),
                            error_msg);
    };
    // Test union with more than 2 branches
    run(R"(["null", "int", "string"])", "Union must have exactly 2 branches, got 3");
    // Test union with null branch not first
    run(R"(["int", "null"])",
        "Unexpected: In paimon, we expect the null branch to be the first branch in a union.");
}

TEST_F(AvroDirectEncoderDecoderTest, TestInvalidMapType) {
    std::string schema_json = R"({
        "type": "map",
        "values": "string"
    })";
    auto avro_schema = ::avro::compileJsonSchemaFromString(schema_json);

    arrow::MapBuilder map_builder(arrow::default_memory_pool(),
                                  std::make_shared<arrow::Int32Builder>(),
                                  std::make_shared<arrow::StringBuilder>());
    auto key_builder = static_cast<arrow::Int32Builder*>(map_builder.key_builder());
    auto value_builder = static_cast<arrow::StringBuilder*>(map_builder.item_builder());
    ASSERT_TRUE(map_builder.Append().ok());
    ASSERT_TRUE(key_builder->Append(1).ok());
    ASSERT_TRUE(value_builder->Append("value1").ok());
    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(map_builder.Finish(&input_array).ok());

    ASSERT_NOK_WITH_MSG(EncodeData(avro_schema.root(), input_array),
                        "AVRO_MAP keys must be StringArray, got int32");
}

TEST_F(AvroDirectEncoderDecoderTest, TestInvalidArrayBasedMapType) {
    std::string schema_json = R"({
        "type" : "array",
        "items" : {
            "type" : "record",
            "name" : "record_f1",
            "fields" : [ {
                "name" : "key",
                "type" : "int"
            }, {
                "name" : "value",
                "type" : "string"
            }, {
                "name" : "metadata",
                "type" : "string"
            } ]
        },
        "logicalType" : "map"
    })";
    auto avro_schema = ::avro::compileJsonSchemaFromString(schema_json);

    arrow::MapBuilder map_builder(arrow::default_memory_pool(),
                                  std::make_shared<arrow::Int32Builder>(),
                                  std::make_shared<arrow::StringBuilder>());
    ASSERT_TRUE(map_builder.Append().ok());
    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(map_builder.Finish(&input_array).ok());

    ASSERT_NOK_WITH_MSG(EncodeData(avro_schema.root(), input_array),
                        "Expected AVRO_RECORD for map key-value pair");
}

#ifndef NDEBUG
TEST_F(AvroDirectEncoderDecoderTest, TestTypeMismatch) {
    // Test string schema with int array (The type-mismatch issue should not occur, so we only
    // perform type conversion checks in debug mode.)
    std::string schema_json = R"({"type": "string"})";
    auto avro_schema = ::avro::compileJsonSchemaFromString(schema_json);
    auto output_stream = ::avro::memoryOutputStream();
    auto encoder = ::avro::binaryEncoder();
    encoder->init(*output_stream);

    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.Append(42).ok());
    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(builder.Finish(&input_array).ok());

    ASSERT_THROW(auto status = AvroDirectEncoder::EncodeArrowToAvro(
                     avro_schema.root(), *input_array, 0, encoder.get(), &encode_ctx_),
                 std::bad_cast);
}
#endif

}  // namespace paimon::avro::test
