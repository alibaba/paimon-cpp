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

#include "paimon/common/types/data_type_json_parser.h"

#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon::test {

TEST(DataTypeJsonParserTest, ParseTypeArrayTypeSuccess) {
    const std::string name = "array_field";
    const char* json = R"({
        "type": "ARRAY",
        "element": "INT"
    })";
    rapidjson::Document doc;
    doc.Parse(json);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> field,
                         DataTypeJsonParser::ParseType(name, doc));
    ASSERT_NE(field, nullptr);
}

TEST(DataTypeJsonParserTest, ParseTypeMapTypeSuccess) {
    const std::string name = "map_field";
    const char* json = R"({
        "type": "MAP",
        "key": "STRING NOT NULL",
        "value": "INT"
    })";
    rapidjson::Document doc;
    doc.Parse(json);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> field,
                         DataTypeJsonParser::ParseType(name, doc));
    ASSERT_NE(field, nullptr);
}

TEST(DataTypeJsonParserTest, ParseTypeRowTypeSuccess) {
    const std::string name = "row_field";
    const char* json = R"({
      "type" : "ROW",
      "fields" : [ {
        "id" : 1,
        "name" : "sub1",
        "type" : "DATE"
      }, {
        "id" : 4,
        "name" : "sub4",
        "type" : "BYTES"
      }]})";
    rapidjson::Document doc;
    doc.Parse(json);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> field,
                         DataTypeJsonParser::ParseType(name, doc));
    ASSERT_NE(field, nullptr);
}

TEST(DataTypeJsonParserTest, ParseTypeAtomicTypeSuccess) {
    // List of atomic types and their expected Arrow types
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> test_cases = {
        {"BOOLEAN", arrow::boolean()},
        {"TINYINT", arrow::int8()},
        {"SMALLINT", arrow::int16()},
        {"INT", arrow::int32()},
        {"INTEGER", arrow::int32()},
        {"BIGINT", arrow::int64()},
        {"FLOAT", arrow::float32()},
        {"DOUBLE", arrow::float64()},
        {"DOUBLE PRECISION", arrow::float64()},
        {"DEC", arrow::decimal128(10, 0)},
        {"DEC(10)", arrow::decimal128(10, 0)},
        {"DEC(10, 3)", arrow::decimal128(10, 3)},
        {"DECIMAL", arrow::decimal128(10, 0)},
        {"DECIMAL(10)", arrow::decimal128(10, 0)},
        {"DECIMAL(10, 3)", arrow::decimal128(10, 3)},
        {"NUMERIC", arrow::decimal128(10, 0)},
        {"NUMERIC(10)", arrow::decimal128(10, 0)},
        {"NUMERIC(10, 3)", arrow::decimal128(10, 3)},
        {"TIMESTAMP(0)", arrow::timestamp(arrow::TimeUnit::SECOND)},
        {"TIMESTAMP(3)", arrow::timestamp(arrow::TimeUnit::MILLI)},
        {"TIMESTAMP(6)", arrow::timestamp(arrow::TimeUnit::MICRO)},
        {"TIMESTAMP(9)", arrow::timestamp(arrow::TimeUnit::NANO)},
        {"TIMESTAMP(9) WITHOUT TIME ZONE", arrow::timestamp(arrow::TimeUnit::NANO)},
        {"TIMESTAMP(9) WITH", arrow::timestamp(arrow::TimeUnit::NANO)},
        {"TIMESTAMP(9) WITH LOCAL TIME ZONE", arrow::timestamp(arrow::TimeUnit::NANO, timezone)},
        {"TIMESTAMP_LTZ(9)", arrow::timestamp(arrow::TimeUnit::NANO, timezone)},
        {"BYTES", arrow::binary()},
        {"STRING", arrow::utf8()},
    };

    for (const auto& test_case : test_cases) {
        const std::string& type_str = test_case.first;

        rapidjson::Document doc;
        rapidjson::Value value(type_str.data(), doc.GetAllocator());

        // Parse type and verify the result
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> field,
                             DataTypeJsonParser::ParseType("field_name", value));
        ASSERT_TRUE(field->type()->Equals(test_case.second));
    }

    // Invalid case
    {
        rapidjson::Document invalid_doc;
        rapidjson::Value value("VARCHAR(test)", invalid_doc.GetAllocator());
        ASSERT_NOK(DataTypeJsonParser::ParseType("field_name", value));
    }
    {
        rapidjson::Document invalid_doc;
        rapidjson::Value value("TIMESTAMP(4)", invalid_doc.GetAllocator());
        ASSERT_NOK_WITH_MSG(DataTypeJsonParser::ParseType("field_name", value),
                            "only support precision 0/3/6/9 in timestamp type");
    }
    {
        rapidjson::Document invalid_doc;
        rapidjson::Value value("TIMESTAMP(8) WITH LOCAL TIME ZONE", invalid_doc.GetAllocator());
        ASSERT_NOK_WITH_MSG(DataTypeJsonParser::ParseType("field_name", value),
                            "only support precision 0/3/6/9 in timestamp type");
    }
}

}  // namespace paimon::test
