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

#include "paimon/common/predicate/literal_converter.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <variant>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class LiteralConverterTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    void CheckResult(const std::shared_ptr<arrow::Array>& field_array,
                     const std::vector<Literal>& expected) const {
        ASSERT_OK_AND_ASSIGN(
            std::vector<Literal> literals,
            LiteralConverter::ConvertLiteralsFromArray(*field_array, /*own_data=*/false));
        ASSERT_EQ(literals.size(), expected.size());
        ASSERT_EQ(literals, expected);
    }

    void CheckLiteralsFromString(const FieldType& type, const std::vector<std::string>& strs,
                                 const std::vector<Literal>& expected) const {
        ASSERT_EQ(strs.size(), expected.size());
        for (size_t i = 0; i < strs.size(); i++) {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 LiteralConverter::ConvertLiteralsFromString(type, strs[i]));
            ASSERT_EQ(result, expected[i]);
        }
    }

    void CheckLiteralFromRow(const std::shared_ptr<arrow::DataType>& data_type,
                             const BinaryRowGenerator::ValueType& values, const FieldType& type,
                             const std::vector<Literal>& expected) const {
        auto pool = GetDefaultPool();
        auto schema = arrow::schema(arrow::FieldVector({arrow::field("f0", data_type)}));
        for (size_t i = 0; i < values.size(); i++) {
            // each value generates a row with 1 arity
            BinaryRow row = BinaryRowGenerator::GenerateRow({values[i]}, pool.get());
            ASSERT_OK_AND_ASSIGN(auto result, LiteralConverter::ConvertLiteralsFromRow(
                                                  schema, row, /*field_idx=*/0, type));
            ASSERT_EQ(result, expected[i]);
        }
    }
};

TEST_F(LiteralConverterTest, TestBooleanLiteral) {
    auto field_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::boolean(), R"([true, false, null])")
            .ValueOrDie();
    CheckResult(field_array,
                std::vector<Literal>({Literal(true), Literal(false), Literal(FieldType::BOOLEAN)}));
    CheckLiteralsFromString(
        FieldType::BOOLEAN, {"true", "false", "yes", "no"},
        std::vector<Literal>({Literal(true), Literal(false), Literal(true), Literal(false)}));
    CheckLiteralFromRow(arrow::boolean(), {true, false, NullType()}, FieldType::BOOLEAN,
                        {Literal(true), Literal(false), Literal(FieldType::BOOLEAN)});
}

TEST_F(LiteralConverterTest, TestTinyIntLiteral) {
    auto field_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int8(), R"([4, 5, null])").ValueOrDie();
    CheckResult(field_array, std::vector<Literal>({Literal(static_cast<int8_t>(4)),
                                                   Literal(static_cast<int8_t>(5)),
                                                   Literal(FieldType::TINYINT)}));
    CheckLiteralsFromString(
        FieldType::TINYINT, {"4", "5"},
        std::vector<Literal>({Literal(static_cast<int8_t>(4)), Literal(static_cast<int8_t>(5))}));
    CheckLiteralFromRow(arrow::int8(), {static_cast<int8_t>(4), static_cast<int8_t>(5), NullType()},
                        FieldType::TINYINT,
                        {Literal(static_cast<int8_t>(4)), Literal(static_cast<int8_t>(5)),
                         Literal(FieldType::TINYINT)});
}
TEST_F(LiteralConverterTest, TestSmallIntLiteral) {
    auto field_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int16(), R"([45, 55, null])").ValueOrDie();
    CheckResult(field_array, std::vector<Literal>({Literal(static_cast<int16_t>(45)),
                                                   Literal(static_cast<int16_t>(55)),
                                                   Literal(FieldType::SMALLINT)}));
    CheckLiteralsFromString(FieldType::SMALLINT, {"45", "55"},
                            std::vector<Literal>({Literal(static_cast<int16_t>(45)),
                                                  Literal(static_cast<int16_t>(55))}));
    CheckLiteralFromRow(arrow::int16(),
                        {static_cast<int16_t>(45), static_cast<int16_t>(55), NullType()},
                        FieldType::SMALLINT,
                        {Literal(static_cast<int16_t>(45)), Literal(static_cast<int16_t>(55)),
                         Literal(FieldType::SMALLINT)});
}
TEST_F(LiteralConverterTest, TestIntLiteral) {
    auto field_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([456, 567, null])")
            .ValueOrDie();
    CheckResult(field_array,
                std::vector<Literal>({Literal(456), Literal(567), Literal(FieldType::INT)}));
    CheckLiteralsFromString(FieldType::INT, {"456", "567"},
                            std::vector<Literal>({Literal(456), Literal(567)}));
    CheckLiteralFromRow(arrow::int32(),
                        {static_cast<int32_t>(456), static_cast<int32_t>(567), NullType()},
                        FieldType::INT,
                        {Literal(static_cast<int32_t>(456)), Literal(static_cast<int32_t>(567)),
                         Literal(FieldType::INT)});
}

TEST_F(LiteralConverterTest, TestBigIntLiteral) {
    auto field_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([4, 5, null])").ValueOrDie();
    CheckResult(field_array,
                std::vector<Literal>({Literal(4l), Literal(5l), Literal(FieldType::BIGINT)}));
    CheckLiteralsFromString(FieldType::BIGINT, {"4", "5"},
                            std::vector<Literal>({Literal(4l), Literal(5l)}));
    CheckLiteralFromRow(arrow::int64(),
                        {static_cast<int64_t>(4), static_cast<int64_t>(5), NullType()},
                        FieldType::BIGINT,
                        {Literal(static_cast<int64_t>(4)), Literal(static_cast<int64_t>(5)),
                         Literal(FieldType::BIGINT)});
}

TEST_F(LiteralConverterTest, TestFloatLiteral) {
    auto field_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::float32(), R"([4.0, 5.1, NaN, null])")
            .ValueOrDie();
    CheckResult(field_array,
                std::vector<Literal>(
                    {Literal(static_cast<float>(4.0)), Literal(static_cast<float>(5.1)),
                     Literal(static_cast<float>(std::nan(""))), Literal(FieldType::FLOAT)}));
    // literal from string do not support nan and inf
    CheckLiteralsFromString(FieldType::FLOAT, {"4.0", "5.1"},
                            std::vector<Literal>({Literal(4.0f), Literal(5.1f)}));
    CheckLiteralFromRow(arrow::float32(), {4.0f, 5.1f, INFINITY, -INFINITY, NAN, NullType()},
                        FieldType::FLOAT,
                        {Literal(4.0f), Literal(5.1f), Literal(INFINITY), Literal(-INFINITY),
                         Literal(NAN), Literal(FieldType::FLOAT)});
}

TEST_F(LiteralConverterTest, TestDoubleLiteral) {
    auto field_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::float64(), R"([4.05, 5.17, NaN, null])")
            .ValueOrDie();
    CheckResult(field_array,
                std::vector<Literal>({Literal(4.05), Literal(5.17), Literal(std::nan("")),
                                      Literal(FieldType::DOUBLE)}));
    // literal from string do not support nan and inf
    CheckLiteralsFromString(FieldType::DOUBLE, {"4.05", "5.17"},
                            std::vector<Literal>({Literal(4.05), Literal(5.17)}));
    CheckLiteralFromRow(arrow::float64(),
                        {4.05, 5.17, static_cast<double> INFINITY, static_cast<double>(-INFINITY),
                         static_cast<double> NAN, NullType()},
                        FieldType::DOUBLE,
                        {Literal(4.05), Literal(5.17), Literal(static_cast<double> INFINITY),
                         Literal(static_cast<double>(-INFINITY)), Literal(static_cast<double> NAN),
                         Literal(FieldType::DOUBLE)});
}

TEST_F(LiteralConverterTest, TestStringLiteral) {
    auto field_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["apple", "苹果", null])")
            .ValueOrDie();
    std::string str = "苹果";
    CheckResult(field_array,
                std::vector<Literal>({Literal(FieldType::STRING, "apple", 5),
                                      Literal(FieldType::STRING, str.data(), str.size()),
                                      Literal(FieldType::STRING)}));
    CheckLiteralsFromString(
        FieldType::STRING, {"apple", "苹果"},
        std::vector<Literal>({Literal(FieldType::STRING, "apple", 5),
                              Literal(FieldType::STRING, str.data(), str.size())}));
    CheckLiteralFromRow(
        arrow::utf8(), {std::string("apple"), std::string("苹果"), NullType()}, FieldType::STRING,
        {Literal(FieldType::STRING, "apple", 5), Literal(FieldType::STRING, str.data(), str.size()),
         Literal(FieldType::STRING)});
}

TEST_F(LiteralConverterTest, TestBinaryLiteral) {
    auto field_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::binary(), R"(["apple", "苹果", null])")
            .ValueOrDie();
    std::string str = "苹果";
    CheckResult(field_array,
                std::vector<Literal>({Literal(FieldType::BINARY, "apple", 5),
                                      Literal(FieldType::BINARY, str.data(), str.size()),
                                      Literal(FieldType::BINARY)}));
    CheckLiteralsFromString(
        FieldType::BINARY, {"apple", "苹果"},
        std::vector<Literal>({Literal(FieldType::BINARY, "apple", 5),
                              Literal(FieldType::BINARY, str.data(), str.size())}));
    CheckLiteralFromRow(
        arrow::binary(), {std::string("apple"), std::string("苹果"), NullType()}, FieldType::BINARY,
        {Literal(FieldType::BINARY, "apple", 5), Literal(FieldType::BINARY, str.data(), str.size()),
         Literal(FieldType::BINARY)});
}

TEST_F(LiteralConverterTest, TestTimestampLiteral) {
    {
        // nano
        std::string timestamp_json =
            R"(["1970-01-01T00:00:59.123456789", "2000-02-29T23:23:23.999999999",
          "1899-01-01T00:59:20.001001001", "2033-05-18T03:33:20.000000000",
          "2020-01-01T01:05:05.001", "2010-01-03T06:30:30.006163",
          "2010-01-04T07:35:35", "2008-12-28", "2012-01-01 01:02:03", null])";
        auto field_array = arrow::ipc::internal::json::ArrayFromJSON(
                               arrow::timestamp(arrow::TimeUnit::NANO), timestamp_json)
                               .ValueOrDie();
        CheckResult(
            field_array,
            std::vector<Literal>(
                {Literal(Timestamp(59123l, 456789)), Literal(Timestamp(951866603999l, 999999)),
                 Literal(Timestamp(-2240521239999l, 1001)), Literal(Timestamp(2000000000000l, 0)),
                 Literal(Timestamp(1577840705001l, 0)), Literal(Timestamp(1262500230006l, 163000)),
                 Literal(Timestamp(1262590535000l, 0)), Literal(Timestamp(1230422400000l, 0)),
                 Literal(Timestamp(1325379723000l, 0)), Literal(FieldType::TIMESTAMP)}));
        CheckLiteralFromRow(
            arrow::timestamp(arrow::TimeUnit::NANO),
            {TimestampType(Timestamp(59123l, 456789), 9),
             TimestampType(Timestamp(951866603999l, 999999), 9),
             TimestampType(Timestamp(-2240521239999l, 1001), 9),
             TimestampType(Timestamp(2000000000000l, 0), 9),
             TimestampType(Timestamp(1577840705001l, 0), 9),
             TimestampType(Timestamp(1262500230006l, 163000), 9),
             TimestampType(Timestamp(1262590535000l, 0), 9),
             TimestampType(Timestamp(1230422400000l, 0), 9),
             TimestampType(Timestamp(1325379723000l, 0), 9), NullType()},
            FieldType::TIMESTAMP,
            {Literal(Timestamp(59123l, 456789)), Literal(Timestamp(951866603999l, 999999)),
             Literal(Timestamp(-2240521239999l, 1001)), Literal(Timestamp(2000000000000l, 0)),
             Literal(Timestamp(1577840705001l, 0)), Literal(Timestamp(1262500230006l, 163000)),
             Literal(Timestamp(1262590535000l, 0)), Literal(Timestamp(1230422400000l, 0)),
             Literal(Timestamp(1325379723000l, 0)), Literal(FieldType::TIMESTAMP)});
    }
    {
        // second
        std::string timestamp_json =
            R"(["1970-01-01T00:00:59", "2000-02-29T23:23:23",
          "1899-01-01T00:59:20", "2033-05-18T03:33:20",
          "2020-01-01T01:05:05", "2010-01-03T06:30:30",
          "2010-01-04T07:35:35", "2008-12-28", "2012-01-01 01:02:03", null])";
        auto field_array = arrow::ipc::internal::json::ArrayFromJSON(
                               arrow::timestamp(arrow::TimeUnit::SECOND), timestamp_json)
                               .ValueOrDie();
        CheckResult(
            field_array,
            std::vector<Literal>(
                {Literal(Timestamp(59000l, 0)), Literal(Timestamp(951866603000l, 0)),
                 Literal(Timestamp(-2240521240000l, 0)), Literal(Timestamp(2000000000000l, 0)),
                 Literal(Timestamp(1577840705000l, 0)), Literal(Timestamp(1262500230000l, 0)),
                 Literal(Timestamp(1262590535000l, 0)), Literal(Timestamp(1230422400000l, 0)),
                 Literal(Timestamp(1325379723000l, 0)), Literal(FieldType::TIMESTAMP)}));
        CheckLiteralFromRow(
            arrow::timestamp(arrow::TimeUnit::SECOND),
            {TimestampType(Timestamp(59000l, 0), 0), TimestampType(Timestamp(951866603000l, 0), 0),
             TimestampType(Timestamp(-2240521240000l, 0), 0),
             TimestampType(Timestamp(2000000000000l, 0), 0),
             TimestampType(Timestamp(1577840705000l, 0), 0),
             TimestampType(Timestamp(1262500230000l, 0), 0),
             TimestampType(Timestamp(1262590535000l, 0), 0),
             TimestampType(Timestamp(1230422400000l, 0), 0),
             TimestampType(Timestamp(1325379723000l, 0), 0), NullType()},
            FieldType::TIMESTAMP,
            {Literal(Timestamp(59000l, 0)), Literal(Timestamp(951866603000l, 0)),
             Literal(Timestamp(-2240521240000l, 0)), Literal(Timestamp(2000000000000l, 0)),
             Literal(Timestamp(1577840705000l, 0)), Literal(Timestamp(1262500230000l, 0)),
             Literal(Timestamp(1262590535000l, 0)), Literal(Timestamp(1230422400000l, 0)),
             Literal(Timestamp(1325379723000l, 0)), Literal(FieldType::TIMESTAMP)});
    }
    {
        // milli
        std::string timestamp_json =
            R"(["1970-01-01T00:00:59.001", "2000-02-29T23:23:23.001",
          "1899-01-01T00:59:20.001", "2033-05-18T03:33:20.001",
          "2020-01-01T01:05:05.001", "2010-01-03T06:30:30.001",
          "2010-01-04T07:35:35.001", "2008-12-28", "2012-01-01 01:02:03.001", null])";
        auto field_array = arrow::ipc::internal::json::ArrayFromJSON(
                               arrow::timestamp(arrow::TimeUnit::MILLI), timestamp_json)
                               .ValueOrDie();
        CheckResult(
            field_array,
            std::vector<Literal>(
                {Literal(Timestamp(59001l, 0)), Literal(Timestamp(951866603001l, 0)),
                 Literal(Timestamp(-2240521239999l, 0)), Literal(Timestamp(2000000000001l, 0)),
                 Literal(Timestamp(1577840705001l, 0)), Literal(Timestamp(1262500230001l, 0)),
                 Literal(Timestamp(1262590535001l, 0)), Literal(Timestamp(1230422400000l, 0)),
                 Literal(Timestamp(1325379723001l, 0)), Literal(FieldType::TIMESTAMP)}));
        CheckLiteralFromRow(
            arrow::timestamp(arrow::TimeUnit::MILLI),
            {TimestampType(Timestamp(59001l, 0), 3), TimestampType(Timestamp(951866603001l, 0), 3),
             TimestampType(Timestamp(-2240521239999l, 0), 3),
             TimestampType(Timestamp(2000000000001l, 0), 3),
             TimestampType(Timestamp(1577840705001l, 0), 3),
             TimestampType(Timestamp(1262500230001l, 0), 3),
             TimestampType(Timestamp(1262590535001l, 0), 3),
             TimestampType(Timestamp(1230422400000l, 0), 3),
             TimestampType(Timestamp(1325379723001l, 0), 3), NullType()},
            FieldType::TIMESTAMP,
            {Literal(Timestamp(59001l, 0)), Literal(Timestamp(951866603001l, 0)),
             Literal(Timestamp(-2240521239999l, 0)), Literal(Timestamp(2000000000001l, 0)),
             Literal(Timestamp(1577840705001l, 0)), Literal(Timestamp(1262500230001l, 0)),
             Literal(Timestamp(1262590535001l, 0)), Literal(Timestamp(1230422400000l, 0)),
             Literal(Timestamp(1325379723001l, 0)), Literal(FieldType::TIMESTAMP)});
    }
    {
        // micro
        std::string timestamp_json =
            R"(["1970-01-01T00:00:59.001001", "2000-02-29T23:23:23.001001",
          "1899-01-01T00:59:20.001001", "2033-05-18T03:33:20.001001",
          "2020-01-01T01:05:05.001001", "2010-01-03T06:30:30.001001",
          "2010-01-04T07:35:35.001001", "2008-12-28", "2012-01-01 01:02:03.001001", null])";
        auto field_array = arrow::ipc::internal::json::ArrayFromJSON(
                               arrow::timestamp(arrow::TimeUnit::MICRO), timestamp_json)
                               .ValueOrDie();
        CheckResult(
            field_array,
            std::vector<Literal>(
                {Literal(Timestamp(59001l, 1000)), Literal(Timestamp(951866603001l, 1000)),
                 Literal(Timestamp(-2240521239999l, 1000)),
                 Literal(Timestamp(2000000000001l, 1000)), Literal(Timestamp(1577840705001l, 1000)),
                 Literal(Timestamp(1262500230001l, 1000)), Literal(Timestamp(1262590535001l, 1000)),
                 Literal(Timestamp(1230422400000l, 0)), Literal(Timestamp(1325379723001l, 1000)),
                 Literal(FieldType::TIMESTAMP)}));
        CheckLiteralFromRow(
            arrow::timestamp(arrow::TimeUnit::MICRO),
            {TimestampType(Timestamp(59001l, 1000), 6),
             TimestampType(Timestamp(951866603001l, 1000), 6),
             TimestampType(Timestamp(-2240521239999l, 1000), 6),
             TimestampType(Timestamp(2000000000001l, 1000), 6),
             TimestampType(Timestamp(1577840705001l, 1000), 6),
             TimestampType(Timestamp(1262500230001l, 1000), 6),
             TimestampType(Timestamp(1262590535001l, 1000), 6),
             TimestampType(Timestamp(1230422400000l, 0), 6),
             TimestampType(Timestamp(1325379723001l, 1000), 6), NullType()},
            FieldType::TIMESTAMP,
            {Literal(Timestamp(59001l, 1000)), Literal(Timestamp(951866603001l, 1000)),
             Literal(Timestamp(-2240521239999l, 1000)), Literal(Timestamp(2000000000001l, 1000)),
             Literal(Timestamp(1577840705001l, 1000)), Literal(Timestamp(1262500230001l, 1000)),
             Literal(Timestamp(1262590535001l, 1000)), Literal(Timestamp(1230422400000l, 0)),
             Literal(Timestamp(1325379723001l, 1000)), Literal(FieldType::TIMESTAMP)});
    }
}

TEST_F(LiteralConverterTest, TestDecimalLiteral) {
    auto field_array =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::decimal128(21, 3),
            R"(["-123456789987654321.234", "123456789987654321.012", "0.000", "123.456", "-123.456", null])")
            .ValueOrDie();
    CheckResult(
        field_array,
        std::vector<Literal>(
            {Literal(Decimal(21, 3, DecimalUtils::StrToInt128("-123456789987654321234").value())),
             Literal(Decimal(21, 3, DecimalUtils::StrToInt128("123456789987654321012").value())),
             Literal(Decimal(21, 3, 0)), Literal(Decimal(21, 3, 123456)),
             Literal(Decimal(21, 3, -123456)), Literal(FieldType::DECIMAL)}));
    CheckLiteralFromRow(
        arrow::decimal128(38, 3),
        {Decimal(38, 3, DecimalUtils::StrToInt128("-123456789987654338234").value()),
         Decimal(38, 3, DecimalUtils::StrToInt128("123456789987654338012").value()),
         Decimal(38, 3, 0), Decimal(38, 3, 123456), Decimal(38, 3, -123456), NullType()},
        FieldType::DECIMAL,
        {Literal(Decimal(38, 3, DecimalUtils::StrToInt128("-123456789987654338234").value())),
         Literal(Decimal(38, 3, DecimalUtils::StrToInt128("123456789987654338012").value())),
         Literal(Decimal(38, 3, 0)), Literal(Decimal(38, 3, 123456)),
         Literal(Decimal(38, 3, -123456)), Literal(FieldType::DECIMAL)});
}

TEST_F(LiteralConverterTest, TestDateLiteral) {
    auto field_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::date32(), R"([0, 4, -5, null])")
            .ValueOrDie();
    CheckResult(field_array,
                std::vector<Literal>({Literal(FieldType::DATE, 0l), Literal(FieldType::DATE, 4l),
                                      Literal(FieldType::DATE, -5l), Literal(FieldType::DATE)}));
    CheckLiteralFromRow(arrow::date32(), {0, 4, -5, NullType()}, FieldType::DATE,
                        {Literal(FieldType::DATE, 0l), Literal(FieldType::DATE, 4l),
                         Literal(FieldType::DATE, -5l), Literal(FieldType::DATE)});
}

TEST_F(LiteralConverterTest, TestInvalidType) {
    auto field_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::large_utf8(), R"(["apple", "苹果", null])")
            .ValueOrDie();
    ASSERT_NOK_WITH_MSG(
        LiteralConverter::ConvertLiteralsFromArray(*field_array, /*own_data=*/false),
        "Not support literal on arrow large_string type");
}

TEST_F(LiteralConverterTest, TestDictType) {
    auto dict = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["foo", "bar", "baz"])")
                    .ValueOrDie();
    auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    auto indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 0, 2, 0, null]")
            .ValueOrDie();
    std::shared_ptr<arrow::DictionaryArray> field_array =
        std::make_shared<arrow::DictionaryArray>(dict_type, indices, dict);

    CheckResult(field_array,
                std::vector<Literal>(
                    {Literal(FieldType::STRING, "bar", 3), Literal(FieldType::STRING, "baz", 3),
                     Literal(FieldType::STRING, "foo", 3), Literal(FieldType::STRING, "baz", 3),
                     Literal(FieldType::STRING, "foo", 3), Literal(FieldType::STRING)}));
}

}  // namespace paimon::test
