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

#include "paimon/core/utils/fields_comparator.h"

#include <cstddef>
#include <string>
#include <variant>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class FieldsComparatorTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    void CheckResult(const InternalRow& row1, const InternalRow& row2,
                     const std::vector<std::shared_ptr<arrow::DataType>>& input_types,
                     const std::vector<int32_t>& sort_fields, bool has_null = false) {
        std::vector<DataField> data_fields;
        data_fields.reserve(input_types.size());
        for (const auto& type : input_types) {
            data_fields.emplace_back(/*id=*/0, arrow::field("fake_name", type));
        }
        ASSERT_OK_AND_ASSIGN(auto comp1, FieldsComparator::Create(data_fields, sort_fields,
                                                                  /*is_ascending_order=*/true,
                                                                  /*use_view=*/false));
        ASSERT_EQ(-1, comp1->CompareTo(row1, row2));
        ASSERT_EQ(1, comp1->CompareTo(row2, row1));
        ASSERT_EQ(0, comp1->CompareTo(row1, row1));
        ASSERT_EQ(0, comp1->CompareTo(row2, row2));

        if (!has_null) {
            ASSERT_OK_AND_ASSIGN(auto comp2, FieldsComparator::Create(data_fields, sort_fields,
                                                                      /*is_ascending_order=*/false,
                                                                      /*use_view=*/false));
            ASSERT_EQ(1, comp2->CompareTo(row1, row2));
            ASSERT_EQ(-1, comp2->CompareTo(row2, row1));
            ASSERT_EQ(0, comp2->CompareTo(row1, row1));
            ASSERT_EQ(0, comp2->CompareTo(row2, row2));
        }
    }

    void CheckResult(const InternalRow& row1, const InternalRow& row2,
                     const std::vector<std::shared_ptr<arrow::DataType>>& input_types,
                     bool has_null = false) {
        std::vector<int32_t> sort_fields;
        for (size_t i = 0; i < input_types.size(); ++i) {
            sort_fields.push_back(i);
        }
        CheckResult(row1, row2, input_types, sort_fields, has_null);
    }
};

TEST_F(FieldsComparatorTest, TestSimple) {
    auto pool = GetDefaultPool();
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {false, static_cast<int8_t>(10), static_cast<int16_t>(100),
             static_cast<int32_t>(100000)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {true, static_cast<int8_t>(10), static_cast<int16_t>(100),
             static_cast<int32_t>(100000)},
            pool.get());
        CheckResult(row1, row2, {arrow::boolean(), arrow::int8(), arrow::int16(), arrow::int32()});
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {true, static_cast<int8_t>(10), static_cast<int16_t>(100),
             static_cast<int32_t>(100000)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {true, static_cast<int8_t>(20), static_cast<int16_t>(100),
             static_cast<int32_t>(100000)},
            pool.get());
        CheckResult(row1, row2, {arrow::boolean(), arrow::int8(), arrow::int16(), arrow::int32()});
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {true, static_cast<int8_t>(10), static_cast<int16_t>(100),
             static_cast<int32_t>(100000)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {true, static_cast<int8_t>(10), static_cast<int16_t>(100),
             static_cast<int32_t>(200000)},
            pool.get());
        CheckResult(row1, row2, {arrow::boolean(), arrow::int8(), arrow::int16(), arrow::int32()});
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {static_cast<int64_t>(9223372036854775800), static_cast<float>(100.1), 123.45},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {static_cast<int64_t>(9223372036854775801), static_cast<float>(100.1), 123.45},
            pool.get());
        CheckResult(row1, row2, {arrow::int64(), arrow::float32(), arrow::float64()});
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {static_cast<int64_t>(9223372036854775800), static_cast<float>(100.1), 123.45},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {static_cast<int64_t>(9223372036854775800), static_cast<float>(100.4), 123.45},
            pool.get());
        CheckResult(row1, row2, {arrow::int64(), arrow::float32(), arrow::float64()});
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {static_cast<int64_t>(9223372036854775800), static_cast<float>(100.1), 1.237E+2},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {static_cast<int64_t>(9223372036854775800), static_cast<float>(100.1), 1.238E+2},
            pool.get());
        CheckResult(row1, row2, {arrow::int64(), arrow::float32(), arrow::float64()});
    }
    {
        auto bytes = std::make_shared<Bytes>("快乐每一天", pool.get());
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {std::string("abandon"), bytes, TimestampType(Timestamp(1725875365442l, 120000), 9)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {std::string("abandon-"), bytes, TimestampType(Timestamp(1725875365442l, 120000), 9)},
            pool.get());
        CheckResult(row1, row2,
                    {arrow::utf8(), arrow::binary(), arrow::timestamp(arrow::TimeUnit::NANO)});
    }
    {
        auto bytes1 = std::make_shared<Bytes>("快乐每一天", pool.get());
        auto bytes2 = std::make_shared<Bytes>("快乐每一天！", pool.get());
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {std::string("abandon"), bytes1, TimestampType(Timestamp(1725875365442l, 120000), 9)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {std::string("abandon"), bytes2, TimestampType(Timestamp(1725875365442l, 120000), 9)},
            pool.get());
        CheckResult(row1, row2,
                    {arrow::utf8(), arrow::binary(), arrow::timestamp(arrow::TimeUnit::NANO)});
    }
    {
        auto bytes = std::make_shared<Bytes>("快乐每一天", pool.get());
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {std::string("abandon"), bytes, TimestampType(Timestamp(1725875365442l, 120000), 9)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {std::string("abandon"), bytes, TimestampType(Timestamp(1725875365442l, 120100), 9)},
            pool.get());
        CheckResult(row1, row2,
                    {arrow::utf8(), arrow::binary(), arrow::timestamp(arrow::TimeUnit::NANO)});
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {Decimal(38, 10, DecimalUtils::StrToInt128("12345678998765432145678").value()),
             static_cast<int32_t>(10)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {Decimal(38, 10, DecimalUtils::StrToInt128("12345678998765432145679").value()),
             static_cast<int32_t>(10)},
            pool.get());
        CheckResult(row1, row2, {arrow::decimal128(38, 10), arrow::date32()});
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {Decimal(38, 10, DecimalUtils::StrToInt128("12345678998765432145678").value()),
             static_cast<int32_t>(10)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {Decimal(38, 10, DecimalUtils::StrToInt128("12345678998765432145678").value()),
             static_cast<int32_t>(20)},
            pool.get());
        CheckResult(row1, row2, {arrow::decimal128(38, 10), arrow::date32()});
    }
}

TEST_F(FieldsComparatorTest, TestTimestampType) {
    auto pool = GetDefaultPool();
    // test ts with different precision
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {TimestampType(Timestamp(1000l, 0), 0), TimestampType(Timestamp(1000l, 0), 3),
             TimestampType(Timestamp(1000l, 1000), 6)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {TimestampType(Timestamp(2000l, 0), 0), TimestampType(Timestamp(1000l, 0), 3),
             TimestampType(Timestamp(1000l, 1000), 6)},
            pool.get());
        CheckResult(
            row1, row2,
            {arrow::timestamp(arrow::TimeUnit::SECOND), arrow::timestamp(arrow::TimeUnit::MILLI),
             arrow::timestamp(arrow::TimeUnit::MICRO)});
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {TimestampType(Timestamp(1000l, 0), 0), TimestampType(Timestamp(1000l, 0), 3),
             TimestampType(Timestamp(1000l, 1000), 6)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {TimestampType(Timestamp(1000l, 0), 0), TimestampType(Timestamp(1001l, 0), 3),
             TimestampType(Timestamp(1000l, 1000), 6)},
            pool.get());
        CheckResult(
            row1, row2,
            {arrow::timestamp(arrow::TimeUnit::SECOND), arrow::timestamp(arrow::TimeUnit::MILLI),
             arrow::timestamp(arrow::TimeUnit::MICRO)});
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {TimestampType(Timestamp(1000l, 0), 0), TimestampType(Timestamp(1000l, 0), 3),
             TimestampType(Timestamp(1000l, 1000), 6)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {TimestampType(Timestamp(1000l, 0), 0), TimestampType(Timestamp(1000l, 0), 3),
             TimestampType(Timestamp(1000l, 2000), 6)},
            pool.get());
        CheckResult(
            row1, row2,
            {arrow::timestamp(arrow::TimeUnit::SECOND), arrow::timestamp(arrow::TimeUnit::MILLI),
             arrow::timestamp(arrow::TimeUnit::MICRO)});
    }
    {
        auto timezone = DateTimeUtils::GetLocalTimezoneName();
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {TimestampType(Timestamp(1000l, 0), 0), TimestampType(Timestamp(1000l, 0), 3),
             TimestampType(Timestamp(1000l, 1000), 6)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {TimestampType(Timestamp(1000l, 0), 0), TimestampType(Timestamp(1000l, 0), 3),
             TimestampType(Timestamp(1000l, 2000), 6)},
            pool.get());
        CheckResult(row1, row2,
                    {arrow::timestamp(arrow::TimeUnit::SECOND, timezone),
                     arrow::timestamp(arrow::TimeUnit::MILLI, timezone),
                     arrow::timestamp(arrow::TimeUnit::MICRO, timezone)});
    }
}
TEST_F(FieldsComparatorTest, TestNull) {
    auto pool = GetDefaultPool();
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow({false, NullType()}, pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow({false, 21}, pool.get());
        CheckResult(row1, row2, {arrow::boolean(), arrow::int32()}, /*has_null=*/true);
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow({NullType(), false}, pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow({NullType(), true}, pool.get());
        CheckResult(row1, row2, {arrow::int32(), arrow::boolean()}, /*has_null=*/true);
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow({true, NullType()}, pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow({false, 21}, pool.get());
        CheckResult(row1, row2, {arrow::boolean(), arrow::int32()}, {1, 0}, /*has_null=*/true);
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow({21, false}, pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow({NullType(), true}, pool.get());
        CheckResult(row1, row2, {arrow::int32(), arrow::boolean()}, {1, 0}, /*has_null=*/true);
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow({false, NullType()}, pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow({true, NullType()}, pool.get());
        CheckResult(row1, row2, {arrow::int32(), arrow::boolean()}, {1, 0}, /*has_null=*/true);
    }
}

TEST_F(FieldsComparatorTest, TestWithSortFields) {
    auto pool = GetDefaultPool();
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {true, static_cast<int8_t>(10), static_cast<int16_t>(200),
             static_cast<int32_t>(100000)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {false, static_cast<int8_t>(5), static_cast<int16_t>(100),
             static_cast<int32_t>(200000)},
            pool.get());
        CheckResult(row1, row2, {arrow::boolean(), arrow::int8(), arrow::int16(), arrow::int32()},
                    {3, 2});
    }
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {true, static_cast<int8_t>(10), static_cast<int16_t>(100),
             static_cast<int32_t>(100000)},
            pool.get());
        BinaryRow row2 = BinaryRowGenerator::GenerateRow(
            {false, static_cast<int8_t>(5), static_cast<int16_t>(200),
             static_cast<int32_t>(100000)},
            pool.get());
        CheckResult(row1, row2, {arrow::boolean(), arrow::int8(), arrow::int16(), arrow::int32()},
                    {3, 2});
    }
}

TEST_F(FieldsComparatorTest, TestInvalidType) {
    auto map_type = arrow::map(arrow::int8(), arrow::int16());
    ASSERT_NOK_WITH_MSG(FieldsComparator::Create({DataField(0, arrow::field("f0", arrow::int32())),
                                                  DataField(1, arrow::field("f1", map_type))},
                                                 /*is_ascending_order=*/true,
                                                 /*use_view=*/false),
                        "Do not support comparing map<int8, int16> type in idx 1");
}

}  // namespace paimon::test
