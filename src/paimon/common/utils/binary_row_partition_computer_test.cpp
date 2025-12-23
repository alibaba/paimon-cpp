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

#include "paimon/common/utils/binary_row_partition_computer.h"

#include <cstdint>
#include <limits>
#include <string>
#include <variant>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(BinaryRowPartitionComputerTest, TestToAndFromBinaryRow) {
    auto pool = GetDefaultPool();
    arrow::FieldVector fields = {arrow::field("f0", arrow::boolean()),
                                 arrow::field("f1", arrow::int8()),
                                 arrow::field("f2", arrow::int8()),
                                 arrow::field("f3", arrow::int16()),
                                 arrow::field("f4", arrow::int16()),
                                 arrow::field("f5", arrow::int32()),
                                 arrow::field("f6", arrow::int32()),
                                 arrow::field("f7", arrow::int64()),
                                 arrow::field("f8", arrow::int64()),
                                 arrow::field("f9", arrow::float32()),
                                 arrow::field("f10", arrow::float64()),
                                 arrow::field("f11", arrow::utf8()),
                                 arrow::field("f12", arrow::utf8()),
                                 arrow::field("f13", arrow::date32()),
                                 arrow::field("non-partition-field", arrow::int32())};

    auto schema = arrow::schema(fields);
    std::vector<std::string> partition_keys = {"f0", "f2", "f1", "f3",  "f4",  "f5",  "f6",
                                               "f7", "f8", "f9", "f10", "f11", "f12", "f13"};
    {
        // simple case with legacy_partition_name_enabled = true
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<BinaryRowPartitionComputer> computer,
            BinaryRowPartitionComputer::Create(partition_keys, schema, "__DEFAULT_PARTITION__",
                                               /*legacy_partition_name_enabled=*/true, pool));
        std::map<std::string, std::string> partition_map = {
            {"f0", "true"},
            {"f1", "10"},
            {"f2", "-20"},
            {"f3", "1556"},
            {"f4", "-2556"},
            {"f5", "348489"},
            {"f6", "-448489"},
            {"f7", "-9223372036854775808"},
            {"f8", "182737474"},
            {"f9", "0.334"},
            {"f10", "467.66472"},
            {"f11", "abcde"},
            {"f12", "这是一个很长很长的中文"},
            {"f13", "5"},
        };
        ASSERT_OK_AND_ASSIGN(BinaryRow row, computer->ToBinaryRow(partition_map));
        ASSERT_EQ(14, row.GetFieldCount());
        ASSERT_EQ(true, row.GetBoolean(0));
        ASSERT_EQ(-20, row.GetByte(1));
        ASSERT_EQ(10, row.GetByte(2));
        ASSERT_EQ(1556, row.GetShort(3));
        ASSERT_EQ(-2556, row.GetShort(4));
        ASSERT_EQ(348489, row.GetInt(5));
        ASSERT_EQ(-448489, row.GetInt(6));
        ASSERT_EQ(std::numeric_limits<int64_t>::min(), row.GetLong(7));
        ASSERT_EQ(182737474l, row.GetLong(8));
        ASSERT_NEAR(0.334, row.GetFloat(9), 0.0000001);
        ASSERT_NEAR(467.66472, row.GetDouble(10), 0.0000001);
        ASSERT_EQ("abcde", row.GetString(11).ToString());
        ASSERT_EQ("这是一个很长很长的中文", row.GetString(12).ToString());
        ASSERT_EQ(5, row.GetDate(13));

        std::vector<std::pair<std::string, std::string>> part_values;
        ASSERT_OK_AND_ASSIGN(part_values, computer->GeneratePartitionVector(row));
        ASSERT_EQ(14, part_values.size());
        std::map<std::string, std::string> actual_part_values_map;
        for (const auto& [key, value] : part_values) {
            actual_part_values_map[key] = value;
        }
        ASSERT_EQ(actual_part_values_map, partition_map);
    }
    {
        // simple case with legacy_partition_name_enabled = false
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<BinaryRowPartitionComputer> computer,
            BinaryRowPartitionComputer::Create(partition_keys, schema, "__DEFAULT_PARTITION__",
                                               /*legacy_partition_name_enabled=*/false, pool));
        std::map<std::string, std::string> partition_map = {
            {"f0", "true"},
            {"f1", "10"},
            {"f2", "-20"},
            {"f3", "1556"},
            {"f4", "-2556"},
            {"f5", "348489"},
            {"f6", "-448489"},
            {"f7", "-9223372036854775808"},
            {"f8", "182737474"},
            {"f9", "0.334"},
            {"f10", "467.66472"},
            {"f11", "abcde"},
            {"f12", "这是一个很长很长的中文"},
            {"f13", "1970-01-06"},
        };
        ASSERT_OK_AND_ASSIGN(BinaryRow row, computer->ToBinaryRow(partition_map));
        ASSERT_EQ(14, row.GetFieldCount());
        ASSERT_EQ(true, row.GetBoolean(0));
        ASSERT_EQ(-20, row.GetByte(1));
        ASSERT_EQ(10, row.GetByte(2));
        ASSERT_EQ(1556, row.GetShort(3));
        ASSERT_EQ(-2556, row.GetShort(4));
        ASSERT_EQ(348489, row.GetInt(5));
        ASSERT_EQ(-448489, row.GetInt(6));
        ASSERT_EQ(std::numeric_limits<int64_t>::min(), row.GetLong(7));
        ASSERT_EQ(182737474l, row.GetLong(8));
        ASSERT_NEAR(0.334, row.GetFloat(9), 0.0000001);
        ASSERT_NEAR(467.66472, row.GetDouble(10), 0.0000001);
        ASSERT_EQ("abcde", row.GetString(11).ToString());
        ASSERT_EQ("这是一个很长很长的中文", row.GetString(12).ToString());
        ASSERT_EQ(5, row.GetDate(13));

        std::vector<std::pair<std::string, std::string>> part_values;
        ASSERT_OK_AND_ASSIGN(part_values, computer->GeneratePartitionVector(row));
        ASSERT_EQ(14, part_values.size());
        std::map<std::string, std::string> actual_part_values_map;
        for (const auto& [key, value] : part_values) {
            actual_part_values_map[key] = value;
        }
        ASSERT_EQ(actual_part_values_map, partition_map);
    }
    {
        // simple case with default partition value
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<BinaryRowPartitionComputer> computer,
            BinaryRowPartitionComputer::Create(partition_keys, schema, "__DEFAULT_PARTITION__",
                                               /*legacy_partition_name_enabled=*/true, pool));
        std::map<std::string, std::string> partition_map = {
            {"f0", "true"},
            {"f1", "10"},
            {"f2", "-20"},
            {"f3", "1556"},
            {"f4", "-2556"},
            {"f5", "348489"},
            {"f6", "-448489"},
            {"f7", "-9223372036854775808"},
            {"f8", "182737474"},
            {"f9", "0.334"},
            {"f10", "467.66472"},
            {"f11", " "},
            {"f12", "__DEFAULT_PARTITION__"},
            {"f13", "5"},
        };
        ASSERT_OK_AND_ASSIGN(BinaryRow row, computer->ToBinaryRow(partition_map));
        ASSERT_EQ(14, row.GetFieldCount());
        ASSERT_EQ(true, row.GetBoolean(0));
        ASSERT_EQ(-20, row.GetByte(1));
        ASSERT_EQ(10, row.GetByte(2));
        ASSERT_EQ(1556, row.GetShort(3));
        ASSERT_EQ(-2556, row.GetShort(4));
        ASSERT_EQ(348489, row.GetInt(5));
        ASSERT_EQ(-448489, row.GetInt(6));
        ASSERT_EQ(std::numeric_limits<int64_t>::min(), row.GetLong(7));
        ASSERT_EQ(182737474l, row.GetLong(8));
        ASSERT_NEAR(0.334, row.GetFloat(9), 0.0000001);
        ASSERT_NEAR(467.66472, row.GetDouble(10), 0.0000001);
        ASSERT_EQ(" ", row.GetString(11).ToString());
        ASSERT_TRUE(row.IsNullAt(12));
        ASSERT_EQ(5, row.GetInt(13));

        std::vector<std::pair<std::string, std::string>> part_values;
        ASSERT_OK_AND_ASSIGN(part_values, computer->GeneratePartitionVector(row));
        ASSERT_EQ(14, part_values.size());
        std::map<std::string, std::string> actual_part_values_map;
        for (const auto& [key, value] : part_values) {
            actual_part_values_map[key] = value;
        }
        std::map<std::string, std::string> expected_map = {
            {"f0", "true"},
            {"f1", "10"},
            {"f2", "-20"},
            {"f3", "1556"},
            {"f4", "-2556"},
            {"f5", "348489"},
            {"f6", "-448489"},
            {"f7", "-9223372036854775808"},
            {"f8", "182737474"},
            {"f9", "0.334"},
            {"f10", "467.66472"},
            {"f11", "__DEFAULT_PARTITION__"},
            {"f12", "__DEFAULT_PARTITION__"},
            {"f13", "5"},
        };
        ASSERT_EQ(actual_part_values_map, expected_map);
    }
    {
        // test partition_str does not contain all partition keys, f4
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<BinaryRowPartitionComputer> computer,
            BinaryRowPartitionComputer::Create(partition_keys, schema, "__DEFAULT_PARTITION__",
                                               /*legacy_partition_name_enabled=*/true, pool));
        std::map<std::string, std::string> partition_map = {{"f0", "true"},
                                                            {"f1", "10"},
                                                            {"f2", "-20"},
                                                            {"f3", "1556"},
                                                            {"f5", "348489"},
                                                            {"f6", "-448489"},
                                                            {"f7", "-9223372036854775808"},
                                                            {"f8", "182737474"},
                                                            {"f9", "0.334"},
                                                            {"f10", "467.66472"},
                                                            {"f11", "abcde"},
                                                            {"f12", "这是一个很长很长的中文"}};

        ASSERT_NOK_WITH_MSG(computer->ToBinaryRow(partition_map),
                            "can not find partition key 'f4' in input partition");
    }
    {
        // test partition_str mismatches schema, f6="abcd"
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<BinaryRowPartitionComputer> computer,
            BinaryRowPartitionComputer::Create(partition_keys, schema, "__DEFAULT_PARTITION__",
                                               /*legacy_partition_name_enabled=*/true, pool));

        std::map<std::string, std::string> partition_map = {{"f0", "true"},
                                                            {"f1", "10"},
                                                            {"f2", "-20"},
                                                            {"f3", "1556"},
                                                            {"f4", "-2556"},
                                                            {"f5", "348489"},
                                                            {"f6", "abcd"},
                                                            {"f7", "-9223372036854775808"},
                                                            {"f8", "182737474"},
                                                            {"f9", "0.334"},
                                                            {"f10", "467.66472"},
                                                            {"f11", "abcde"},
                                                            {"f12", "这是一个很长很长的中文"}};
        ASSERT_NOK_WITH_MSG(computer->ToBinaryRow(partition_map),
                            "cannot convert field idx 6, field value abcd to type INT32");
    }
}

TEST(BinaryRowPartitionComputerTest, TestNullOrWhitespaceOnlyStr) {
    auto pool = GetDefaultPool();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::utf8()),
    };

    auto schema = arrow::schema(fields);
    std::vector<std::string> partition_keys = {"f0", "f1", "f2"};
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<BinaryRowPartitionComputer> computer,
        BinaryRowPartitionComputer::Create(partition_keys, schema, "__DEFAULT_PARTITION__",
                                           /*legacy_partition_name_enabled=*/true, pool));

    ASSERT_OK_AND_ASSIGN(auto partition_key_values,
                         computer->GeneratePartitionVector(BinaryRowGenerator::GenerateRow(
                             {std::string(" "), std::string(""), std::string("ab ")}, pool.get())));
    std::vector<std::pair<std::string, std::string>> expected = {
        {"f0", "__DEFAULT_PARTITION__"}, {"f1", "__DEFAULT_PARTITION__"}, {"f2", "ab "}};
    ASSERT_EQ(partition_key_values, expected);
}
}  // namespace paimon::test
