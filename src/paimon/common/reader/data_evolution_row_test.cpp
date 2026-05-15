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

#include "paimon/common/reader/data_evolution_row.h"

#include <cstdint>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_map.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(DataEvolutionRowTest, TestSimple) {
    // f0:int32
    // f1:int32
    // f2:string
    // f3:int32
    // f4:string
    // f5:int32
    std::vector<int32_t> row_offsets = {0, 2, 0, 1, 2, 1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0};

    auto pool = GetDefaultPool();
    BinaryRow row1 = BinaryRowGenerator::GenerateRow({0, std::string("00")}, pool.get());
    BinaryRow row2 = BinaryRowGenerator::GenerateRow({10, 110}, pool.get());
    BinaryRow row3 = BinaryRowGenerator::GenerateRow({20, std::string("20")}, pool.get());

    DataEvolutionRow row({row1, row2, row3}, row_offsets, field_offsets);
    ASSERT_EQ(row.GetFieldCount(), 6);
    ASSERT_FALSE(row.IsNullAt(0));
    ASSERT_EQ(row.GetInt(0), 0);
    ASSERT_EQ(row.GetInt(1), 20);
    ASSERT_EQ(row.GetString(2).ToString(), "00");
    ASSERT_EQ(row.GetInt(3), 110);
    ASSERT_EQ(row.GetString(4).ToString(), "20");
    ASSERT_EQ(row.GetInt(5), 10);

    // test set and get row kind
    ASSERT_OK_AND_ASSIGN(const RowKind* row_kind, row.GetRowKind());
    ASSERT_EQ(*row_kind, *RowKind::Insert());
    row.SetRowKind(RowKind::UpdateAfter());
    ASSERT_OK_AND_ASSIGN(const RowKind* new_row_kind, row.GetRowKind());
    ASSERT_EQ(*new_row_kind, *RowKind::UpdateAfter());
    ASSERT_EQ(row.ToString(), "DataEvolutionRow");
}

TEST(DataEvolutionRowTest, TestNull) {
    // f0:int32
    // f1:int32
    // f2:string
    // f3:int32
    // f4:string
    // f5:int32
    // f6:non-exist
    std::vector<int32_t> row_offsets = {0, 2, 0, 1, 2, 1, -1, -2};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0, -1, -1};

    auto pool = GetDefaultPool();
    BinaryRow row1 = BinaryRowGenerator::GenerateRow({0, std::string("00")}, pool.get());
    BinaryRow row2 = BinaryRowGenerator::GenerateRow({10, 110}, pool.get());
    BinaryRow row3 = BinaryRowGenerator::GenerateRow({20, NullType()}, pool.get());

    DataEvolutionRow row({row1, row2, row3}, row_offsets, field_offsets);
    ASSERT_EQ(row.GetFieldCount(), 8);
    ASSERT_FALSE(row.IsNullAt(0));
    ASSERT_EQ(row.GetInt(0), 0);
    ASSERT_EQ(row.GetInt(1), 20);
    ASSERT_EQ(row.GetString(2).ToString(), "00");
    ASSERT_EQ(row.GetInt(3), 110);
    ASSERT_TRUE(row.IsNullAt(4));
    ASSERT_EQ(row.GetInt(5), 10);
    ASSERT_TRUE(row.IsNullAt(6));
    ASSERT_TRUE(row.IsNullAt(7));
}

TEST(DataEvolutionRowTest, TestAllFieldTypes) {
    auto pool = GetDefaultPool();

    // Row0: boolean(true), byte(42), short(1024), int(100000), date(19000)
    BinaryRow row0 =
        BinaryRowGenerator::GenerateRow({true, static_cast<int8_t>(42), static_cast<int16_t>(1024),
                                         static_cast<int32_t>(100000), static_cast<int32_t>(19000)},
                                        pool.get());

    // Row1: long(-987654321), float(3.14f), double(-9.87654321)
    BinaryRow row1 = BinaryRowGenerator::GenerateRow(
        {static_cast<int64_t>(-987654321LL), static_cast<float>(3.14f),
         static_cast<double>(-9.87654321)},
        pool.get());

    // Row2: string("hello"), string("world")
    BinaryRow row2 =
        BinaryRowGenerator::GenerateRow({std::string("hello"), std::string("world")}, pool.get());

    // Row3: decimal(10,2,67890), timestamp(1000,500) with precision 9
    BinaryRow row3 = BinaryRowGenerator::GenerateRow(
        {Decimal(10, 2, 67890), TimestampType(Timestamp(1000, 500), 9)}, pool.get());

    // Row4: binary("fghij")
    auto bytes_val = std::make_shared<Bytes>("fghij", pool.get());
    BinaryRow row4 = BinaryRowGenerator::GenerateRow({bytes_val}, pool.get());

    // Row5: nested array [10,20], nested map {1:100, 2:200}, nested row ("Alice", 30)
    // Built manually via BinaryRowWriter since BinaryRowGenerator doesn't support nested types
    BinaryRow row5(3);
    {
        BinaryRowWriter writer(&row5, 0, pool.get());

        // field 0: array [10, 20]
        auto inner_array = BinaryArray::FromIntArray({10, 20}, pool.get());
        writer.WriteArray(0, inner_array);

        // field 1: map {1->100, 2->200}
        auto map_key = BinaryArray::FromIntArray({1, 2}, pool.get());
        auto map_val = BinaryArray::FromLongArray({100LL, 200LL}, pool.get());
        auto map_obj = BinaryMap::ValueOf(map_key, map_val, pool.get());
        writer.WriteMap(1, *map_obj);

        // field 2: row ("Alice", 30)
        BinaryRow inner_row = BinaryRowGenerator::GenerateRow(
            {std::string("Alice"), static_cast<int32_t>(30)}, pool.get());
        writer.WriteRow(2, inner_row);

        writer.Complete();
    }

    // Build the DataEvolutionRow that picks specific fields from each row.
    // evolution field -> (row_index, field_index_within_row)
    // pos 0:  boolean   -> row0[0]  = true
    // pos 1:  byte      -> row0[1]  = 42
    // pos 2:  short     -> row0[2]  = 1024
    // pos 3:  int       -> row0[3]  = 100000
    // pos 4:  date      -> row0[4]  = 19000
    // pos 5:  long      -> row1[0]  = -987654321
    // pos 6:  float     -> row1[1]  = 3.14f
    // pos 7:  double    -> row1[2]  = -9.87654321
    // pos 8:  string    -> row2[0]  = "hello"
    // pos 9:  stringview-> row2[1]  = "world"
    // pos 10: decimal   -> row3[0]  = Decimal(10,2,67890)
    // pos 11: timestamp -> row3[1]  = Timestamp(1000,500)
    // pos 12: binary    -> row4[0]  = "fghij"
    // pos 13: array     -> row5[0]  = [10,20]
    // pos 14: map       -> row5[1]  = {1:100, 2:200}
    // pos 15: row       -> row5[2]  = ("Alice", 30)
    std::vector<int32_t> row_offsets = {0, 0, 0, 0, 0, 1, 1, 1, 2, 2, 3, 3, 4, 5, 5, 5};
    std::vector<int32_t> field_offsets = {0, 1, 2, 3, 4, 0, 1, 2, 0, 1, 0, 1, 0, 0, 1, 2};

    DataEvolutionRow evolution({row0, row1, row2, row3, row4, row5}, row_offsets, field_offsets);

    ASSERT_EQ(evolution.GetFieldCount(), 16);

    // Boolean
    ASSERT_FALSE(evolution.IsNullAt(0));
    ASSERT_EQ(evolution.GetBoolean(0), true);

    // Byte
    ASSERT_EQ(evolution.GetByte(1), 42);

    // Short
    ASSERT_EQ(evolution.GetShort(2), 1024);

    // Int
    ASSERT_EQ(evolution.GetInt(3), 100000);

    // Date
    ASSERT_EQ(evolution.GetDate(4), 19000);

    // Long
    ASSERT_EQ(evolution.GetLong(5), -987654321LL);

    // Float
    ASSERT_FLOAT_EQ(evolution.GetFloat(6), 3.14f);

    // Double
    ASSERT_DOUBLE_EQ(evolution.GetDouble(7), -9.87654321);

    // String
    ASSERT_EQ(evolution.GetString(8).ToString(), "hello");

    // StringView
    ASSERT_EQ(evolution.GetStringView(9), "world");

    // Decimal
    ASSERT_EQ(evolution.GetDecimal(10, 10, 2), Decimal(10, 2, 67890));

    // Timestamp
    ASSERT_EQ(evolution.GetTimestamp(11, 9), Timestamp(1000, 500));

    // Binary
    auto retrieved_binary = evolution.GetBinary(12);
    Bytes expected_binary("fghij", pool.get());
    ASSERT_EQ(*retrieved_binary, expected_binary);

    // Nested Array
    auto retrieved_array = evolution.GetArray(13);
    ASSERT_OK_AND_ASSIGN(auto inner_values, retrieved_array->ToIntArray());
    ASSERT_EQ(inner_values, std::vector<int32_t>({10, 20}));

    // Map
    auto retrieved_map = evolution.GetMap(14);
    ASSERT_EQ(retrieved_map->Size(), 2);

    // Nested Row
    auto retrieved_row = evolution.GetRow(15, 2);
    ASSERT_EQ(retrieved_row->GetString(0).ToString(), "Alice");
    ASSERT_EQ(retrieved_row->GetInt(1), 30);

    // ToString
    ASSERT_EQ(evolution.ToString(), "DataEvolutionRow");
}

}  // namespace paimon::test
