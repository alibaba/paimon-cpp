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

#include "paimon/common/reader/data_evolution_array.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_map.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(DataEvolutionArrayTest, TestSimple) {
    auto pool = GetDefaultPool();

    std::vector<int32_t> array_offsets = {0, 2, 0, 1, 2, 1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0};

    std::vector<int64_t> src_array1 = {1, -100};
    auto array1 = BinaryRowGenerator::FromLongArrayWithNull(src_array1, pool.get());

    std::vector<int64_t> src_array2 = {2, -2};
    auto array2 = BinaryRowGenerator::FromLongArrayWithNull(src_array2, pool.get());

    std::vector<int64_t> src_array3 = {3, -3};
    auto array3 = BinaryRowGenerator::FromLongArrayWithNull(src_array3, pool.get());

    DataEvolutionArray data_evolution_array(std::vector<BinaryArray>({array1, array2, array3}),
                                            array_offsets, field_offsets);

    ASSERT_FALSE(data_evolution_array.IsNullAt(0));

    ASSERT_EQ(data_evolution_array.GetLong(0), 1);
    ASSERT_EQ(data_evolution_array.GetLong(1), 3);
    ASSERT_EQ(data_evolution_array.GetLong(2), -100);
    ASSERT_EQ(data_evolution_array.GetLong(3), -2);
    ASSERT_EQ(data_evolution_array.GetLong(4), -3);
    ASSERT_EQ(data_evolution_array.GetLong(5), 2);

    ASSERT_OK_AND_ASSIGN(auto ret, data_evolution_array.ToLongArray());
    ASSERT_EQ(ret, std::vector<int64_t>({1, 3, -100, -2, -3, 2}));

    ASSERT_NOK_WITH_MSG(data_evolution_array.ToBooleanArray(),
                        "DataEvolutionArray not support ToBooleanArray");
}

TEST(DataEvolutionArrayTest, TestNullValue) {
    auto pool = GetDefaultPool();

    std::vector<int32_t> array_offsets = {-2, -1, 0, 0};
    std::vector<int32_t> field_offsets = {-1, -1, 0, 1};

    std::vector<int64_t> src_array1 = {1, -100};
    auto array1 = BinaryRowGenerator::FromLongArrayWithNull(src_array1, pool.get());

    DataEvolutionArray data_evolution_array(std::vector<BinaryArray>({array1}), array_offsets,
                                            field_offsets);

    ASSERT_TRUE(data_evolution_array.IsNullAt(0));
    ASSERT_TRUE(data_evolution_array.IsNullAt(1));
    ASSERT_EQ(data_evolution_array.GetLong(2), 1);
    ASSERT_EQ(data_evolution_array.GetLong(3), -100);
}

TEST(DataEvolutionArrayTest, TestAllFieldTypes) {
    auto pool = GetDefaultPool();

    // We create separate BinaryArrays for each field type and use DataEvolutionArray
    // to proxy access across them. Each array has 2 elements; the evolution array
    // picks one element from each underlying array via array_offsets/field_offsets.

    // --- Boolean array (array index 0) ---
    BinaryArray bool_array;
    {
        BinaryArrayWriter writer(&bool_array, 2, sizeof(bool), pool.get());
        writer.WriteBoolean(0, true);
        writer.WriteBoolean(1, false);
        writer.Complete();
    }

    // --- Byte array (array index 1) ---
    BinaryArray byte_array;
    {
        BinaryArrayWriter writer(&byte_array, 2, sizeof(int8_t), pool.get());
        writer.WriteByte(0, 42);
        writer.WriteByte(1, -7);
        writer.Complete();
    }

    // --- Short array (array index 2) ---
    BinaryArray short_array;
    {
        BinaryArrayWriter writer(&short_array, 2, sizeof(int16_t), pool.get());
        writer.WriteShort(0, 1024);
        writer.WriteShort(1, -512);
        writer.Complete();
    }

    // --- Int array (array index 3) ---
    BinaryArray int_array;
    {
        BinaryArrayWriter writer(&int_array, 2, sizeof(int32_t), pool.get());
        writer.WriteInt(0, 100000);
        writer.WriteInt(1, -99999);
        writer.Complete();
    }

    // --- Long array (array index 4) ---
    BinaryArray long_array;
    {
        BinaryArrayWriter writer(&long_array, 2, sizeof(int64_t), pool.get());
        writer.WriteLong(0, 1234567890LL);
        writer.WriteLong(1, -987654321LL);
        writer.Complete();
    }

    // --- Float array (array index 5) ---
    BinaryArray float_array;
    {
        BinaryArrayWriter writer(&float_array, 2, sizeof(float), pool.get());
        writer.WriteFloat(0, 3.14f);
        writer.WriteFloat(1, -2.71f);
        writer.Complete();
    }

    // --- Double array (array index 6) ---
    BinaryArray double_array;
    {
        BinaryArrayWriter writer(&double_array, 2, sizeof(double), pool.get());
        writer.WriteDouble(0, 1.23456789);
        writer.WriteDouble(1, -9.87654321);
        writer.Complete();
    }

    // --- String array (array index 7) ---
    BinaryArray string_array;
    {
        BinaryArrayWriter writer(&string_array, 2, 8, pool.get());
        writer.WriteString(0, BinaryString::FromString("hello", pool.get()));
        writer.WriteString(1, BinaryString::FromString("world", pool.get()));
        writer.Complete();
    }

    // --- Decimal array (array index 8), precision=10, scale=2 ---
    BinaryArray decimal_array;
    {
        BinaryArrayWriter writer(&decimal_array, 2, 8, pool.get());
        writer.WriteDecimal(0, Decimal(10, 2, 12345), 10);
        writer.WriteDecimal(1, Decimal(10, 2, 67890), 10);
        writer.Complete();
    }

    // --- Timestamp array (array index 9), precision=9 (non-compact) ---
    BinaryArray timestamp_array;
    {
        BinaryArrayWriter writer(&timestamp_array, 2, 8, pool.get());
        writer.WriteTimestamp(0, Timestamp(1000, 500), 9);
        writer.WriteTimestamp(1, Timestamp(2000, 999), 9);
        writer.Complete();
    }

    // --- Binary/Bytes array (array index 10) ---
    BinaryArray binary_array;
    {
        Bytes bytes_val1("abcde", pool.get());
        Bytes bytes_val2("fghij", pool.get());
        BinaryArrayWriter writer(&binary_array, 2, 8, pool.get());
        writer.WriteBinary(0, bytes_val1);
        writer.WriteBinary(1, bytes_val2);
        writer.Complete();
    }

    // --- Nested Array array (array index 11) ---
    BinaryArray nested_array;
    {
        auto inner = BinaryArray::FromIntArray({10, 20}, pool.get());
        auto inner2 = BinaryArray::FromIntArray({30, 40}, pool.get());
        BinaryArrayWriter writer(&nested_array, 2, 8, pool.get());
        writer.WriteArray(0, inner);
        writer.WriteArray(1, inner2);
        writer.Complete();
    }

    // --- Map array (array index 12) ---
    BinaryArray map_array;
    {
        auto map_key = BinaryArray::FromIntArray({1, 2}, pool.get());
        auto map_val = BinaryArray::FromLongArray({100LL, 200LL}, pool.get());
        auto map_obj = BinaryMap::ValueOf(map_key, map_val, pool.get());
        BinaryArrayWriter writer(&map_array, 1, 8, pool.get());
        writer.WriteMap(0, *map_obj);
        writer.Complete();
    }

    // --- Row array (array index 13) ---
    BinaryArray row_array;
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {std::string("Alice"), static_cast<int32_t>(30)}, pool.get());
        BinaryArrayWriter writer(&row_array, 1, 8, pool.get());
        writer.WriteRow(0, row1);
        writer.Complete();
    }

    // --- Date array (array index 14), stored as int32 ---
    BinaryArray date_array;
    {
        BinaryArrayWriter writer(&date_array, 2, sizeof(int32_t), pool.get());
        writer.WriteInt(0, 19000);  // days since epoch
        writer.WriteInt(1, 18500);
        writer.Complete();
    }

    std::vector<BinaryArray> arrays = {
        bool_array,       // 0
        byte_array,       // 1
        short_array,      // 2
        int_array,        // 3
        long_array,       // 4
        float_array,      // 5
        double_array,     // 6
        string_array,     // 7
        decimal_array,    // 8
        timestamp_array,  // 9
        binary_array,     // 10
        nested_array,     // 11
        map_array,        // 12
        row_array,        // 13
        date_array,       // 14
    };

    // Each evolution entry: (array_index, field_index_within_array)
    // pos 0:  boolean   -> arrays[0][0]  = true
    // pos 1:  byte      -> arrays[1][0]  = 42
    // pos 2:  short     -> arrays[2][1]  = -512
    // pos 3:  int       -> arrays[3][0]  = 100000
    // pos 4:  long      -> arrays[4][1]  = -987654321
    // pos 5:  float     -> arrays[5][0]  = 3.14f
    // pos 6:  double    -> arrays[6][1]  = -9.87654321
    // pos 7:  string    -> arrays[7][0]  = "hello"
    // pos 8:  decimal   -> arrays[8][1]  = Decimal(10,2,67890)
    // pos 9:  timestamp -> arrays[9][0]  = Timestamp(1000,500)
    // pos 10: binary    -> arrays[10][1] = "fghij"
    // pos 11: array     -> arrays[11][0] = [10,20]
    // pos 12: map       -> arrays[12][0]
    // pos 13: row       -> arrays[13][0] = ("Alice", 30)
    // pos 14: date      -> arrays[14][0] = 19000
    // pos 15: string    -> arrays[7][1]  = "world"
    std::vector<int32_t> array_offsets = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 7};
    std::vector<int32_t> field_offsets = {0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 1};

    DataEvolutionArray evolution(arrays, array_offsets, field_offsets);

    ASSERT_EQ(evolution.Size(), 16);

    // Boolean
    ASSERT_FALSE(evolution.IsNullAt(0));
    ASSERT_EQ(evolution.GetBoolean(0), true);

    // Byte
    ASSERT_EQ(evolution.GetByte(1), 42);

    // Short
    ASSERT_EQ(evolution.GetShort(2), -512);

    // Int
    ASSERT_EQ(evolution.GetInt(3), 100000);

    // Long
    ASSERT_EQ(evolution.GetLong(4), -987654321LL);

    // Float
    ASSERT_FLOAT_EQ(evolution.GetFloat(5), 3.14f);

    // Double
    ASSERT_DOUBLE_EQ(evolution.GetDouble(6), -9.87654321);

    // String
    ASSERT_EQ(evolution.GetString(7).ToString(), "hello");

    // StringView
    ASSERT_EQ(evolution.GetStringView(15), "world");

    // Decimal
    ASSERT_EQ(evolution.GetDecimal(8, 10, 2), Decimal(10, 2, 67890));

    // Timestamp
    ASSERT_EQ(evolution.GetTimestamp(9, 9), Timestamp(1000, 500));

    // Binary
    auto retrieved_binary = evolution.GetBinary(10);
    Bytes expected_binary("fghij", pool.get());
    ASSERT_EQ(*retrieved_binary, expected_binary);

    // Nested Array
    auto retrieved_array = evolution.GetArray(11);
    ASSERT_OK_AND_ASSIGN(auto inner_values, retrieved_array->ToIntArray());
    ASSERT_EQ(inner_values, std::vector<int32_t>({10, 20}));

    // Map
    auto retrieved_map = evolution.GetMap(12);
    ASSERT_EQ(retrieved_map->Size(), 2);

    // Row
    auto retrieved_row = evolution.GetRow(13, 2);
    ASSERT_EQ(retrieved_row->GetString(0).ToString(), "Alice");
    ASSERT_EQ(retrieved_row->GetInt(1), 30);

    // Date
    ASSERT_EQ(evolution.GetDate(14), 19000);

    // Verify unsupported ToXxxArray methods
    ASSERT_NOK_WITH_MSG(evolution.ToBooleanArray(),
                        "DataEvolutionArray not support ToBooleanArray");
    ASSERT_NOK_WITH_MSG(evolution.ToByteArray(), "DataEvolutionArray not support ToByteArray");
    ASSERT_NOK_WITH_MSG(evolution.ToShortArray(), "DataEvolutionArray not support ToShortArray");
    ASSERT_NOK_WITH_MSG(evolution.ToIntArray(), "DataEvolutionArray not support ToIntArray");
    ASSERT_NOK_WITH_MSG(evolution.ToFloatArray(), "DataEvolutionArray not support ToFloatArray");
    ASSERT_NOK_WITH_MSG(evolution.ToDoubleArray(), "DataEvolutionArray not support ToDoubleArray");
}

}  // namespace paimon::test
