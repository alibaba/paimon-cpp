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

#include "paimon/common/data/binary_array.h"

#include <cstdlib>
#include <optional>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/util/checked_cast.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_map.h"
#include "paimon/common/data/columnar/columnar_array.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {

TEST(BinaryArrayTest, TestBinaryArraySimple) {
    auto pool = GetDefaultPool();
    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);
    std::vector<int64_t> values(100, 0);
    for (auto& value : values) {
        value = paimon::test::RandomNumber(0, 10000000);
    }
    auto binary_array = BinaryArray::FromLongArray(values, pool.get());
    ASSERT_EQ(values.size(), binary_array.Size()) << "seed:" << seed;
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], binary_array.GetLong(i)) << "seed:" << seed << ", idx: " << i;
    }

    ASSERT_FALSE(binary_array.IsNullAt(40)) << "seed:" << seed;
    ASSERT_FALSE(binary_array.IsNullAt(70)) << "seed:" << seed;
    auto res = binary_array.ToLongArray();
    ASSERT_EQ(res.value(), values);
}

TEST(BinaryArrayTest, TestSetAndGet) {
    auto pool = GetDefaultPool();
    {
        std::vector<bool> arr = {true, false};
        BinaryArray array;
        BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), sizeof(bool), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteBoolean(i, arr[i]);
        }
        writer.Complete();
        ASSERT_EQ(true, array.GetBoolean(0));
        ASSERT_EQ(false, array.GetBoolean(1));
        ASSERT_OK_AND_ASSIGN(auto res, array.ToBooleanArray());
        std::vector<char> char_arr = {1, 0};
        ASSERT_EQ(res, char_arr);
    }
    {
        std::vector<int8_t> arr = {1, 2, 3, 4, 5};
        BinaryArray array;
        BinaryArrayWriter writer =
            BinaryArrayWriter(&array, arr.size(), sizeof(int8_t), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteByte(i, arr[i]);
        }
        writer.Complete();
        ASSERT_EQ(1, array.GetByte(0));
        ASSERT_EQ(5, array.GetByte(4));
        ASSERT_OK_AND_ASSIGN(auto res, array.ToByteArray());
        std::vector<char> char_arr = {1, 2, 3, 4, 5};
        ASSERT_EQ(res, char_arr);
    }
    {
        std::vector<int16_t> arr = {1, 2, 3, 4, 5};
        BinaryArray array;
        BinaryArrayWriter writer =
            BinaryArrayWriter(&array, arr.size(), sizeof(int16_t), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteShort(i, arr[i]);
        }
        writer.Complete();
        ASSERT_EQ(1, array.GetShort(0));
        ASSERT_EQ(5, array.GetShort(4));
        ASSERT_OK_AND_ASSIGN(auto res, array.ToShortArray());
        ASSERT_EQ(res, arr);
    }
    {
        std::vector<int32_t> arr = {1, 2, 3, 4, 5};
        BinaryArray array;
        BinaryArrayWriter writer =
            BinaryArrayWriter(&array, arr.size(), sizeof(int32_t), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteInt(i, arr[i]);
        }
        writer.Complete();
        ASSERT_EQ(1, array.GetInt(0));
        ASSERT_EQ(5, array.GetInt(4));
        ASSERT_OK_AND_ASSIGN(auto res, array.ToIntArray());
        ASSERT_EQ(res, arr);
    }
    {
        // test date
        std::vector<int32_t> arr = {10000, 20006};
        BinaryArray array;
        BinaryArrayWriter writer =
            BinaryArrayWriter(&array, arr.size(), sizeof(int32_t), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteInt(i, arr[i]);
        }
        writer.Complete();
        ASSERT_EQ(10000, array.GetDate(0));
        ASSERT_EQ(20006, array.GetDate(1));
        ASSERT_OK_AND_ASSIGN(auto res, array.ToIntArray());
        ASSERT_EQ(res, arr);
    }
    {
        std::vector<float> arr = {1.0, 2.0};
        BinaryArray array;
        BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), sizeof(float), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteFloat(i, arr[i]);
        }
        writer.Complete();
        ASSERT_EQ(1.0, array.GetFloat(0));
        ASSERT_EQ(2.0, array.GetFloat(1));
        ASSERT_OK_AND_ASSIGN(auto res, array.ToFloatArray());
        ASSERT_EQ(res, arr);
    }
    {
        std::vector<double> arr = {1.0, 2.0};
        BinaryArray array;
        BinaryArrayWriter writer =
            BinaryArrayWriter(&array, arr.size(), sizeof(double), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteDouble(i, arr[i]);
        }
        writer.Complete();
        ASSERT_EQ(2, writer.GetNumElements());
        ASSERT_EQ(1.0, array.GetDouble(0));
        ASSERT_EQ(2.0, array.GetDouble(1));
        ASSERT_OK_AND_ASSIGN(auto res, array.ToDoubleArray());
        ASSERT_EQ(res, arr);
    }
    // decimal
    {
        // not compact (precision <= 18)
        std::vector<Decimal> arr = {Decimal(6, 2, 123456), Decimal(6, 3, 123456)};
        BinaryArray array;
        BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), 8, pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteDecimal(i, arr[i], 6);
        }
        writer.Complete();
        ASSERT_EQ(arr[0], array.GetDecimal(0, 6, 2));
        ASSERT_EQ(arr[1], array.GetDecimal(1, 6, 3));
    }
    {
        // compact (precision > 18)
        std::vector<Decimal> arr = {Decimal(/*precision=*/20, /*scale=*/3, 123456),
                                    Decimal(/*precision=*/20, /*scale=*/3, 123456789)};
        BinaryArray array;
        BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), 8, pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteDecimal(i, arr[i], /*precision=*/20);
        }
        writer.Complete();
        ASSERT_EQ(arr[0], array.GetDecimal(0, /*precision=*/20, /*scale=*/3));
        ASSERT_EQ(arr[1], array.GetDecimal(1, /*precision=*/20, /*scale=*/3));
    }
    // timestamp
    {
        // not compact (precision > 3)
        std::vector<Timestamp> arr = {Timestamp(0, 0), Timestamp(12345, 1)};
        BinaryArray array;
        BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), 8, pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteTimestamp(i, arr[i], 9);
        }
        writer.Complete();
        ASSERT_EQ(arr[0], array.GetTimestamp(0, 9));
        ASSERT_EQ(arr[1], array.GetTimestamp(1, 9));
    }
    {
        // compact (precision <= 3)
        std::vector<Timestamp> arr = {Timestamp(0, 0), Timestamp(12345, 0)};
        BinaryArray array;
        BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), 8, pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteTimestamp(i, arr[i], 3);
        }
        writer.Complete();
        ASSERT_EQ(arr[0], array.GetTimestamp(0, 3));
        ASSERT_EQ(arr[1], array.GetTimestamp(1, 3));
    }
    // binary
    {
        std::vector<Bytes> arr;
        arr.emplace_back("hello", pool.get());
        arr.emplace_back("world", pool.get());
        BinaryArray array;
        BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), 8, pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteBinary(i, arr[i]);
        }
        writer.Complete();
        ASSERT_EQ(arr[0], *array.GetBinary(0));
        ASSERT_EQ(arr[1], *array.GetBinary(1));
    }
    // array
    {
        // element1
        std::vector<int32_t> arr1 = {1, 2};
        BinaryArray array1;
        BinaryArrayWriter writer1 =
            BinaryArrayWriter(&array1, arr1.size(), sizeof(int32_t), pool.get());
        for (size_t i = 0; i < arr1.size(); i++) {
            writer1.WriteInt(i, arr1[i]);
        }
        writer1.Complete();
        // element2
        std::vector<int32_t> arr2 = {100, 200};
        BinaryArray array2;
        BinaryArrayWriter writer2 =
            BinaryArrayWriter(&array2, arr2.size(), sizeof(int32_t), pool.get());
        for (size_t i = 0; i < arr2.size(); i++) {
            writer2.WriteInt(i, arr2[i]);
        }
        writer2.Complete();
        // array
        std::vector<BinaryArray> arr;
        arr.push_back(array1);
        arr.push_back(array2);
        BinaryArray array;
        BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), 8, pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteArray(i, arr[i]);
        }
        writer.Complete();
        ASSERT_EQ(arr[0].ToIntArray().value(), array.GetArray(0)->ToIntArray().value());
        ASSERT_EQ(arr[1].ToIntArray().value(), array.GetArray(1)->ToIntArray().value());
    }
    // row
    {
        BinaryRow row1 = BinaryRowGenerator::GenerateRow(
            {std::string("Alice"), 30, 12.1, NullType()}, pool.get());
        BinaryRow row2 =
            BinaryRowGenerator::GenerateRow({std::string("Bob"), 40, 14.1, NullType()}, pool.get());
        BinaryArray array;
        BinaryArrayWriter writer =
            BinaryArrayWriter(&array, /*num_elements=*/2, /*element_size=*/8, pool.get());
        writer.WriteRow(0, row1);
        writer.WriteRow(1, row2);
        writer.Complete();

        ASSERT_EQ(2, array.Size());
        auto de_row1 = array.GetRow(0, 4);
        ASSERT_EQ(row1, *(std::dynamic_pointer_cast<BinaryRow>(de_row1)));
        auto de_row2 = array.GetRow(1, 4);
        ASSERT_EQ(row2, *(std::dynamic_pointer_cast<BinaryRow>(de_row2)));
    }
    // map
    {
        auto key = BinaryArray::FromIntArray({1, 2, 3, 5}, pool.get());
        auto value = BinaryArray::FromLongArray({100ll, 200ll, 300ll, 500ll}, pool.get());
        auto map = BinaryMap::ValueOf(key, value, pool.get());
        BinaryArray array;
        BinaryArrayWriter writer =
            BinaryArrayWriter(&array, /*num_elements=*/1, /*element_size=*/8, pool.get());
        writer.WriteMap(0, *map);
        writer.Complete();

        ASSERT_EQ(1, array.Size());
        auto de_map = array.GetMap(0);
        ASSERT_EQ(key.HashCode(),
                  std::dynamic_pointer_cast<BinaryArray>(de_map->KeyArray())->HashCode());
        ASSERT_EQ(value.HashCode(),
                  std::dynamic_pointer_cast<BinaryArray>(de_map->ValueArray())->HashCode());
    }
}

TEST(BinaryArrayTest, TestCopy) {
    auto pool = GetDefaultPool();

    std::vector<bool> arr = {true, false};
    BinaryArray array;
    BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), sizeof(bool), pool.get());
    for (size_t i = 0; i < arr.size(); i++) {
        writer.WriteBoolean(i, arr[i]);
    }
    writer.Complete();

    auto copy_array = array.Copy(pool.get());
    ASSERT_EQ(array.ToBooleanArray().value(), copy_array.ToBooleanArray().value());
}

TEST(BinaryArrayTest, TestNullValue) {
    auto pool = GetDefaultPool();
    std::vector<int64_t> arr = {1, 2, 3, 4, 5};
    BinaryArray array;
    BinaryArrayWriter writer =
        BinaryArrayWriter(&array, arr.size() + 2, sizeof(int64_t), pool.get());
    for (size_t i = 0; i < arr.size(); i++) {
        writer.WriteLong(i, arr[i]);
    }
    // last two element is null
    writer.SetNullValue<int64_t>(5);
    writer.SetNullAt(6);
    writer.Complete();
    ASSERT_TRUE(array.AnyNull());

    auto ret = BinaryArray::FromLongArray(&array, pool.get());
    ASSERT_EQ(ret, array);
}

TEST(BinaryArrayTest, TestFromLongArray) {
    auto pool = GetDefaultPool();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::list(arrow::int64()),
                                                        R"([[123, null], [789], [12345], [12]])")
                  .ValueOrDie();
    auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
    auto array = ColumnarArray(list_array->values(), pool, /*offset=*/0, 2);

    BinaryArray ret = BinaryArray::FromLongArray(&array, pool.get());

    BinaryArray expected_array;
    BinaryArrayWriter writer = BinaryArrayWriter(&expected_array, 2, sizeof(int64_t), pool.get());
    writer.Reset();
    writer.WriteLong(0, 123);
    writer.SetNullAt(1);
    writer.Complete();

    ASSERT_EQ(ret, expected_array);

    ASSERT_NOK_WITH_MSG(expected_array.ToLongArray(),
                        "Primitive array must not contain a null value.");
}

TEST(BinaryArrayTest, TestFromIntArray) {
    auto pool = GetDefaultPool();
    std::vector<int32_t> values = {10, 20, 30, 100};
    BinaryArray array = BinaryArray::FromIntArray(values, pool.get());
    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> result, array.ToIntArray());
    ASSERT_EQ(result, values);
}

TEST(BinaryArrayTest, TestFromAllNullLongArray) {
    auto pool = GetDefaultPool();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::list(arrow::int64()),
                                                        R"([[null, null], [789], [12345], [12]])")
                  .ValueOrDie();
    auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
    auto array = ColumnarArray(list_array->values(), pool, /*offset=*/0, 2);

    BinaryArray ret = BinaryArray::FromLongArray(&array, pool.get());

    BinaryArray expected_array;
    BinaryArrayWriter writer = BinaryArrayWriter(&expected_array, 2, sizeof(int64_t), pool.get());
    writer.Reset();
    writer.SetNullAt(0);
    writer.SetNullAt(1);
    writer.Complete();

    ASSERT_EQ(ret, expected_array);

    ASSERT_NOK_WITH_MSG(expected_array.ToLongArray(),
                        "Primitive array must not contain a null value.");
}
TEST(BinaryArrayTest, TestReset) {
    auto pool = GetDefaultPool();
    std::vector<int64_t> arr = {1, 2, 3, 4, 5};
    BinaryArray array;
    BinaryArrayWriter writer = BinaryArrayWriter(&array, arr.size(), sizeof(int64_t), pool.get());
    writer.Reset();
    for (size_t i = 0; i < arr.size(); i++) {
        writer.WriteLong(i, arr[i]);
    }
    writer.Complete();
    ASSERT_EQ(arr, array.ToLongArray().value());
}

TEST(BinaryArrayTest, TestGetElementSize) {
    ASSERT_EQ(sizeof(bool), BinaryArrayWriter::GetElementSize(arrow::Type::type::BOOL));
    ASSERT_EQ(sizeof(int8_t), BinaryArrayWriter::GetElementSize(arrow::Type::type::INT8));
    ASSERT_EQ(sizeof(int16_t), BinaryArrayWriter::GetElementSize(arrow::Type::type::INT16));
    ASSERT_EQ(sizeof(int32_t), BinaryArrayWriter::GetElementSize(arrow::Type::type::INT32));
    ASSERT_EQ(sizeof(int32_t), BinaryArrayWriter::GetElementSize(arrow::Type::type::DATE32));
    ASSERT_EQ(sizeof(int64_t), BinaryArrayWriter::GetElementSize(arrow::Type::type::INT64));
    ASSERT_EQ(sizeof(float), BinaryArrayWriter::GetElementSize(arrow::Type::type::FLOAT));
    ASSERT_EQ(sizeof(double), BinaryArrayWriter::GetElementSize(arrow::Type::type::DOUBLE));
    // default cases: variable-length types use 8 bytes (offset + length)
    ASSERT_EQ(8, BinaryArrayWriter::GetElementSize(arrow::Type::type::STRING));
    ASSERT_EQ(8, BinaryArrayWriter::GetElementSize(arrow::Type::type::BINARY));
    ASSERT_EQ(8, BinaryArrayWriter::GetElementSize(arrow::Type::type::TIMESTAMP));
    ASSERT_EQ(8, BinaryArrayWriter::GetElementSize(arrow::Type::type::DECIMAL128));
}

TEST(BinaryArrayTest, TestSetNullAtWithArrowType) {
    auto pool = GetDefaultPool();

    {
        // BOOL
        BinaryArray array;
        BinaryArrayWriter writer(&array, 2, sizeof(bool), pool.get());
        writer.WriteBoolean(0, true);
        writer.SetNullAt(1, arrow::Type::type::BOOL);
        writer.Complete();
        ASSERT_FALSE(array.IsNullAt(0));
        ASSERT_TRUE(array.GetBoolean(0));
        ASSERT_TRUE(array.IsNullAt(1));
    }
    {
        // INT8
        BinaryArray array;
        BinaryArrayWriter writer(&array, 2, sizeof(int8_t), pool.get());
        writer.WriteByte(0, 42);
        writer.SetNullAt(1, arrow::Type::type::INT8);
        writer.Complete();
        ASSERT_FALSE(array.IsNullAt(0));
        ASSERT_EQ(42, array.GetByte(0));
        ASSERT_TRUE(array.IsNullAt(1));
    }
    {
        // INT16
        BinaryArray array;
        BinaryArrayWriter writer(&array, 2, sizeof(int16_t), pool.get());
        writer.WriteShort(0, 1000);
        writer.SetNullAt(1, arrow::Type::type::INT16);
        writer.Complete();
        ASSERT_FALSE(array.IsNullAt(0));
        ASSERT_EQ(1000, array.GetShort(0));
        ASSERT_TRUE(array.IsNullAt(1));
    }
    {
        // INT32
        BinaryArray array;
        BinaryArrayWriter writer(&array, 2, sizeof(int32_t), pool.get());
        writer.WriteInt(0, 100000);
        writer.SetNullAt(1, arrow::Type::type::INT32);
        writer.Complete();
        ASSERT_FALSE(array.IsNullAt(0));
        ASSERT_EQ(100000, array.GetInt(0));
        ASSERT_TRUE(array.IsNullAt(1));
    }
    {
        // DATE32
        BinaryArray array;
        BinaryArrayWriter writer(&array, 2, sizeof(int32_t), pool.get());
        writer.WriteInt(0, 19000);
        writer.SetNullAt(1, arrow::Type::type::DATE32);
        writer.Complete();
        ASSERT_FALSE(array.IsNullAt(0));
        ASSERT_EQ(19000, array.GetDate(0));
        ASSERT_TRUE(array.IsNullAt(1));
    }
    {
        // INT64
        BinaryArray array;
        BinaryArrayWriter writer(&array, 2, sizeof(int64_t), pool.get());
        writer.WriteLong(0, 123456789L);
        writer.SetNullAt(1, arrow::Type::type::INT64);
        writer.Complete();
        ASSERT_FALSE(array.IsNullAt(0));
        ASSERT_EQ(123456789L, array.GetLong(0));
        ASSERT_TRUE(array.IsNullAt(1));
    }
    {
        // FLOAT
        BinaryArray array;
        BinaryArrayWriter writer(&array, 2, sizeof(float), pool.get());
        writer.WriteFloat(0, 3.14f);
        writer.SetNullAt(1, arrow::Type::type::FLOAT);
        writer.Complete();
        ASSERT_FALSE(array.IsNullAt(0));
        ASSERT_FLOAT_EQ(3.14f, array.GetFloat(0));
        ASSERT_TRUE(array.IsNullAt(1));
    }
    {
        // DOUBLE
        BinaryArray array;
        BinaryArrayWriter writer(&array, 2, sizeof(double), pool.get());
        writer.WriteDouble(0, 2.718);
        writer.SetNullAt(1, arrow::Type::type::DOUBLE);
        writer.Complete();
        ASSERT_FALSE(array.IsNullAt(0));
        ASSERT_DOUBLE_EQ(2.718, array.GetDouble(0));
        ASSERT_TRUE(array.IsNullAt(1));
    }
    {
        // STRING (default path, uses 8-byte null)
        BinaryArray array;
        BinaryArrayWriter writer(&array, 2, 8, pool.get());
        writer.WriteString(0, BinaryString::FromString("hello", pool.get()));
        writer.SetNullAt(1, arrow::Type::type::STRING);
        writer.Complete();
        ASSERT_FALSE(array.IsNullAt(0));
        ASSERT_EQ("hello", std::string(array.GetStringView(0)));
        ASSERT_TRUE(array.IsNullAt(1));
    }
}

}  // namespace paimon::test
