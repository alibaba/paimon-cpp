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

#include "paimon/common/utils/projected_array.h"

#include <algorithm>
#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_map.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"

namespace paimon::test {
TEST(ProjectedArrayTest, TestSimple) {
    auto pool = GetDefaultPool();
    {
        std::vector<bool> arr = {true, false};
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer =
            BinaryArrayWriter(array.get(), arr.size(), sizeof(bool), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteBoolean(i, arr[i]);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {1, 0, -1};
        ProjectedArray projected_array(array, mapping);
        ASSERT_EQ(projected_array.GetBoolean(1), true);
        ASSERT_EQ(projected_array.GetBoolean(0), false);
        ASSERT_TRUE(projected_array.IsNullAt(2));
    }
    {
        std::vector<int8_t> arr = {1, 2, 3, 4, 5};
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer =
            BinaryArrayWriter(array.get(), arr.size(), sizeof(int8_t), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteByte(i, arr[i]);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {4, 3, 2, 1, 0, -1};
        ProjectedArray projected_array(array, mapping);
        ASSERT_EQ(projected_array.GetByte(0), 5);
        ASSERT_EQ(projected_array.GetByte(1), 4);
        ASSERT_EQ(projected_array.GetByte(4), 1);
        ASSERT_TRUE(projected_array.IsNullAt(5));
    }
    {
        std::vector<int16_t> arr = {1, 2, 3, 4, 5};
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer =
            BinaryArrayWriter(array.get(), arr.size(), sizeof(int16_t), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteShort(i, arr[i]);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {4, 3, 2, 1, 0, -1};
        ProjectedArray projected_array(array, mapping);
        ASSERT_EQ(projected_array.GetShort(0), 5);
        ASSERT_EQ(projected_array.GetShort(1), 4);
        ASSERT_EQ(projected_array.GetShort(4), 1);
        ASSERT_TRUE(projected_array.IsNullAt(5));
    }
    {
        std::vector<int32_t> arr = {1, 2, 3, 4, 5};
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer =
            BinaryArrayWriter(array.get(), arr.size(), sizeof(int32_t), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteInt(i, arr[i]);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {4, 3, 2, 1, 0, -1};
        ProjectedArray projected_array(array, mapping);
        ASSERT_EQ(projected_array.GetInt(0), 5);
        ASSERT_EQ(projected_array.GetInt(1), 4);
        ASSERT_EQ(projected_array.GetInt(4), 1);
        ASSERT_TRUE(projected_array.IsNullAt(5));
    }
    {
        // test date
        std::vector<int32_t> arr = {10000, 20006};
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer =
            BinaryArrayWriter(array.get(), arr.size(), sizeof(int32_t), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteInt(i, arr[i]);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {1, 0, -1};
        ProjectedArray projected_array(array, mapping);
        ASSERT_EQ(projected_array.GetDate(0), 20006);
        ASSERT_EQ(projected_array.GetDate(1), 10000);
        ASSERT_TRUE(projected_array.IsNullAt(2));
    }
    {
        std::vector<float> arr = {1.0, 2.0};
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer =
            BinaryArrayWriter(array.get(), arr.size(), sizeof(float), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteFloat(i, arr[i]);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {1, 0, -1};
        ProjectedArray projected_array(array, mapping);
        ASSERT_EQ(projected_array.GetFloat(0), 2.0);
        ASSERT_EQ(projected_array.GetFloat(1), 1.0);
        ASSERT_TRUE(projected_array.IsNullAt(2));
    }
    {
        std::vector<double> arr = {1.0, 2.0};
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer =
            BinaryArrayWriter(array.get(), arr.size(), sizeof(double), pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteDouble(i, arr[i]);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {1, 0, -1};
        ProjectedArray projected_array(array, mapping);
        ASSERT_EQ(projected_array.GetDouble(0), 2.0);
        ASSERT_EQ(projected_array.GetDouble(1), 1.0);
        ASSERT_TRUE(projected_array.IsNullAt(2));
    }
    // decimal
    {
        // not compact (precision <= 18)
        std::vector<Decimal> arr = {Decimal(6, 2, 123456), Decimal(6, 3, 123456)};
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer =
            BinaryArrayWriter(array.get(), arr.size(), /*element_size=*/8, pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteDecimal(i, arr[i], 6);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {1, 0, -1};
        ProjectedArray projected_array(array, mapping);
        ASSERT_EQ(projected_array.GetDecimal(0, 6, 3), Decimal(6, 3, 123456));
        ASSERT_EQ(projected_array.GetDecimal(1, 6, 2), Decimal(6, 2, 123456));
        ASSERT_TRUE(projected_array.IsNullAt(2));
    }
    {
        // compact (precision > 18)
        std::vector<Decimal> arr = {Decimal(/*precision=*/20, /*scale=*/3, 123456),
                                    Decimal(/*precision=*/20, /*scale=*/3, 123456789)};
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer =
            BinaryArrayWriter(array.get(), arr.size(), /*element_size=*/8, pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteDecimal(i, arr[i], /*precision=*/20);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {1, 0, -1};
        ProjectedArray projected_array(array, mapping);
        ASSERT_EQ(projected_array.GetDecimal(0, 20, 3), Decimal(20, 3, 123456789));
        ASSERT_EQ(projected_array.GetDecimal(1, 20, 3), Decimal(20, 3, 123456));
        ASSERT_TRUE(projected_array.IsNullAt(2));
    }
    // timestamp
    {
        std::vector<Timestamp> arr = {Timestamp(0, 0), Timestamp(12345, 1)};
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer =
            BinaryArrayWriter(array.get(), arr.size(), /*element_size=*/8, pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteTimestamp(i, arr[i], 9);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {1, 0, -1};
        ProjectedArray projected_array(array, mapping);
        ASSERT_EQ(projected_array.GetTimestamp(0, 9), Timestamp(12345, 1));
        ASSERT_EQ(projected_array.GetTimestamp(1, 9), Timestamp(0, 0));
        ASSERT_TRUE(projected_array.IsNullAt(2));
    }
    // binary
    {
        std::vector<Bytes> arr;
        arr.emplace_back("hello", pool.get());
        arr.emplace_back("world", pool.get());
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer =
            BinaryArrayWriter(array.get(), arr.size(), /*element_size=*/8, pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteBinary(i, arr[i]);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {1, 0, -1};
        ProjectedArray projected_array(array, mapping);
        ASSERT_EQ(*projected_array.GetBinary(0), arr[1]);
        ASSERT_EQ(*projected_array.GetBinary(1), arr[0]);
        ASSERT_TRUE(projected_array.IsNullAt(2));
    }
    // string
    {
        std::vector<BinaryString> arr;
        arr.push_back(BinaryString::FromString("hello", pool.get()));
        arr.push_back(BinaryString::FromString("world", pool.get()));
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer =
            BinaryArrayWriter(array.get(), arr.size(), /*element_size=*/8, pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteString(i, arr[i]);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {1, 0, -1};
        ProjectedArray projected_array(array, mapping);
        ASSERT_EQ(projected_array.GetString(0), arr[1]);
        ASSERT_EQ(projected_array.GetString(1), arr[0]);
        ASSERT_TRUE(projected_array.IsNullAt(2));
    }
    // array
    {
        // element1
        std::vector<int32_t> arr1 = {1, 2};
        auto array1 = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer1 =
            BinaryArrayWriter(array1.get(), arr1.size(), sizeof(int32_t), pool.get());
        for (size_t i = 0; i < arr1.size(); i++) {
            writer1.WriteInt(i, arr1[i]);
        }
        writer1.Complete();
        // element2
        std::vector<int32_t> arr2 = {100, 200};
        auto array2 = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer2 =
            BinaryArrayWriter(array2.get(), arr2.size(), sizeof(int32_t), pool.get());
        for (size_t i = 0; i < arr2.size(); i++) {
            writer2.WriteInt(i, arr2[i]);
        }
        writer2.Complete();
        // array
        std::vector<std::shared_ptr<BinaryArray>> arr;
        arr.push_back(array1);
        arr.push_back(array2);
        auto array = std::make_shared<BinaryArray>();
        BinaryArrayWriter writer = BinaryArrayWriter(array.get(), arr.size(), 8, pool.get());
        for (size_t i = 0; i < arr.size(); i++) {
            writer.WriteArray(i, *arr[i]);
        }
        writer.Complete();

        std::vector<int32_t> mapping = {1, 0, -1};
        ProjectedArray projected_array(array, mapping);

        auto ret0 = std::dynamic_pointer_cast<BinaryArray>(projected_array.GetArray(0));
        auto ret1 = std::dynamic_pointer_cast<BinaryArray>(projected_array.GetArray(1));
        ASSERT_TRUE(ret0);
        ASSERT_TRUE(ret1);
        ASSERT_EQ(*ret0, *arr[1]);
        ASSERT_EQ(*ret1, *arr[0]);
        ASSERT_TRUE(projected_array.IsNullAt(2));
    }
}

TEST(ProjectedArrayTest, TestGetStringView) {
    auto pool = GetDefaultPool();
    std::vector<BinaryString> arr;
    arr.push_back(BinaryString::FromString("hello", pool.get()));
    arr.push_back(BinaryString::FromString("world", pool.get()));
    auto array = std::make_shared<BinaryArray>();
    BinaryArrayWriter writer =
        BinaryArrayWriter(array.get(), arr.size(), /*element_size=*/8, pool.get());
    for (size_t i = 0; i < arr.size(); i++) {
        writer.WriteString(i, arr[i]);
    }
    writer.Complete();

    std::vector<int32_t> mapping = {1, 0, -1};
    ProjectedArray projected_array(array, mapping);
    ASSERT_EQ(projected_array.GetStringView(0), "world");
    ASSERT_EQ(projected_array.GetStringView(1), "hello");
    ASSERT_TRUE(projected_array.IsNullAt(2));
}

TEST(ProjectedArrayTest, TestGetMap) {
    auto pool = GetDefaultPool();
    auto key = BinaryArray::FromIntArray({1, 2, 3}, pool.get());
    auto value = BinaryArray::FromLongArray({100ll, 200ll, 300ll}, pool.get());
    auto map1 = BinaryMap::ValueOf(key, value, pool.get());

    auto key2 = BinaryArray::FromIntArray({10, 20}, pool.get());
    auto value2 = BinaryArray::FromLongArray({1000ll, 2000ll}, pool.get());
    auto map2 = BinaryMap::ValueOf(key2, value2, pool.get());

    auto array = std::make_shared<BinaryArray>();
    BinaryArrayWriter writer =
        BinaryArrayWriter(array.get(), /*num_elements=*/2, /*element_size=*/8, pool.get());
    writer.WriteMap(0, *map1);
    writer.WriteMap(1, *map2);
    writer.Complete();

    std::vector<int32_t> mapping = {1, 0, -1};
    ProjectedArray projected_array(array, mapping);

    // mapping[0] -> original index 1 -> map2
    auto result0 = projected_array.GetMap(0);
    ASSERT_EQ(result0->Size(), 2);
    ASSERT_EQ(result0->KeyArray()->ToIntArray().value(), (std::vector<int32_t>{10, 20}));

    // mapping[1] -> original index 0 -> map1
    auto result1 = projected_array.GetMap(1);
    ASSERT_EQ(result1->Size(), 3);
    ASSERT_EQ(result1->KeyArray()->ToIntArray().value(), (std::vector<int32_t>{1, 2, 3}));

    ASSERT_TRUE(projected_array.IsNullAt(2));
}

TEST(ProjectedArrayTest, TestGetRow) {
    auto pool = GetDefaultPool();
    BinaryRow row1 = BinaryRowGenerator::GenerateRow({std::string("Alice"), 30}, pool.get());
    BinaryRow row2 = BinaryRowGenerator::GenerateRow({std::string("Bob"), 40}, pool.get());

    auto array = std::make_shared<BinaryArray>();
    BinaryArrayWriter writer =
        BinaryArrayWriter(array.get(), /*num_elements=*/2, /*element_size=*/8, pool.get());
    writer.WriteRow(0, row1);
    writer.WriteRow(1, row2);
    writer.Complete();

    std::vector<int32_t> mapping = {1, 0, -1};
    ProjectedArray projected_array(array, mapping);

    // mapping[0] -> original index 1 -> row2
    auto result0 = std::dynamic_pointer_cast<BinaryRow>(projected_array.GetRow(0, 2));
    ASSERT_TRUE(result0);
    ASSERT_EQ(*result0, row2);

    // mapping[1] -> original index 0 -> row1
    auto result1 = std::dynamic_pointer_cast<BinaryRow>(projected_array.GetRow(1, 2));
    ASSERT_TRUE(result1);
    ASSERT_EQ(*result1, row1);

    ASSERT_TRUE(projected_array.IsNullAt(2));
}

}  // namespace paimon::test
