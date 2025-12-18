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
#include "paimon/common/data/columnar/columnar_array.h"
#include "paimon/common/utils/date_time_utils.h"
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

}  // namespace paimon::test
