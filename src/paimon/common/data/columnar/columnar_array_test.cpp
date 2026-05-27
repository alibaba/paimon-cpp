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

#include "paimon/common/data/columnar/columnar_array.h"

#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/util/checked_cast.h"
#include "gtest/gtest.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(ColumnarArrayTest, TestSimple) {
    auto pool = GetDefaultPool();
    {
        auto f1 =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::list(arrow::boolean()), "[[true, false], [true], [false], [false, true]]")
                .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/2, 1);
        ASSERT_EQ(array.Size(), 1);
        ASSERT_EQ(array.GetBoolean(0), true);
        std::vector<char> expected_array = {static_cast<char>(1)};
        ASSERT_EQ(array.ToBooleanArray().value(), expected_array);
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::list(arrow::int8()),
                                                            "[[1, 1, 2], [3], [2], [2]]")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/5, 1);
        ASSERT_EQ(array.GetByte(0), 2);
        std::vector<char> expected_array = {static_cast<char>(2)};
        ASSERT_EQ(array.ToByteArray().value(), expected_array);
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::list(arrow::int16()),
                                                            "[[1, 1, 2], [3], [2], [-4]]")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/0, 3);
        ASSERT_EQ(array.GetShort(0), 1);
        ASSERT_EQ(array.GetShort(1), 1);
        ASSERT_EQ(array.GetShort(2), 2);
        std::vector<int16_t> expected_array = {1, 1, 2};
        ASSERT_EQ(array.ToShortArray().value(), expected_array);
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::list(arrow::int32()),
                                                            "[[1, 1, 2], [3], [2], [-4]]")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/3, 1);
        ASSERT_EQ(array.GetInt(0), 3);
        std::vector<int32_t> expected_array = {3};
        ASSERT_EQ(array.ToIntArray().value(), expected_array);
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::list(arrow::int64()),
                                                            "[[1, 1, 2], [3], [2], [-4]]")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/4, 1);
        ASSERT_EQ(array.GetLong(0), 2);
        std::vector<int64_t> expected_array = {2};
        ASSERT_EQ(array.ToLongArray().value(), expected_array);
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::list(arrow::int64()),
                                                            "[[1, 1, 2], [3], [null], null]")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/4, 1);
        ASSERT_NOK_WITH_MSG(array.ToLongArray(), "is null");
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::float32()), "[[0.0, 1.1, 2.2], [3.3], [4.4], [5.5]]")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/0, 3);
        ASSERT_NEAR(array.GetFloat(1), 1.1, 0.001);
        std::vector<float> expected_array = {0.0, 1.1, 2.2};
        ASSERT_EQ(array.ToFloatArray().value(), expected_array);
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::float64()), "[[0.0, 1.1, 2.2], [3.3], [4.4], [5.5]]")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/3, 1);
        ASSERT_NEAR(array.GetDouble(0), 3.3, 0.001);
        std::vector<double> expected_array = {3.3};
        ASSERT_EQ(array.ToDoubleArray().value(), expected_array);
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::utf8()), R"([["abc", "def"], ["efg"], ["hello"], ["hi"]])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/4, 1);
        ASSERT_EQ(array.GetString(0).ToString(), "hi");
        ASSERT_EQ(std::string(array.GetStringView(0)), "hi");
    }
}

TEST(ColumnarArrayTest, TestComplexAndNestedType) {
    auto pool = GetDefaultPool();
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::list(arrow::date32()),
                                                            "[[1, 1, 2], [3], [2], [-4]]")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/3, 1);
        ASSERT_EQ(array.GetDate(0), 3);
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::decimal128(10, 3)),
                      R"([["1.234", "1234.000"], ["-9876.543"], ["666.888"]])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/0, 2);
        ASSERT_EQ(array.GetDecimal(0, 10, 3), Decimal(10, 3, 1234));
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::timestamp(arrow::TimeUnit::NANO)),
                      R"([["1970-01-01T00:00:59"],["2000-02-29T23:23:23",
          "1899-01-01T00:59:20"],["2033-05-18T03:33:20"]])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/0, 1);
        auto ts = array.GetTimestamp(0, 9);
        ASSERT_EQ(ts, Timestamp(59000, 0));
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::list(arrow::binary()),
                                                            R"([["aaa", "bb"], ["ccc"], ["bbb"]])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/0, 2);
        ASSERT_EQ(*array.GetBinary(1), Bytes("bb", pool.get()));
        ASSERT_EQ(std::string(array.GetStringView(1)), "bb");
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::list(arrow::struct_({
                                                                field("sub1", arrow::int64()),
                                                                field("sub2", arrow::int64()),
                                                                field("sub3", arrow::int64()),
                                                                field("sub4", arrow::int64()),
                                                            })),
                                                            R"([
      [[1, 3, 2, 5],
      [2, 2, 1, 3]],
      [[3, 2, 1, 3]],
      [[4, 1, 0, 2]]
    ])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/0, 2);
        auto result_row = array.GetRow(1, 4);
        ASSERT_EQ(result_row->GetLong(0), 2);
        ASSERT_EQ(result_row->GetLong(1), 2);
        ASSERT_EQ(result_row->GetLong(2), 1);
        ASSERT_EQ(result_row->GetLong(3), 3);
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::list(arrow::int64())), "[[[1, 2, 3], [4, 5, 6]], []]")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/0, 1);
        auto result_array = array.GetArray(0);
        auto inner_result_array = array.GetArray(0);
        std::vector<int64_t> values = {1, 2, 3};
        ASSERT_EQ(inner_result_array->ToLongArray().value(), values);
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::map(arrow::int32(), arrow::int64())),
                      R"([
                       [[[1, 3], [4, 4]]], []
                      ])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        ASSERT_TRUE(list_array);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/0, 1);
        auto result_key = array.GetMap(0)->KeyArray();
        auto result_value = array.GetMap(0)->ValueArray();
        ASSERT_EQ(result_key->ToIntArray().value(), std::vector<int32_t>({1, 4}));
        ASSERT_EQ(result_value->ToLongArray().value(), std::vector<int64_t>({3, 4}));
    }
}
TEST(ColumnarArrayTest, TestTimestampType) {
    auto pool = GetDefaultPool();
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::timestamp(arrow::TimeUnit::SECOND)),
                      R"([["1970-01-01T00:00:59"],["2000-02-29T23:23:23",
          "1899-01-01T00:59:20"],["2033-05-18T03:33:20"]])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/1, 2);
        auto ts = array.GetTimestamp(0, 0);
        ASSERT_EQ(ts, Timestamp(951866603000, 0)) << ts.GetMillisecond();
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::timestamp(arrow::TimeUnit::MILLI)),
                      R"([["1970-01-01T00:00:59"],["2000-02-29T23:23:23.001",
          "1899-01-01T00:59:20"],["2033-05-18T03:33:20"]])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/1, 2);
        auto ts = array.GetTimestamp(0, 3);
        ASSERT_EQ(ts, Timestamp(951866603001, 0)) << ts.GetMillisecond();
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::timestamp(arrow::TimeUnit::MICRO)),
                      R"([["1970-01-01T00:00:59"],["2000-02-29T23:23:23.001001",
          "1899-01-01T00:59:20"],["2033-05-18T03:33:20"]])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/1, 2);
        auto ts = array.GetTimestamp(0, 6);
        ASSERT_EQ(ts, Timestamp(951866603001, 1000)) << ts.GetMillisecond();
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::timestamp(arrow::TimeUnit::NANO)),
                      R"([["1970-01-01T00:00:59"],["2000-02-29T23:23:23.001001001",
          "1899-01-01T00:59:20"],["2033-05-18T03:33:20"]])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/1, 2);
        auto ts = array.GetTimestamp(0, 9);
        ASSERT_EQ(ts, Timestamp(951866603001, 1001)) << ts.GetMillisecond();
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
                      R"([["1970-01-01T00:00:59"],["2000-02-29T23:23:23",
          "1899-01-01T00:59:20"],["2033-05-18T03:33:20"]])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/1, 2);
        auto ts = array.GetTimestamp(0, 0);
        ASSERT_EQ(ts, Timestamp(951866603000, 0)) << ts.GetMillisecond();
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
                      R"([["1970-01-01T00:00:59"],["2000-02-29T23:23:23.001",
          "1899-01-01T00:59:20"],["2033-05-18T03:33:20"]])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/1, 2);
        auto ts = array.GetTimestamp(0, 3);
        ASSERT_EQ(ts, Timestamp(951866603001, 0)) << ts.GetMillisecond();
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
                      R"([["1970-01-01T00:00:59"],["2000-02-29T23:23:23.001001",
          "1899-01-01T00:59:20"],["2033-05-18T03:33:20"]])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/1, 2);
        auto ts = array.GetTimestamp(0, 6);
        ASSERT_EQ(ts, Timestamp(951866603001, 1000)) << ts.GetMillisecond();
    }
    {
        auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::list(arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
                      R"([["1970-01-01T00:00:59"],["2000-02-29T23:23:23.001001001",
          "1899-01-01T00:59:20"],["2033-05-18T03:33:20"]])")
                      .ValueOrDie();
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(f1);
        auto array = ColumnarArray(list_array->values().get(), pool, /*offset=*/1, 2);
        auto ts = array.GetTimestamp(0, 9);
        ASSERT_EQ(ts, Timestamp(951866603001, 1001)) << ts.GetMillisecond();
    }
}

}  // namespace paimon::test
