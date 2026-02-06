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

#include "paimon/common/data/columnar/columnar_row.h"

#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::test {
TEST(ColumnarRowTest, TestSimple) {
    auto pool = GetDefaultPool();
    std::shared_ptr<arrow::DataType> target_type =
        arrow::struct_({arrow::field("f1", arrow::boolean()), arrow::field("f2", arrow::int8()),
                        arrow::field("f3", arrow::int16()), arrow::field("f4", arrow::int32()),
                        arrow::field("f5", arrow::int64()), arrow::field("f6", arrow::float32()),
                        arrow::field("f7", arrow::float64()), arrow::field("f8", arrow::utf8())});
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::boolean(), R"([true, false, false, true])")
            .ValueOrDie();
    auto f2 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int8(), R"([0, 1, 2, 3])").ValueOrDie();
    auto f3 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int16(), R"([4, 5, 6, 7])").ValueOrDie();
    auto f4 = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([10, 11, 12, 13])")
                  .ValueOrDie();
    auto f5 = arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([15, 16, 17, 18])")
                  .ValueOrDie();
    auto f6 = arrow::ipc::internal::json::ArrayFromJSON(arrow::float32(), R"([0.0, 1.1, 2.2, 3.3])")
                  .ValueOrDie();
    auto f7 = arrow::ipc::internal::json::ArrayFromJSON(arrow::float64(), R"([5.5, 6.6, 7.7, 8.8])")
                  .ValueOrDie();
    auto f8 = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(),
                                                        R"(["Hello", "World", "HELLO", "WORLD"])")
                  .ValueOrDie();
    auto data = arrow::StructArray::Make({f1, f2, f3, f4, f5, f6, f7, f8}, target_type->fields())
                    .ValueOrDie();

    auto row = ColumnarRow(data->fields(), pool, 0);
    ASSERT_EQ(row.GetFieldCount(), 8);
    ASSERT_EQ(row.GetBoolean(0), true);
    ASSERT_EQ(row.GetByte(1), 0);
    ASSERT_EQ(row.GetShort(2), 4);
    ASSERT_EQ(row.GetInt(3), 10);
    ASSERT_EQ(row.GetLong(4), 15);
    ASSERT_EQ(row.GetFloat(5), 0.0);
    ASSERT_EQ(row.GetDouble(6), 5.5);
    ASSERT_EQ(row.GetString(7).ToString(), "Hello");
    ASSERT_EQ(std::string(row.GetStringView(7)), "Hello");
}

TEST(ColumnarRowRefTest, TestSimple) {
    auto pool = GetDefaultPool();
    std::shared_ptr<arrow::DataType> target_type =
        arrow::struct_({arrow::field("f1", arrow::int32()), arrow::field("f2", arrow::utf8())});
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([1, 2, 3])").ValueOrDie();
    auto f2 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["alpha", "beta", "gamma"])")
            .ValueOrDie();
    auto data = arrow::StructArray::Make({f1, f2}, target_type->fields()).ValueOrDie();

    auto ctx = std::make_shared<ColumnarBatchContext>(data, data->fields(), pool);
    ColumnarRowRef row(ctx, 1);
    ASSERT_EQ(row.GetFieldCount(), 2);
    ASSERT_EQ(row.GetInt(0), 2);
    ASSERT_EQ(std::string(row.GetStringView(1)), "beta");

    auto row_kind = row.GetRowKind();
    ASSERT_TRUE(row_kind.ok());
    ASSERT_EQ(row_kind.value(), RowKind::Insert());
    row.SetRowKind(RowKind::Delete());
    auto updated_kind = row.GetRowKind();
    ASSERT_TRUE(updated_kind.ok());
    ASSERT_EQ(updated_kind.value(), RowKind::Delete());
}

TEST(ColumnarRowTest, TestComplexAndNestedType) {
    auto pool = GetDefaultPool();
    std::shared_ptr<arrow::DataType> target_type = arrow::struct_({
        arrow::field("f0", arrow::date32()),
        arrow::field("f1", arrow::decimal128(10, 3)),
        arrow::field("f2", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f3", arrow::binary()),
        arrow::field(
            "f4", arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::int64()),
                                  field("sub3", arrow::int64()), field("sub4", arrow::int64())})),
        arrow::field("f5", arrow::list(arrow::int64())),
        arrow::field("f6", arrow::map(arrow::int32(), arrow::int64())),
        arrow::field("f7", arrow::map(arrow::int32(), arrow::list(arrow::int64()))),
        arrow::field("f8", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("f9", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("f10", arrow::timestamp(arrow::TimeUnit::MICRO)),
    });
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::date32(), R"([109, 1000, -1000, 555])")
            .ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                  arrow::decimal128(10, 3), R"(["1.234", "1234.000", "-9876.543", "666.888"])")
                  .ValueOrDie();
    auto f2 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::timestamp(arrow::TimeUnit::NANO),
                                                  R"(["1970-01-01T00:00:59","2000-02-29T23:23:23",
          "1899-01-01T00:59:20","2033-05-18T03:33:20"])")
            .ValueOrDie();
    auto f3 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::binary(), R"(["aaa", "bb", "ccc", "bbb"])")
            .ValueOrDie();
    auto f4 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({
                                                            field("sub1", arrow::int64()),
                                                            field("sub2", arrow::int64()),
                                                            field("sub3", arrow::int64()),
                                                            field("sub4", arrow::int64()),
                                                        }),
                                                        R"([
      [1, 3, 2, 5],
      [2, 2, 1, 3],
      [3, 2, 1, 3],
      [4, 1, 0, 2]
    ])")
                  .ValueOrDie();
    auto f5 = arrow::ipc::internal::json::ArrayFromJSON(arrow::list(arrow::int64()),
                                                        "[[1, 1, 2], [3], [2], [-4]]")
                  .ValueOrDie();
    auto f6 = arrow::ipc::internal::json::ArrayFromJSON(arrow::map(arrow::int32(), arrow::int64()),
                                                        R"([[[1, 3], [4, 4]],
                                                            [[1, 5], [7, 6], [100, 7]],
                                                            [[0, 9]],
                                                            null])")
                  .ValueOrDie();
    auto f7 = arrow::ipc::internal::json::ArrayFromJSON(
                  arrow::map(arrow::int32(), arrow::list(arrow::int64())),
                  R"([[[1, [10, 20]], [4, [40, 50, 100]]],
                      [[1, [1, 2]], [7, [6]], [100, [8]]],
                      [[0, [9]]],
                      null])")
                  .ValueOrDie();
    auto f8 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::timestamp(arrow::TimeUnit::SECOND),
                                                  R"(["1970-01-01T00:00:59","2000-02-29T23:23:23",
                                                      "1899-01-01T00:59:20","2033-05-18T03:33:20"])")
            .ValueOrDie();
    auto f9 = arrow::ipc::internal::json::ArrayFromJSON(
                  arrow::timestamp(arrow::TimeUnit::MILLI),
                  R"(["1970-01-01T00:00:59.001","2000-02-29T23:23:23",
                                                      "1899-01-01T00:59:20","2033-05-18T03:33:20"])")
                  .ValueOrDie();
    auto f10 = arrow::ipc::internal::json::ArrayFromJSON(
                   arrow::timestamp(arrow::TimeUnit::MICRO),
                   R"(["1970-01-01T00:00:59.000001","2000-02-29T23:23:23",
                                                      "1899-01-01T00:59:20","2033-05-18T03:33:20"])")
                   .ValueOrDie();

    auto data = arrow::StructArray::Make({f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10},
                                         target_type->fields())
                    .ValueOrDie();

    auto row = ColumnarRow(data->fields(), pool, 0);
    ASSERT_EQ(row.GetFieldCount(), 11);
    ASSERT_EQ(row.GetDate(0), 109);
    ASSERT_EQ(row.GetDecimal(1, 10, 3), Decimal(10, 3, 1234));

    auto ts = row.GetTimestamp(2, /*precision=*/9);
    ASSERT_EQ(ts, Timestamp(59000, 0));

    ASSERT_EQ(*row.GetBinary(3), Bytes("aaa", pool.get()));
    ASSERT_EQ(std::string(row.GetStringView(3)), "aaa");

    auto result_row = row.GetRow(4, 4);
    ASSERT_EQ(result_row->GetLong(0), 1);
    ASSERT_EQ(result_row->GetLong(1), 3);
    ASSERT_EQ(result_row->GetLong(2), 2);
    ASSERT_EQ(result_row->GetLong(3), 5);

    std::vector<int64_t> values = {1, 1, 2};
    auto result_array = row.GetArray(5);
    ASSERT_EQ(result_array->ToLongArray().value(), values);

    auto result_key = row.GetMap(6)->KeyArray();
    auto result_value = row.GetMap(6)->ValueArray();
    ASSERT_EQ(result_key->ToIntArray().value(), std::vector<int32_t>({1, 4}));
    ASSERT_EQ(result_value->ToLongArray().value(), std::vector<int64_t>({3, 4}));

    result_key = row.GetMap(7)->KeyArray();
    result_value = row.GetMap(7)->ValueArray();

    ASSERT_EQ(result_key->ToIntArray().value(), std::vector<int32_t>({1, 4}));
    ASSERT_EQ(2, result_value->Size());
    ASSERT_EQ(result_value->GetArray(0)->ToLongArray().value(), std::vector<int64_t>({10, 20}));
    ASSERT_EQ(result_value->GetArray(1)->ToLongArray().value(),
              std::vector<int64_t>({40, 50, 100}));

    auto ts_second = row.GetTimestamp(8, /*precision=*/0);
    ASSERT_EQ(ts_second, Timestamp(59000, 0));

    auto ts_milli = row.GetTimestamp(9, /*precision=*/3);
    ASSERT_EQ(ts_milli, Timestamp(59001, 0));

    auto ts_micro = row.GetTimestamp(10, /*precision=*/6);
    ASSERT_EQ(ts_micro, Timestamp(59000, 1000));
}

TEST(ColumnarRowTest, TestTimestampType) {
    auto pool = GetDefaultPool();
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };
    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001", "1970-01-01 00:00:02", "1970-01-01 00:00:00.002", "1970-01-01 00:00:00.000002", "1970-01-01 00:00:00.000000002"],
["1970-01-01 00:00:03", "1970-01-01 00:00:00.003", null, "1970-01-01 00:00:00.000000003", "1970-01-01 00:00:04", "1970-01-01 00:00:00.004", "1970-01-01 00:00:00.000004", "1970-01-01 00:00:00.000000004"],
["1970-01-01 00:00:05", "1970-01-01 00:00:00.005", null, null, "1970-01-01 00:00:06", null, "1970-01-01 00:00:00.000006", null]
    ])")
            .ValueOrDie());
    {
        auto row = ColumnarRow(array->fields(), pool, 0);
        ASSERT_EQ(row.GetFieldCount(), 8);
        ASSERT_EQ(row.GetTimestamp(0, /*precision=*/0), Timestamp(1000, 0));
        ASSERT_EQ(row.GetTimestamp(1, /*precision=*/3), Timestamp(1, 0));
        ASSERT_EQ(row.GetTimestamp(2, /*precision=*/6), Timestamp(0, 1000));
        ASSERT_EQ(row.GetTimestamp(3, /*precision=*/9), Timestamp(0, 1));
        ASSERT_EQ(row.GetTimestamp(4, /*precision=*/0), Timestamp(2000, 0));
        ASSERT_EQ(row.GetTimestamp(5, /*precision=*/3), Timestamp(2, 0));
        ASSERT_EQ(row.GetTimestamp(6, /*precision=*/6), Timestamp(0, 2000));
        ASSERT_EQ(row.GetTimestamp(7, /*precision=*/9), Timestamp(0, 2));
    }
    {
        auto row = ColumnarRow(array->fields(), pool, 2);
        ASSERT_EQ(row.GetFieldCount(), 8);
        ASSERT_EQ(row.GetTimestamp(0, /*precision=*/0), Timestamp(5000, 0));
        ASSERT_EQ(row.GetTimestamp(1, /*precision=*/3), Timestamp(5, 0));
        ASSERT_TRUE(row.IsNullAt(2));
        ASSERT_TRUE(row.IsNullAt(3));
        ASSERT_EQ(row.GetTimestamp(4, /*precision=*/0), Timestamp(6000, 0));
        ASSERT_TRUE(row.IsNullAt(5));
        ASSERT_EQ(row.GetTimestamp(6, /*precision=*/6), Timestamp(0, 6000));
    }
}

TEST(ColumnarRowTest, TestNull) {
    auto pool = GetDefaultPool();
    std::shared_ptr<arrow::DataType> target_type =
        arrow::struct_({arrow::field("f1", arrow::boolean()), arrow::field("f2", arrow::int8())});
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::boolean(), R"([null, false, false, true])")
            .ValueOrDie();
    auto f2 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int8(), R"([null, 1, 2, 3])").ValueOrDie();
    auto data = arrow::StructArray::Make({f1, f2}, target_type->fields()).ValueOrDie();

    auto row = ColumnarRow(data->fields(), pool, 0);
    row.SetRowKind(RowKind::Insert());
    ASSERT_EQ(row.GetFieldCount(), 2);
    ASSERT_EQ(row.GetRowKind().value(), RowKind::Insert());
    ASSERT_TRUE(row.IsNullAt(0));
    ASSERT_TRUE(row.IsNullAt(1));
}

TEST(ColumnarRowTest, TestDictionary) {
    auto pool = GetDefaultPool();
    auto dict = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["foo", "bar", "baz"])")
                    .ValueOrDie();
    auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    auto indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 0, 2, 0]").ValueOrDie();
    std::shared_ptr<arrow::DictionaryArray> dict_array =
        std::make_shared<arrow::DictionaryArray>(dict_type, indices, dict);

    auto f0 = arrow::field("f0", arrow::list(dict_type));
    auto f1 = arrow::field("f1", arrow::struct_({arrow::field("sub1", arrow::int64()),
                                                 arrow::field("sub2", arrow::binary()),
                                                 arrow::field("sub3", dict_type)}));

    auto list_offsets =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 5])").ValueOrDie();
    auto f0_array = arrow::ListArray::FromArrays(*list_offsets, *dict_array).ValueOrDie();

    auto sub1_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([1])").ValueOrDie();
    auto sub2_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::binary(), R"(["apple"])").ValueOrDie();
    auto sub3_array =
        std::make_shared<arrow::DictionaryArray>(dict_type, indices->Slice(0, 1), dict);
    auto f1_array = std::make_shared<arrow::StructArray>(
        f1->type(), /*length=*/1, arrow::ArrayVector({sub1_array, sub2_array, sub3_array}));

    auto struct_type = arrow::struct_({f0, f1});
    // data: [["bar", "baz", "foo", "baz", "foo"], [1, "apple", "bar"]]
    auto data = std::make_shared<arrow::StructArray>(struct_type, /*length=*/1,
                                                     arrow::ArrayVector({f0_array, f1_array}));

    auto row = ColumnarRow(data->fields(), pool, 0);
    ASSERT_FALSE(row.IsNullAt(0));
    auto internal_array = row.GetArray(0);
    ASSERT_TRUE(internal_array);
    ASSERT_EQ(5, internal_array->Size());
    ASSERT_EQ("bar", internal_array->GetString(0).ToString());
    ASSERT_EQ("baz", internal_array->GetString(1).ToString());
    ASSERT_EQ("foo", internal_array->GetString(2).ToString());
    ASSERT_EQ("baz", internal_array->GetString(3).ToString());
    ASSERT_EQ("foo", internal_array->GetString(4).ToString());

    ASSERT_EQ("bar", std::string(internal_array->GetStringView(0)));
    ASSERT_EQ("baz", std::string(internal_array->GetStringView(1)));
    ASSERT_EQ("foo", std::string(internal_array->GetStringView(2)));
    ASSERT_EQ("baz", std::string(internal_array->GetStringView(3)));
    ASSERT_EQ("foo", std::string(internal_array->GetStringView(4)));

    ASSERT_FALSE(row.IsNullAt(1));
    auto internal_row = row.GetRow(1, 3);
    ASSERT_TRUE(internal_row);
    ASSERT_EQ(1, internal_row->GetLong(0));
    auto bytes = internal_row->GetBinary(1);
    ASSERT_EQ("apple", std::string(bytes->data(), bytes->size()));
    ASSERT_EQ("apple", std::string(internal_row->GetStringView(1)));
    ASSERT_EQ("bar", internal_row->GetString(2).ToString());
    ASSERT_EQ("bar", std::string(internal_row->GetStringView(2)));
}

TEST(ColumnarRowTest, TestDataLifeCycle) {
    auto pool = GetDefaultPool();
    std::shared_ptr<arrow::DataType> target_type = arrow::struct_({arrow::field(
        "f0", arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::int64()),
                              field("sub3", arrow::int64()), field("sub4", arrow::int64())}))});
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({
                                                            field("sub1", arrow::int64()),
                                                            field("sub2", arrow::int64()),
                                                            field("sub3", arrow::int64()),
                                                            field("sub4", arrow::int64()),
                                                        }),
                                                        R"([
      [1, 3, 2, 5],
      [2, 2, 1, 3],
      [3, 2, 1, 3],
      [4, 1, 0, 2]
    ])")
                  .ValueOrDie();
    auto data = arrow::StructArray::Make({f0}, target_type->fields()).ValueOrDie();
    auto row = std::make_unique<ColumnarRow>(data, data->fields(), pool, 0);
    data.reset();
    f0.reset();
    // array data is only held by columnar row
    ASSERT_EQ(1, row->struct_array_.use_count());

    ASSERT_EQ(row->GetFieldCount(), 1);
    ASSERT_FALSE(row->IsNullAt(0));
    auto result_row = row->GetRow(0, 4);

    ASSERT_FALSE(result_row->IsNullAt(0));
    ASSERT_EQ(result_row->GetLong(0), 1);
    ASSERT_FALSE(result_row->IsNullAt(1));
    ASSERT_EQ(result_row->GetLong(1), 3);
    ASSERT_FALSE(result_row->IsNullAt(2));
    ASSERT_EQ(result_row->GetLong(2), 2);
    ASSERT_FALSE(result_row->IsNullAt(3));
    ASSERT_EQ(result_row->GetLong(3), 5);
}

}  // namespace paimon::test
