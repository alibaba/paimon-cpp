/*
 * Copyright 2026-present Alibaba Inc.
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

#include "paimon/common/data/serializer/row_compacted_serializer.h"

#include <string>

#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/columnar/columnar_row.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
TEST(RowCompactedSerializerTest, TestSimple) {
    auto pool = GetDefaultPool();
    // prepare data
    std::shared_ptr<arrow::DataType> arrow_type = arrow::struct_({
        arrow::field("f1", arrow::boolean()),
        arrow::field("f2", arrow::int8()),
        arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int32()),
        arrow::field("f5", arrow::int64()),
        arrow::field("f6", arrow::float32()),
        arrow::field("f7", arrow::float64()),
        arrow::field("f8", arrow::utf8()),
        arrow::field("f9", arrow::binary()),
        arrow::field("f10", arrow::date32()),
        arrow::field("f11", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f12", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("f13", arrow::decimal128(5, 2)),
        arrow::field("f14", arrow::decimal128(30, 5)),
    });

    std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(arrow_type,
                                                                                    R"([
        [true, 0, 32767, 2147483647, 4294967295, 0.5, 1.141, "20250327", "banana", 2026, 1732603136054000054, 11, "55.02", "-123456789987654321.45678"],
        [null, null, null, null, null, null, null, null, null, null, null, null, null, null]
    ])")
                                              .ValueOrDie();
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array);
    ASSERT_OK_AND_ASSIGN(auto serializer,
                         RowCompactedSerializer::Create(arrow::schema(arrow_type->fields()), pool));
    {
        auto columnar_row = std::make_shared<ColumnarRow>(
            /*struct_array=*/nullptr, struct_array->fields(), pool, /*row_id=*/0);
        columnar_row->SetRowKind(RowKind::UpdateAfter());

        // serialize and deserialize
        ASSERT_OK_AND_ASSIGN(auto bytes, serializer->SerializeToBytes(*columnar_row));
        ASSERT_OK_AND_ASSIGN(auto de_row, serializer->Deserialize(bytes));

        // check result
        ASSERT_EQ(*(de_row->GetRowKind().value()), *(RowKind::UpdateAfter()));
        ASSERT_EQ(de_row->GetFieldCount(), 14);
        ASSERT_FALSE(de_row->IsNullAt(0));

        ASSERT_EQ(de_row->GetBoolean(0), true);
        ASSERT_EQ(de_row->GetByte(1), 0);
        ASSERT_EQ(de_row->GetShort(2), 32767);
        ASSERT_EQ(de_row->GetInt(3), 2147483647);
        ASSERT_EQ(de_row->GetLong(4), 4294967295l);
        ASSERT_EQ(de_row->GetFloat(5), 0.5);
        ASSERT_EQ(de_row->GetDouble(6), 1.141);
        ASSERT_EQ(de_row->GetString(7), BinaryString::FromString("20250327", pool.get()));
        auto f9_bytes = std::make_shared<Bytes>("banana", pool.get());
        ASSERT_EQ(*de_row->GetBinary(8), *f9_bytes);
        ASSERT_EQ(de_row->GetDate(9), 2026);
        ASSERT_EQ(de_row->GetTimestamp(10, 9),
                  Timestamp(/*millisecond=*/1732603136054ll, /*nano_of_millisecond*/ 54));
        ASSERT_EQ(de_row->GetTimestamp(11, 0),
                  Timestamp(/*millisecond=*/11000ll, /*nano_of_millisecond*/ 0));
        ASSERT_EQ(de_row->GetDecimal(12, 5, 2),
                  Decimal(5, 2, DecimalUtils::StrToInt128("5502").value()));
        ASSERT_EQ(de_row->GetDecimal(13, 30, 5),
                  Decimal(30, 5, DecimalUtils::StrToInt128("-12345678998765432145678").value()));

        // test compatibility
        std::vector<uint8_t> java_bytes = {
            2,   0,   0,  1,  0,  255, 127, 255, 255, 255, 127, 255, 255, 255, 255, 0,  0,   0,
            0,   0,   0,  0,  63, 168, 198, 75,  55,  137, 65,  242, 63,  8,   50,  48, 50,  53,
            48,  51,  50, 55, 6,  98,  97,  110, 97,  110, 97,  234, 7,   0,   0,   54, 200, 49,
            103, 147, 1,  0,  0,  54,  248, 42,  0,   0,   0,   0,   0,   0,   126, 21, 0,   0,
            0,   0,   0,  0,  10, 253, 98,  189, 73,  88,  213, 98,  56,  248, 242};
        std::vector<uint8_t> cpp_bytes(bytes->data(), bytes->data() + bytes->size());
        ASSERT_EQ(java_bytes, cpp_bytes);
    }
    {
        // all null
        auto columnar_row = std::make_shared<ColumnarRow>(
            /*struct_array=*/nullptr, struct_array->fields(), pool, /*row_id=*/1);
        columnar_row->SetRowKind(RowKind::UpdateAfter());

        // serialize and deserialize
        ASSERT_OK_AND_ASSIGN(auto bytes, serializer->SerializeToBytes(*columnar_row));
        ASSERT_OK_AND_ASSIGN(auto de_row, serializer->Deserialize(bytes));

        // check result
        ASSERT_EQ(*(de_row->GetRowKind().value()), *(RowKind::UpdateAfter()));
        ASSERT_EQ(de_row->GetFieldCount(), 14);
        ASSERT_TRUE(de_row->IsNullAt(0));

        // test compatibility
        std::vector<uint8_t> java_bytes = {2, 255, 63};
        std::vector<uint8_t> cpp_bytes(bytes->data(), bytes->data() + bytes->size());
        ASSERT_EQ(java_bytes, cpp_bytes);
    }
}

TEST(RowCompactedSerializerTest, TestNestedType) {
    auto pool = GetDefaultPool();
    // prepare data
    auto key_field = arrow::field("key", arrow::int32());
    auto value_field = arrow::field("value", arrow::int32());

    auto inner_child1 = arrow::field(
        "inner1", arrow::map(arrow::int32(), arrow::field("inner_list", arrow::list(value_field))));
    auto inner_child2 = arrow::field(
        "inner2", arrow::map(arrow::int32(),
                             arrow::field("inner_map", arrow::map(arrow::int32(), value_field))));
    auto inner_child3 = arrow::field(
        "inner3",
        arrow::map(arrow::int32(),
                   arrow::field("inner_struct", arrow::struct_({key_field, value_field}))));

    auto arrow_type = arrow::struct_({inner_child1, inner_child2, inner_child3});
    // each inner child per row
    std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(arrow_type,
                                                                                    R"([
[[[100, [1, 2, 3, 4]], [101, [5, 6, 7]]],
[[200, [[500, 1]]], [201, [[501, 2]]]],
[[600, [100, 200]]]]
    ])")
                                              .ValueOrDie();
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array);
    auto columnar_row =
        std::make_shared<ColumnarRow>(/*struct_array=*/nullptr, struct_array->fields(), pool,
                                      /*row_id=*/0);

    // serialize and deserialize
    ASSERT_OK_AND_ASSIGN(auto serializer,
                         RowCompactedSerializer::Create(arrow::schema(arrow_type->fields()), pool));
    ASSERT_OK_AND_ASSIGN(auto bytes, serializer->SerializeToBytes(*columnar_row));
    ASSERT_OK_AND_ASSIGN(auto row, serializer->Deserialize(bytes));

    // check result
    ASSERT_EQ(row->GetFieldCount(), 3);

    // for inner_child1
    ASSERT_EQ(row->GetMap(0)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({100, 101}));
    auto value1 = row->GetMap(0)->ValueArray();
    ASSERT_EQ(value1->Size(), 2);
    ASSERT_EQ(value1->GetArray(0)->ToIntArray().value(), std::vector<int32_t>({1, 2, 3, 4}));
    ASSERT_EQ(value1->GetArray(1)->ToIntArray().value(), std::vector<int32_t>({5, 6, 7}));

    // for inner_child2
    ASSERT_EQ(row->GetMap(1)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({200, 201}));
    auto value2 = row->GetMap(1)->ValueArray();
    ASSERT_EQ(value2->Size(), 2);
    ASSERT_EQ(value2->GetMap(0)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({500}));
    ASSERT_EQ(value2->GetMap(0)->ValueArray()->ToIntArray().value(), std::vector<int32_t>({1}));
    ASSERT_EQ(value2->GetMap(1)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({501}));
    ASSERT_EQ(value2->GetMap(1)->ValueArray()->ToIntArray().value(), std::vector<int32_t>({2}));

    // for inner_child3
    ASSERT_EQ(row->GetMap(2)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({600}));
    auto value3 = row->GetMap(2)->ValueArray();
    ASSERT_EQ(value3->Size(), 1);
    ASSERT_EQ(value3->GetRow(0, /*num_fields=*/2)->GetInt(0), 100);
    ASSERT_EQ(value3->GetRow(0, /*num_fields=*/2)->GetInt(1), 200);

    // test compatibility
    std::vector<uint8_t> java_bytes = {
        0,  0,   92,  16, 0,  0, 0, 2,   0,  0,   0, 0,  0, 0,  0,  100, 0,   0,  0,  101, 0,   0,
        0,  2,   0,   0,  0,  0, 0, 0,   0,  24,  0, 0,  0, 24, 0,  0,   0,   24, 0,  0,   0,   48,
        0,  0,   0,   4,  0,  0, 0, 0,   0,  0,   0, 1,  0, 0,  0,  2,   0,   0,  0,  3,   0,   0,
        0,  4,   0,   0,  0,  3, 0, 0,   0,  0,   0, 0,  0, 5,  0,  0,   0,   6,  0,  0,   0,   7,
        0,  0,   0,   0,  0,  0, 0, 124, 16, 0,   0, 0,  2, 0,  0,  0,   0,   0,  0,  0,   200, 0,
        0,  0,   201, 0,  0,  0, 2, 0,   0,  0,   0, 0,  0, 0,  36, 0,   0,   0,  24, 0,   0,   0,
        36, 0,   0,   0,  64, 0, 0, 0,   16, 0,   0, 0,  1, 0,  0,  0,   0,   0,  0,  0,   244, 1,
        0,  0,   0,   0,  0,  0, 1, 0,   0,  0,   0, 0,  0, 0,  1,  0,   0,   0,  0,  0,   0,   0,
        0,  0,   0,   0,  16, 0, 0, 0,   1,  0,   0, 0,  0, 0,  0,  0,   245, 1,  0,  0,   0,   0,
        0,  0,   1,   0,  0,  0, 0, 0,   0,  0,   2, 0,  0, 0,  0,  0,   0,   0,  0,  0,   0,   0,
        60, 16,  0,   0,  0,  1, 0, 0,   0,  0,   0, 0,  0, 88, 2,  0,   0,   0,  0,  0,   0,   1,
        0,  0,   0,   0,  0,  0, 0, 24,  0,  0,   0, 16, 0, 0,  0,  0,   0,   0,  0,  0,   0,   0,
        0,  100, 0,   0,  0,  0, 0, 0,   0,  200, 0, 0,  0, 0,  0,  0,   0};
    std::vector<uint8_t> cpp_bytes(bytes->data(), bytes->data() + bytes->size());
    ASSERT_EQ(java_bytes, cpp_bytes);
}

TEST(RowCompactedSerializerTest, TestNestedTypeWithNull) {
    auto pool = GetDefaultPool();
    // prepare data
    auto key_field = arrow::field("key", arrow::int32());
    auto value_field = arrow::field("value", arrow::int32());

    auto inner_child1 = arrow::field(
        "inner1", arrow::map(arrow::int32(), arrow::field("inner_list", arrow::list(value_field))));
    auto inner_child2 = arrow::field(
        "inner2", arrow::map(arrow::int32(),
                             arrow::field("inner_map", arrow::map(arrow::int32(), value_field))));
    auto inner_child3 = arrow::field(
        "inner3",
        arrow::map(arrow::int32(),
                   arrow::field("inner_struct", arrow::struct_({key_field, value_field}))));

    auto arrow_type = arrow::struct_({inner_child1, inner_child2, inner_child3});
    // each inner child per row
    std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(arrow_type,
                                                                                    R"([
[[[100, null], [101, [5, 6, null]]],
[[200, [[500, null]]], [201, [[501, 2]]]],
[[600, [100, null]]]]
    ])")
                                              .ValueOrDie();
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array);
    auto columnar_row =
        std::make_shared<ColumnarRow>(/*struct_array=*/nullptr, struct_array->fields(), pool,
                                      /*row_id=*/0);

    // serialize and deserialize
    ASSERT_OK_AND_ASSIGN(auto serializer,
                         RowCompactedSerializer::Create(arrow::schema(arrow_type->fields()), pool));
    ASSERT_OK_AND_ASSIGN(auto bytes, serializer->SerializeToBytes(*columnar_row));
    ASSERT_OK_AND_ASSIGN(auto row, serializer->Deserialize(bytes));

    // check result
    ASSERT_EQ(row->GetFieldCount(), 3);

    // for inner_child1
    ASSERT_EQ(row->GetMap(0)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({100, 101}));
    auto value1 = row->GetMap(0)->ValueArray();
    ASSERT_EQ(value1->Size(), 2);
    ASSERT_TRUE(value1->IsNullAt(0));
    ASSERT_EQ(value1->GetArray(1)->GetInt(0), 5);
    ASSERT_EQ(value1->GetArray(1)->GetInt(1), 6);
    ASSERT_TRUE(value1->GetArray(1)->IsNullAt(2));

    // for inner_child2
    ASSERT_EQ(row->GetMap(1)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({200, 201}));
    auto value2 = row->GetMap(1)->ValueArray();
    ASSERT_EQ(value2->Size(), 2);
    ASSERT_EQ(value2->GetMap(0)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({500}));
    ASSERT_TRUE(value2->GetMap(0)->ValueArray()->IsNullAt(0));
    ASSERT_EQ(value2->GetMap(1)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({501}));
    ASSERT_EQ(value2->GetMap(1)->ValueArray()->ToIntArray().value(), std::vector<int32_t>({2}));

    // for inner_child3
    ASSERT_EQ(row->GetMap(2)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({600}));
    auto value3 = row->GetMap(2)->ValueArray();
    ASSERT_EQ(value3->Size(), 1);
    ASSERT_EQ(value3->GetRow(0, /*num_fields=*/2)->GetInt(0), 100);
    ASSERT_TRUE(value3->GetRow(0, /*num_fields=*/2)->IsNullAt(1));

    // test compatibility
    std::vector<uint8_t> java_bytes = {
        0,   0, 68, 16, 0, 0,   0,  2, 0, 0,  0, 0,  0,  0, 0,   100, 0,  0,  0,   101, 0,  0,
        0,   2, 0,  0,  0, 1,   0,  0, 0, 0,  0, 0,  0,  0, 0,   0,   0,  24, 0,   0,   0,  24,
        0,   0, 0,  3,  0, 0,   0,  4, 0, 0,  0, 5,  0,  0, 0,   6,   0,  0,  0,   0,   0,  0,
        0,   0, 0,  0,  0, 124, 16, 0, 0, 0,  2, 0,  0,  0, 0,   0,   0,  0,  200, 0,   0,  0,
        201, 0, 0,  0,  2, 0,   0,  0, 0, 0,  0, 0,  36, 0, 0,   0,   24, 0,  0,   0,   36, 0,
        0,   0, 64, 0,  0, 0,   16, 0, 0, 0,  1, 0,  0,  0, 0,   0,   0,  0,  244, 1,   0,  0,
        0,   0, 0,  0,  1, 0,   0,  0, 1, 0,  0, 0,  0,  0, 0,   0,   0,  0,  0,   0,   0,  0,
        0,   0, 16, 0,  0, 0,   1,  0, 0, 0,  0, 0,  0,  0, 245, 1,   0,  0,  0,   0,   0,  0,
        1,   0, 0,  0,  0, 0,   0,  0, 2, 0,  0, 0,  0,  0, 0,   0,   0,  0,  0,   0,   60, 16,
        0,   0, 0,  1,  0, 0,   0,  0, 0, 0,  0, 88, 2,  0, 0,   0,   0,  0,  0,   1,   0,  0,
        0,   0, 0,  0,  0, 24,  0,  0, 0, 16, 0, 0,  0,  0, 2,   0,   0,  0,  0,   0,   0,  100,
        0,   0, 0,  0,  0, 0,   0,  0, 0, 0,  0, 0,  0,  0, 0};
    std::vector<uint8_t> cpp_bytes(bytes->data(), bytes->data() + bytes->size());
    ASSERT_EQ(java_bytes, cpp_bytes);
}

TEST(RowCompactedSerializerTest, TestNestedNullWithTimestampAndDecimal) {
    // test map/list with nested child types
    auto pool = GetDefaultPool();
    // prepare data
    auto inner_child1 = arrow::field(
        "inner1",
        arrow::map(
            arrow::int32(),
            arrow::field("inner_list", arrow::list(arrow::field("f0", arrow::decimal128(5, 2))))));
    auto inner_child2 = arrow::field(
        "inner2",
        arrow::map(
            arrow::int32(),
            arrow::field("inner_list", arrow::list(arrow::field("f1", arrow::decimal128(30, 5))))));
    auto inner_child3 = arrow::field(
        "inner3",
        arrow::map(arrow::int32(),
                   arrow::field("inner_struct",
                                arrow::struct_({
                                    arrow::field("f0", arrow::timestamp(arrow::TimeUnit::NANO)),
                                    arrow::field("f1", arrow::timestamp(arrow::TimeUnit::SECOND)),
                                    arrow::field("f2", arrow::decimal128(5, 2)),
                                    arrow::field("f3", arrow::decimal128(30, 5)),
                                }))));

    auto arrow_type = arrow::struct_({inner_child1, inner_child2, inner_child3});
    // each inner child per row
    std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(arrow_type,
                                                                                    R"([
[[[100, ["5.12", "6.12"]]],
[[200, ["-123456789987654321.45678", "23456789987654321.45678"]]],
[[300, [1732603136054000054, 11, "7.89", "3456789987654321.45678"]]]],
[[[102, [null, "6.12"]]],
[[202, ["-123456789987654321.45678", null]]],
[[302, [null, null, null, null]]]]
    ])")
                                              .ValueOrDie();

    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array);
    ASSERT_OK_AND_ASSIGN(auto serializer,
                         RowCompactedSerializer::Create(arrow::schema(arrow_type->fields()), pool));

    {
        auto columnar_row =
            std::make_shared<ColumnarRow>(/*struct_array=*/nullptr, struct_array->fields(), pool,
                                          /*row_id=*/0);

        // serialize and deserialize
        ASSERT_OK_AND_ASSIGN(auto bytes, serializer->SerializeToBytes(*columnar_row));
        ASSERT_OK_AND_ASSIGN(auto row, serializer->Deserialize(bytes));

        // check result
        ASSERT_EQ(row->GetFieldCount(), 3);

        // test compatibility
        std::vector<uint8_t> java_bytes = {
            0,   0,   60,  16,  0,   0,  0,   1,   0,   0,  0,   0,   0,   0,  0,   100, 0,   0,
            0,   0,   0,   0,   0,   1,  0,   0,   0,   0,  0,   0,   0,   24, 0,   0,   0,   16,
            0,   0,   0,   2,   0,   0,  0,   0,   0,   0,  0,   0,   2,   0,  0,   0,   0,   0,
            0,   100, 2,   0,   0,   0,  0,   0,   0,   92, 16,  0,   0,   0,  1,   0,   0,   0,
            0,   0,   0,   0,   200, 0,  0,   0,   0,   0,  0,   0,   1,   0,  0,   0,   0,   0,
            0,   0,   56,  0,   0,   0,  16,  0,   0,   0,  2,   0,   0,   0,  0,   0,   0,   0,
            10,  0,   0,   0,   24,  0,  0,   0,   9,   0,  0,   0,   40,  0,  0,   0,   253, 98,
            189, 73,  88,  213, 98,  56, 248, 242, 0,   0,  0,   0,   0,   0,  127, 40,  213, 221,
            111, 235, 135, 7,   14,  0,  0,   0,   0,   0,  0,   0,   100, 16, 0,   0,   0,   1,
            0,   0,   0,   0,   0,   0,  0,   44,  1,   0,  0,   0,   0,   0,  0,   1,   0,   0,
            0,   0,   0,   0,   0,   64, 0,   0,   0,   16, 0,   0,   0,   0,  0,   0,   0,   0,
            0,   0,   0,   54,  0,   0,  0,   40,  0,   0,  0,   248, 42,  0,  0,   0,   0,   0,
            0,   21,  3,   0,   0,   0,  0,   0,   0,   9,  0,   0,   0,   48, 0,   0,   0,   54,
            200, 49,  103, 147, 1,   0,  0,   18,  189, 66, 129, 228, 46,  71, 7,   14,  0,   0,
            0,   0,   0,   0,   0};
        std::vector<uint8_t> cpp_bytes(bytes->data(), bytes->data() + bytes->size());
        ASSERT_EQ(java_bytes, cpp_bytes);
    }
    {
        auto columnar_row =
            std::make_shared<ColumnarRow>(/*struct_array=*/nullptr, struct_array->fields(), pool,
                                          /*row_id=*/1);

        // serialize and deserialize
        ASSERT_OK_AND_ASSIGN(auto bytes, serializer->SerializeToBytes(*columnar_row));
        ASSERT_OK_AND_ASSIGN(auto row, serializer->Deserialize(bytes));

        // check result
        ASSERT_EQ(row->GetFieldCount(), 3);

        // test compatibility
        std::vector<uint8_t> java_bytes = {
            0,   0,  60, 16,  0,  0,  0,   1,   0,  0, 0,  0, 0,  0,   0,   102, 0, 0, 0, 0,   0,
            0,   0,  1,  0,   0,  0,  0,   0,   0,  0, 24, 0, 0,  0,   16,  0,   0, 0, 2, 0,   0,
            0,   1,  0,  0,   0,  0,  0,   0,   0,  0, 0,  0, 0,  100, 2,   0,   0, 0, 0, 0,   0,
            76,  16, 0,  0,   0,  1,  0,   0,   0,  0, 0,  0, 0,  202, 0,   0,   0, 0, 0, 0,   0,
            1,   0,  0,  0,   0,  0,  0,   0,   40, 0, 0,  0, 16, 0,   0,   0,   2, 0, 0, 0,   2,
            0,   0,  0,  10,  0,  0,  0,   24,  0,  0, 0,  0, 0,  0,   0,   0,   0, 0, 0, 253, 98,
            189, 73, 88, 213, 98, 56, 248, 242, 0,  0, 0,  0, 0,  0,   100, 16,  0, 0, 0, 1,   0,
            0,   0,  0,  0,   0,  0,  46,  1,   0,  0, 0,  0, 0,  0,   1,   0,   0, 0, 0, 0,   0,
            0,   64, 0,  0,   0,  16, 0,   0,   0,  0, 15, 0, 0,  0,   0,   0,   0, 0, 0, 0,   0,
            40,  0,  0,  0,   0,  0,  0,   0,   0,  0, 0,  0, 0,  0,   0,   0,   0, 0, 0, 0,   0,
            0,   0,  0,  48,  0,  0,  0,   0,   0,  0, 0,  0, 0,  0,   0,   0,   0, 0, 0, 0,   0,
            0,   0,  0,  0,   0,  0,  0,   0,   0,  0};
        std::vector<uint8_t> cpp_bytes(bytes->data(), bytes->data() + bytes->size());
        ASSERT_EQ(java_bytes, cpp_bytes);
    }
}
TEST(RowCompactedSerializerTest, TestNestedNullWithTimestampAndDecimal2) {
    // test struct with nested child types
    auto pool = GetDefaultPool();
    // prepare data
    auto inner_child1 = arrow::field(
        "inner1",
        arrow::map(
            arrow::int32(),
            arrow::field("inner_list", arrow::list(arrow::field("f0", arrow::decimal128(5, 2))))));
    auto inner_child2 = arrow::field(
        "inner2",
        arrow::map(
            arrow::int32(),
            arrow::field("inner_list", arrow::list(arrow::field("f1", arrow::decimal128(30, 5))))));
    auto inner_child3 = arrow::field(
        "inner3",
        arrow::map(arrow::int32(),
                   arrow::field("inner_struct",
                                arrow::struct_({
                                    arrow::field("f0", arrow::timestamp(arrow::TimeUnit::NANO)),
                                    arrow::field("f1", arrow::timestamp(arrow::TimeUnit::SECOND)),
                                    arrow::field("f2", arrow::decimal128(5, 2)),
                                    arrow::field("f3", arrow::decimal128(30, 5)),
                                }))));
    auto child1 =
        arrow::field("child1", arrow::struct_({inner_child1, inner_child2, inner_child3}));
    auto child2 = arrow::field("child2", arrow::utf8());
    auto arrow_type = arrow::struct_({child1, child2});
    // each inner child per row
    std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(arrow_type,
                                                                                    R"([
[[[[100, ["5.12", "6.12"]]],
[[200, ["-123456789987654321.45678", "23456789987654321.45678"]]],
[[300, [1732603136054000054, 11, "7.89", "3456789987654321.45678"]]]],
"Alice"],
[[[[102, [null, "6.12"]]],
[[202, ["-123456789987654321.45678", null]]],
[[302, [null, null, null, null]]]],
"Bob"]
    ])")
                                              .ValueOrDie();

    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array);
    ASSERT_OK_AND_ASSIGN(auto serializer,
                         RowCompactedSerializer::Create(arrow::schema(arrow_type->fields()), pool));

    {
        auto columnar_row =
            std::make_shared<ColumnarRow>(/*struct_array=*/nullptr, struct_array->fields(), pool,
                                          /*row_id=*/0);

        // serialize and deserialize
        ASSERT_OK_AND_ASSIGN(auto bytes, serializer->SerializeToBytes(*columnar_row));
        ASSERT_OK_AND_ASSIGN(auto row, serializer->Deserialize(bytes));

        // check result
        ASSERT_EQ(row->GetFieldCount(), 2);
        auto inner_row = row->GetRow(0, 3);
        ASSERT_EQ(inner_row->GetFieldCount(), 3);
        ASSERT_EQ(row->GetString(1).ToString(), "Alice");

        // test compatibility
        std::vector<uint8_t> java_bytes = {
            0,   0,   129, 2,   0,   0,   60,  16,  0,   0,  0,   1,   0,   0,  0,   0,   0,   0,
            0,   100, 0,   0,   0,   0,   0,   0,   0,   1,  0,   0,   0,   0,  0,   0,   0,   24,
            0,   0,   0,   16,  0,   0,   0,   2,   0,   0,  0,   0,   0,   0,  0,   0,   2,   0,
            0,   0,   0,   0,   0,   100, 2,   0,   0,   0,  0,   0,   0,   92, 16,  0,   0,   0,
            1,   0,   0,   0,   0,   0,   0,   0,   200, 0,  0,   0,   0,   0,  0,   0,   1,   0,
            0,   0,   0,   0,   0,   0,   56,  0,   0,   0,  16,  0,   0,   0,  2,   0,   0,   0,
            0,   0,   0,   0,   10,  0,   0,   0,   24,  0,  0,   0,   9,   0,  0,   0,   40,  0,
            0,   0,   253, 98,  189, 73,  88,  213, 98,  56, 248, 242, 0,   0,  0,   0,   0,   0,
            127, 40,  213, 221, 111, 235, 135, 7,   14,  0,  0,   0,   0,   0,  0,   0,   100, 16,
            0,   0,   0,   1,   0,   0,   0,   0,   0,   0,  0,   44,  1,   0,  0,   0,   0,   0,
            0,   1,   0,   0,   0,   0,   0,   0,   0,   64, 0,   0,   0,   16, 0,   0,   0,   0,
            0,   0,   0,   0,   0,   0,   0,   54,  0,   0,  0,   40,  0,   0,  0,   248, 42,  0,
            0,   0,   0,   0,   0,   21,  3,   0,   0,   0,  0,   0,   0,   9,  0,   0,   0,   48,
            0,   0,   0,   54,  200, 49,  103, 147, 1,   0,  0,   18,  189, 66, 129, 228, 46,  71,
            7,   14,  0,   0,   0,   0,   0,   0,   0,   5,  65,  108, 105, 99, 101};
        std::vector<uint8_t> cpp_bytes(bytes->data(), bytes->data() + bytes->size());
        ASSERT_EQ(java_bytes, cpp_bytes);
    }
    {
        auto columnar_row =
            std::make_shared<ColumnarRow>(/*struct_array=*/nullptr, struct_array->fields(), pool,
                                          /*row_id=*/1);

        // serialize and deserialize
        ASSERT_OK_AND_ASSIGN(auto bytes, serializer->SerializeToBytes(*columnar_row));
        ASSERT_OK_AND_ASSIGN(auto row, serializer->Deserialize(bytes));

        // check result
        ASSERT_EQ(row->GetFieldCount(), 2);
        auto inner_row = row->GetRow(0, 3);
        ASSERT_EQ(inner_row->GetFieldCount(), 3);
        ASSERT_EQ(row->GetString(1).ToString(), "Bob");

        // test compatibility
        std::vector<uint8_t> java_bytes = {
            0, 0, 241, 1,  0,   0,  60, 16,  0,  0,  0,   1,   0,  0, 0,  0,  0,   0,   0,   102, 0,
            0, 0, 0,   0,  0,   0,  1,  0,   0,  0,  0,   0,   0,  0, 24, 0,  0,   0,   16,  0,   0,
            0, 2, 0,   0,  0,   1,  0,  0,   0,  0,  0,   0,   0,  0, 0,  0,  0,   100, 2,   0,   0,
            0, 0, 0,   0,  76,  16, 0,  0,   0,  1,  0,   0,   0,  0, 0,  0,  0,   202, 0,   0,   0,
            0, 0, 0,   0,  1,   0,  0,  0,   0,  0,  0,   0,   40, 0, 0,  0,  16,  0,   0,   0,   2,
            0, 0, 0,   2,  0,   0,  0,  10,  0,  0,  0,   24,  0,  0, 0,  0,  0,   0,   0,   0,   0,
            0, 0, 253, 98, 189, 73, 88, 213, 98, 56, 248, 242, 0,  0, 0,  0,  0,   0,   100, 16,  0,
            0, 0, 1,   0,  0,   0,  0,  0,   0,  0,  46,  1,   0,  0, 0,  0,  0,   0,   1,   0,   0,
            0, 0, 0,   0,  0,   64, 0,  0,   0,  16, 0,   0,   0,  0, 15, 0,  0,   0,   0,   0,   0,
            0, 0, 0,   0,  40,  0,  0,  0,   0,  0,  0,   0,   0,  0, 0,  0,  0,   0,   0,   0,   0,
            0, 0, 0,   0,  0,   0,  0,  48,  0,  0,  0,   0,   0,  0, 0,  0,  0,   0,   0,   0,   0,
            0, 0, 0,   0,  0,   0,  0,  0,   0,  0,  0,   0,   0,  0, 3,  66, 111, 98};
        std::vector<uint8_t> cpp_bytes(bytes->data(), bytes->data() + bytes->size());
        ASSERT_EQ(java_bytes, cpp_bytes);
    }
}

TEST(RowCompactedSerializerTest, TestListType) {
    auto pool = GetDefaultPool();
    // prepare data
    auto inner_child1 = arrow::field("inner1", arrow::list(arrow::int32()));
    auto arrow_type = arrow::struct_({inner_child1});
    // each inner child per row
    std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(arrow_type,
                                                                                    R"([
[[5, 6, 7]],
[[1, 2, 3]],
[[4]]
    ])")
                                              .ValueOrDie();
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array);
    auto columnar_row =
        std::make_shared<ColumnarRow>(/*struct_array=*/nullptr, struct_array->fields(), pool,
                                      /*row_id=*/0);

    // serialize and deserialize
    ASSERT_OK_AND_ASSIGN(auto serializer,
                         RowCompactedSerializer::Create(arrow::schema(arrow_type->fields()), pool));
    ASSERT_OK_AND_ASSIGN(auto bytes, serializer->SerializeToBytes(*columnar_row));
    ASSERT_OK_AND_ASSIGN(auto row, serializer->Deserialize(bytes));

    // check result
    ASSERT_EQ(row->GetFieldCount(), 1);

    // for inner_child1
    ASSERT_EQ(row->GetArray(0)->ToIntArray().value(), std::vector<int32_t>({5, 6, 7}));
}

TEST(RowCompactedSerializerTest, TestSliceComparator) {
    auto pool = GetDefaultPool();
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::boolean()),
        arrow::field("f2", arrow::int8()),
        arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int32()),
        arrow::field("f5", arrow::int64()),
        arrow::field("f6", arrow::float32()),
        arrow::field("f7", arrow::float64()),
        arrow::field("f8", arrow::utf8()),
        arrow::field("f9", arrow::binary()),
        arrow::field("f10", arrow::date32()),
        arrow::field("f11", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f12", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("f13", arrow::decimal128(5, 2)),
        arrow::field("f14", arrow::decimal128(30, 5)),
    };
    auto check_result = [&](const std::shared_ptr<arrow::DataType>& type,
                            const std::shared_ptr<arrow::Array>& array) {
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
        ASSERT_TRUE(struct_array);
        ASSERT_OK_AND_ASSIGN(auto serializer,
                             RowCompactedSerializer::Create(arrow::schema(type->fields()), pool));
        auto columnar_row1 = std::make_shared<ColumnarRow>(
            /*struct_array=*/nullptr, struct_array->fields(), pool, /*row_id=*/0);
        auto columnar_row2 = std::make_shared<ColumnarRow>(
            /*struct_array=*/nullptr, struct_array->fields(), pool, /*row_id=*/1);

        ASSERT_OK_AND_ASSIGN(auto bytes1, serializer->SerializeToBytes(*columnar_row1));
        auto slice1 = MemorySlice::Wrap(bytes1);
        ASSERT_OK_AND_ASSIGN(auto bytes2, serializer->SerializeToBytes(*columnar_row2));
        auto slice2 = MemorySlice::Wrap(bytes2);

        ASSERT_OK_AND_ASSIGN(auto comparator, RowCompactedSerializer::CreateSliceComparator(
                                                  arrow::schema(type->fields()), pool));
        ASSERT_EQ(-1, comparator(slice1, slice2).value());
        ASSERT_EQ(1, comparator(slice2, slice1).value());
        ASSERT_EQ(0, comparator(slice1, slice1).value());
        ASSERT_EQ(0, comparator(slice2, slice2).value());
    };
    // test various type
    {
        auto type = arrow::struct_({fields[0], fields[1], fields[2], fields[3]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [false, 0, 32767, 2147483647],
        [true, 0, 32767, 2147483647]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[0], fields[1], fields[2], fields[3]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [true, 0, 32767, 2147483647],
        [true, 1, 32767, 2147483647]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[0], fields[1], fields[2], fields[3]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [true, 0, 32766, 2147483647],
        [true, 0, 32767, 2147483647]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[0], fields[1], fields[2], fields[3]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [true, 0, 32767, 2147483646],
        [true, 0, 32767, 2147483647]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[4], fields[5], fields[6]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [4294967295, 10.1, 100.123],
        [4294967296, 10.1, 100.123]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[4], fields[5], fields[6]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [4294967295, 10.1, 100.123],
        [4294967295, 10.11, 100.123]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[4], fields[5], fields[6]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [4294967295, 10.1, 100.123],
        [4294967295, 10.1, 100.124]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[7], fields[8], fields[9]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        ["Alice", "这是一个中文", 10],
        ["Bob", "这是一个中文", 10]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[7], fields[8], fields[9]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        ["Alice", "这是一个中文", 10],
        ["Alice", "这是一个中文！", 10]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[7], fields[8], fields[9]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        ["Alice", "这是一个中文", 10],
        ["Alice", "这是一个中文", 20]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[10], fields[11], fields[12], fields[13]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [1732603136054000054, 11, "55.02", "-123456789987654321.45678"],
        [1732603136054000055, 11, "55.02", "-123456789987654321.45678"]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[10], fields[11], fields[12], fields[13]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [1732603136054000054, 11, "55.02", "-123456789987654321.45678"],
        [1732603136054000054, 12, "55.02", "-123456789987654321.45678"]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[10], fields[11], fields[12], fields[13]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [1732603136054000054, 11, "55.02", "-123456789987654321.45678"],
        [1732603136054000054, 11, "55.03", "-123456789987654321.45678"]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[10], fields[11], fields[12], fields[13]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [1732603136054000054, 11, "55.02", "-123456789987654321.45678"],
        [1732603136054000054, 11, "55.02", "-123456789987654321.45670"]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    // test null
    {
        auto type = arrow::struct_({fields[0], fields[1]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [false, null],
        [false, 20]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[0], fields[1]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [null, 20],
        [null, 21]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[0], fields[1]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [false, null],
        [true, 20]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[0], fields[1]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [null, 21],
        [false, 20]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[0], fields[1]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [null, null],
        [null, 20]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    // test float & double
    // -infinity < -0.0 < +0.0 < +infinity < NaN == NaN
    {
        auto type = arrow::struct_({fields[5], fields[5], fields[5], fields[6], fields[6]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [-Inf, -0.0, 0.0, Inf, NaN],
        [-0.0, -0.0, 0.0, Inf, NaN]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[5], fields[5], fields[5], fields[6], fields[6]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [-Inf, -0.0, 0.0, Inf, NaN],
        [-Inf, 0.0, 0.0, Inf, NaN]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[5], fields[5], fields[5], fields[6], fields[6]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [-Inf, -0.0, 0.0, 0.0, NaN],
        [-Inf, -0.0, 0.0, Inf, NaN]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
    {
        auto type = arrow::struct_({fields[5], fields[5], fields[5], fields[6], fields[6]});
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(type,
                                                                                        R"([
        [-Inf, -0.0, 0.0, Inf, Inf],
        [-Inf, -0.0, 0.0, Inf, NaN]
    ])")
                                                  .ValueOrDie();
        check_result(type, array);
    }
}

}  // namespace paimon::test
