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

#include "paimon/common/data/serializer/binary_serializer_utils.h"

#include <string>

#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/columnar/columnar_row.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
TEST(BinarySerializerUtilsTest, TestSimple) {
    auto pool = GetDefaultPool();
    // prepare data
    std::shared_ptr<arrow::DataType> arrow_type =
        arrow::struct_({arrow::field("f1", arrow::boolean()), arrow::field("f2", arrow::int8()),
                        arrow::field("f3", arrow::int16()), arrow::field("f4", arrow::int32()),
                        arrow::field("field_null", arrow::int32()),
                        arrow::field("f5", arrow::int64()), arrow::field("f6", arrow::float32()),
                        arrow::field("f7", arrow::float64()), arrow::field("f8", arrow::utf8()),
                        arrow::field("f9", arrow::binary()), arrow::field("f10", arrow::date32()),
                        arrow::field("f11", arrow::timestamp(arrow::TimeUnit::NANO)),
                        arrow::field("f12", arrow::decimal128(5, 2))});

    std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(arrow_type,
                                                                                    R"([
        [true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141, "20250327", "banana", 2026, 5000001, "5.12"]
    ])")
                                              .ValueOrDie();
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array);
    auto columnar_row = std::make_shared<ColumnarRow>(/*struct_array=*/nullptr,
                                                      struct_array->fields(), pool, /*row_id=*/0);

    ASSERT_OK_AND_ASSIGN(auto binary_row, BinarySerializerUtils::WriteBinaryRow(
                                              columnar_row, arrow_type, pool.get()));

    // check result
    ASSERT_EQ(binary_row->GetFieldCount(), 13);
    ASSERT_FALSE(binary_row->IsNullAt(0));

    ASSERT_EQ(binary_row->GetBoolean(0), true);
    ASSERT_EQ(binary_row->GetByte(1), 0);
    ASSERT_EQ(binary_row->GetShort(2), 32767);
    ASSERT_EQ(binary_row->GetInt(3), 2147483647);
    ASSERT_TRUE(binary_row->IsNullAt(4));
    ASSERT_EQ(binary_row->GetLong(5), 4294967295l);
    ASSERT_EQ(binary_row->GetFloat(6), 0.5);
    ASSERT_EQ(binary_row->GetDouble(7), 1.141);
    ASSERT_EQ(binary_row->GetString(8), BinaryString::FromString("20250327", pool.get()));
    auto f9_bytes = std::make_shared<Bytes>("banana", pool.get());
    ASSERT_EQ(*binary_row->GetBinary(9), *f9_bytes);
    ASSERT_EQ(binary_row->GetDate(10), 2026);
    ASSERT_EQ(binary_row->GetTimestamp(11, 9),
              Timestamp(/*millisecond=*/5, /*nano_of_millisecond*/ 1));
    ASSERT_EQ(binary_row->GetDecimal(12, 5, 2),
              Decimal(5, 2, DecimalUtils::StrToInt128("512").value()));
}

TEST(BinarySerializerUtilsTest, TestNestedType) {
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

    ASSERT_OK_AND_ASSIGN(auto binary_row, BinarySerializerUtils::WriteBinaryRow(
                                              columnar_row, arrow_type, pool.get()));

    // check result
    ASSERT_EQ(binary_row->GetFieldCount(), 3);

    // for inner_child1
    ASSERT_EQ(binary_row->GetMap(0)->KeyArray()->ToIntArray().value(),
              std::vector<int32_t>({100, 101}));
    auto value1 = binary_row->GetMap(0)->ValueArray();
    ASSERT_EQ(value1->Size(), 2);
    ASSERT_EQ(value1->GetArray(0)->ToIntArray().value(), std::vector<int32_t>({1, 2, 3, 4}));
    ASSERT_EQ(value1->GetArray(1)->ToIntArray().value(), std::vector<int32_t>({5, 6, 7}));

    // for inner_child2
    ASSERT_EQ(binary_row->GetMap(1)->KeyArray()->ToIntArray().value(),
              std::vector<int32_t>({200, 201}));
    auto value2 = binary_row->GetMap(1)->ValueArray();
    ASSERT_EQ(value2->Size(), 2);
    ASSERT_EQ(value2->GetMap(0)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({500}));
    ASSERT_EQ(value2->GetMap(0)->ValueArray()->ToIntArray().value(), std::vector<int32_t>({1}));
    ASSERT_EQ(value2->GetMap(1)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({501}));
    ASSERT_EQ(value2->GetMap(1)->ValueArray()->ToIntArray().value(), std::vector<int32_t>({2}));

    // for inner_child3
    ASSERT_EQ(binary_row->GetMap(2)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({600}));
    auto value3 = binary_row->GetMap(2)->ValueArray();
    ASSERT_EQ(value3->Size(), 1);
    ASSERT_EQ(value3->GetRow(0, /*num_fields=*/2)->GetInt(0), 100);
    ASSERT_EQ(value3->GetRow(0, /*num_fields=*/2)->GetInt(1), 200);
}

TEST(BinarySerializerUtilsTest, TestNestedTypeWithNull) {
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

    ASSERT_OK_AND_ASSIGN(auto binary_row, BinarySerializerUtils::WriteBinaryRow(
                                              columnar_row, arrow_type, pool.get()));

    // check result
    ASSERT_EQ(binary_row->GetFieldCount(), 3);

    // for inner_child1
    ASSERT_EQ(binary_row->GetMap(0)->KeyArray()->ToIntArray().value(),
              std::vector<int32_t>({100, 101}));
    auto value1 = binary_row->GetMap(0)->ValueArray();
    ASSERT_EQ(value1->Size(), 2);
    ASSERT_TRUE(value1->IsNullAt(0));
    ASSERT_EQ(value1->GetArray(1)->GetInt(0), 5);
    ASSERT_EQ(value1->GetArray(1)->GetInt(1), 6);
    ASSERT_TRUE(value1->GetArray(1)->IsNullAt(2));

    // for inner_child2
    ASSERT_EQ(binary_row->GetMap(1)->KeyArray()->ToIntArray().value(),
              std::vector<int32_t>({200, 201}));
    auto value2 = binary_row->GetMap(1)->ValueArray();
    ASSERT_EQ(value2->Size(), 2);
    ASSERT_EQ(value2->GetMap(0)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({500}));
    ASSERT_TRUE(value2->GetMap(0)->ValueArray()->IsNullAt(0));
    ASSERT_EQ(value2->GetMap(1)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({501}));
    ASSERT_EQ(value2->GetMap(1)->ValueArray()->ToIntArray().value(), std::vector<int32_t>({2}));

    // for inner_child3
    ASSERT_EQ(binary_row->GetMap(2)->KeyArray()->ToIntArray().value(), std::vector<int32_t>({600}));
    auto value3 = binary_row->GetMap(2)->ValueArray();
    ASSERT_EQ(value3->Size(), 1);
    ASSERT_EQ(value3->GetRow(0, /*num_fields=*/2)->GetInt(0), 100);
    ASSERT_TRUE(value3->GetRow(0, /*num_fields=*/2)->IsNullAt(1));
}

}  // namespace paimon::test
