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

#include "paimon/core/casting/casted_row.h"

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/columnar/columnar_row.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(CastedRowTest, TestSimpleWithNoCasting) {
    auto pool = GetDefaultPool();

    std::vector<DataField> fields = {DataField(0, arrow::field("f0", arrow::boolean())),
                                     DataField(1, arrow::field("f1", arrow::int8())),
                                     DataField(2, arrow::field("f2", arrow::int16())),
                                     DataField(3, arrow::field("f3", arrow::int32())),
                                     DataField(4, arrow::field("field_null", arrow::int32())),
                                     DataField(5, arrow::field("f4", arrow::int64())),
                                     DataField(6, arrow::field("f5", arrow::float32())),
                                     DataField(7, arrow::field("f6", arrow::float64())),
                                     DataField(8, arrow::field("f7", arrow::utf8())),
                                     DataField(9, arrow::field("f8", arrow::binary()))};
    auto arrow_type = DataField::ConvertDataFieldsToArrowStructType(fields);

    std::string data =
        R"([[true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141592659, "2025-03-27", "banana"],
            [true, -2, -32768, -2147483648, null, -4294967298, 2.0, 3.141592657, "2025-03-26", "mouse"]])";
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow_type, data).ValueOrDie();
    ASSERT_TRUE(array);
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array);
    auto row = std::make_shared<ColumnarRow>(struct_array->fields(), pool, /*row_id=*/0);

    // all null cast executor
    std::vector<std::shared_ptr<CastExecutor>> cast_executors(fields.size(), nullptr);
    ASSERT_OK_AND_ASSIGN(auto casted_row, CastedRow::Create(cast_executors, /*src_fields=*/fields,
                                                            /*target_fields=*/fields, row));
    ASSERT_EQ(casted_row->GetFieldCount(), 10);
    ASSERT_EQ(casted_row->GetRowKind().value(), RowKind::Insert());
    ASSERT_EQ(casted_row->GetBoolean(0), true);
    ASSERT_EQ(casted_row->GetByte(1), static_cast<char>(0));
    ASSERT_EQ(casted_row->GetShort(2), static_cast<int16_t>(32767));
    ASSERT_FALSE(casted_row->IsNullAt(3));
    ASSERT_EQ(casted_row->GetInt(3), static_cast<int32_t>(2147483647));
    ASSERT_TRUE(casted_row->IsNullAt(4));
    ASSERT_EQ(casted_row->GetLong(5), static_cast<int64_t>(4294967295));
    ASSERT_EQ(casted_row->GetFloat(6), static_cast<float>(0.5));
    ASSERT_EQ(casted_row->GetDouble(7), static_cast<double>(1.141592659));
    ASSERT_EQ(casted_row->GetString(8).ToString(), "2025-03-27");
    ASSERT_EQ(*casted_row->GetBinary(9), Bytes("banana", pool.get()));

    ASSERT_EQ("casted row, inner row = ColumnarRow, row_id 0", casted_row->ToString());
}

TEST(CastedRowTest, TestSimpleWithCasting) {
    auto pool = GetDefaultPool();

    std::vector<DataField> fields = {
        DataField(0, arrow::field("f0", arrow::boolean())),
        DataField(1, arrow::field("f1", arrow::int8())),
        DataField(2, arrow::field("f2", arrow::int16())),
        DataField(3, arrow::field("f3", arrow::int32())),
        DataField(4, arrow::field("field_null", arrow::int32())),
        DataField(5, arrow::field("f4", arrow::int64())),
        DataField(6, arrow::field("f5", arrow::float32())),
        DataField(7, arrow::field("f6", arrow::float64())),
        DataField(8, arrow::field("f7", arrow::timestamp(arrow::TimeUnit::SECOND))),
        DataField(9, arrow::field("f8", arrow::binary())),
        DataField(10, arrow::field("f9", arrow::int16()))};
    auto arrow_type = DataField::ConvertDataFieldsToArrowStructType(fields);

    std::string data =
        R"([[true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141592659, "2025-03-27", "banana", 5],
            [true, -2, -32768, -2147483648, null, -4294967298, 2.0, 3.141592657, "2025-03-26", "mouse", 2]])";
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow_type, data).ValueOrDie();
    ASSERT_TRUE(array);
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array);
    auto row = std::make_shared<ColumnarRow>(struct_array->fields(), pool, /*row_id=*/0);

    // create cast executor
    std::vector<DataField> target_fields = {
        DataField(0, arrow::field("f0", arrow::utf8())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f2", arrow::utf8())),
        DataField(3, arrow::field("f3", arrow::utf8())),
        DataField(4, arrow::field("field_null", arrow::utf8())),
        DataField(5, arrow::field("f4", arrow::utf8())),
        DataField(6, arrow::field("f5", arrow::utf8())),
        DataField(7, arrow::field("f6", arrow::utf8())),
        DataField(8, arrow::field("f7", arrow::timestamp(arrow::TimeUnit::SECOND))),
        DataField(9, arrow::field("f8", arrow::utf8())),
        DataField(10, arrow::field("f9", arrow::int8()))};

    ASSERT_OK_AND_ASSIGN(auto cast_executors,
                         FieldMappingBuilder::CreateDataCastExecutors(target_fields, fields));
    ASSERT_OK_AND_ASSIGN(auto casted_row, CastedRow::Create(cast_executors, /*src_fields=*/fields,
                                                            /*target_fields=*/target_fields, row));
    ASSERT_EQ(casted_row->GetFieldCount(), 11);
    ASSERT_EQ(casted_row->GetRowKind().value(), RowKind::Insert());
    ASSERT_EQ(casted_row->GetString(0).ToString(), "true");
    ASSERT_EQ(casted_row->GetString(1).ToString(), "0");
    ASSERT_EQ(casted_row->GetString(2).ToString(), "32767");
    ASSERT_FALSE(casted_row->IsNullAt(3));
    ASSERT_EQ(casted_row->GetString(3).ToString(), "2147483647");
    ASSERT_TRUE(casted_row->IsNullAt(4));
    ASSERT_EQ(casted_row->GetString(5).ToString(), "4294967295");
    ASSERT_EQ(casted_row->GetString(6).ToString(), "0.5");
    ASSERT_EQ(casted_row->GetString(7).ToString(), "1.141592659");
    ASSERT_EQ(casted_row->GetTimestamp(8, /*precision=*/0), Timestamp(1743033600000l, 0l))
        << casted_row->GetTimestamp(8, /*precision=*/0).ToString();
    ASSERT_EQ(casted_row->GetString(9).ToString(), "banana");
    ASSERT_EQ(casted_row->GetByte(10), 5);

    ASSERT_EQ("casted row, inner row = ColumnarRow, row_id 0", casted_row->ToString());
}

TEST(CastedRowTest, TestNestedTypeWithCasting) {
    auto pool = GetDefaultPool();
    std::vector<DataField> fields = {
        DataField(0, arrow::field("f1", arrow::map(arrow::int8(), arrow::int16()))),
        DataField(1, arrow::field("f2", arrow::list(arrow::float32()))),
        DataField(2, arrow::field("f3", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                                        arrow::field("f1", arrow::int64())}))),
        DataField(3, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai"))),
        DataField(4, arrow::field("f5", arrow::date32())),
        DataField(5, arrow::field("f6", arrow::decimal128(2, 2)))};

    auto arrow_type = DataField::ConvertDataFieldsToArrowStructType(fields);
    std::string data = R"([
        [[[10, 20]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [[[11, 64], [12, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.123123", 24, "0.78"]
    ])";

    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow_type, data).ValueOrDie();
    ASSERT_TRUE(array);
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array);
    auto row = std::make_shared<ColumnarRow>(struct_array->fields(), pool, /*row_id=*/0);

    // create cast executor
    std::vector<DataField> target_fields = {
        fields[0],
        fields[1],
        fields[2],
        DataField(3, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai"))),
        DataField(4, arrow::field("f5", arrow::utf8())),
        DataField(5, arrow::field("f6", arrow::utf8()))};

    ASSERT_OK_AND_ASSIGN(auto cast_executors,
                         FieldMappingBuilder::CreateDataCastExecutors(target_fields, fields));
    ASSERT_OK_AND_ASSIGN(auto casted_row, CastedRow::Create(cast_executors, /*src_fields=*/fields,
                                                            /*target_fields=*/target_fields, row));
    ASSERT_EQ(casted_row->GetFieldCount(), 6);
    ASSERT_EQ(casted_row->GetRowKind().value(), RowKind::Insert());

    ASSERT_EQ(casted_row->GetMap(0)->KeyArray()->ToByteArray().value(), std::vector<char>({10}));
    ASSERT_EQ(casted_row->GetMap(0)->ValueArray()->ToShortArray().value(),
              std::vector<int16_t>({20}));

    ASSERT_EQ(casted_row->GetArray(1)->ToFloatArray().value(), std::vector<float>({0.1, 0.2}));

    auto inner_row = casted_row->GetRow(2, 2);
    ASSERT_EQ(inner_row->GetBoolean(0), true);
    ASSERT_EQ(inner_row->GetLong(1), 2l);

    ASSERT_FALSE(casted_row->IsNullAt(3));
    ASSERT_EQ(casted_row->GetTimestamp(3, /*precision=*/9).ToString(),
              "1970-01-01 00:02:03.123123000");
    ASSERT_EQ(casted_row->GetString(4).ToString(), "1976-09-22");
    ASSERT_EQ(casted_row->GetString(5).ToString(), "0.22");

    ASSERT_EQ("casted row, inner row = ColumnarRow, row_id 0", casted_row->ToString());
}

TEST(CastedRowTest, TestInvalidCast) {
    auto pool = GetDefaultPool();

    std::vector<DataField> fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                     DataField(1, arrow::field("f1", arrow::utf8())),
                                     DataField(2, arrow::field("f2", arrow::utf8()))};
    auto arrow_type = DataField::ConvertDataFieldsToArrowStructType(fields);

    std::string data = R"([["apple", "noo", "2024-11-21T09:91:56.1"]])";
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow_type, data).ValueOrDie();
    ASSERT_TRUE(array);
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array);
    auto row = std::make_shared<ColumnarRow>(struct_array->fields(), pool, /*row_id=*/0);

    // create cast executor
    std::vector<DataField> target_fields = {
        DataField(0, arrow::field("f0", arrow::binary())),
        DataField(1, arrow::field("f1", arrow::boolean())),
        DataField(2, arrow::field("f2", arrow::timestamp(arrow::TimeUnit::NANO)))};

    ASSERT_OK_AND_ASSIGN(auto cast_executors,
                         FieldMappingBuilder::CreateDataCastExecutors(target_fields, fields));
    ASSERT_OK_AND_ASSIGN(auto casted_row, CastedRow::Create(cast_executors, /*src_fields=*/fields,
                                                            /*target_fields=*/target_fields, row));
    ASSERT_EQ(casted_row->GetFieldCount(), 3);
    ASSERT_EQ(casted_row->GetRowKind().value(), RowKind::Insert());

    Bytes f0_bytes("apple", pool.get());
    ASSERT_EQ(*casted_row->GetBinary(0), f0_bytes);
    ASSERT_THROW(casted_row->GetBoolean(1), std::invalid_argument);
    ASSERT_THROW(casted_row->GetTimestamp(2, 9), std::invalid_argument);
}

TEST(CastedRowTest, TestInvalidCastedRowCreate) {
    std::vector<DataField> fields = {DataField(0, arrow::field("f0", arrow::utf8()))};
    // cast_executors.size() != fields.size()
    ASSERT_NOK_WITH_MSG(
        CastedRow::Create(/*cast_executors=*/{}, /*src_fields=*/fields,
                          /*target_fields=*/fields, nullptr),
        "CastedRow create failed, src_fields & target_fields & cast_executors & row size mismatch");
}

}  // namespace paimon::test
