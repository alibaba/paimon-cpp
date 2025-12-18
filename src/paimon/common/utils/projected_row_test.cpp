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

#include "paimon/common/utils/projected_row.h"

#include <utility>
#include <variant>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/columnar/columnar_map.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"

namespace paimon::test {
TEST(ProjectedRowTest, TestSimple) {
    auto pool = GetDefaultPool();
    auto row = std::make_shared<GenericRow>(16);
    row->SetField(0, true);
    row->SetField(1, static_cast<char>(1));
    row->SetField(2, static_cast<int16_t>(2));
    row->SetField(3, static_cast<int32_t>(3));
    row->SetField(4, static_cast<int64_t>(4));
    row->SetField(5, static_cast<float>(5.1));
    row->SetField(6, 6.12);
    auto str = BinaryString::FromString("abcd", pool.get());
    row->SetField(7, str);
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes("efgh", pool.get());
    row->SetField(8, bytes);
    std::string str9 = "apple";
    row->SetField(9, std::string_view(str9.data(), str9.size()));

    Timestamp ts(100, 20);
    row->SetField(10, ts);
    Decimal decimal(/*precision=*/30, /*scale=*/20,
                    DecimalUtils::StrToInt128("12345678998765432145678").value());
    row->SetField(11, decimal);

    auto array = std::make_shared<BinaryArray>(BinaryArray::FromLongArray(
        {static_cast<int64_t>(10), static_cast<int64_t>(20)}, pool.get()));
    row->SetField(12, array);

    std::shared_ptr<InternalRow> binary_row =
        BinaryRowGenerator::GenerateRowPtr({100, 200}, pool.get());
    row->SetField(13, binary_row);

    auto key = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 3]").ValueOrDie();
    auto value =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[2, 4, 6]").ValueOrDie();
    auto map = std::make_shared<ColumnarMap>(key, value, pool, /*offset=*/0, /*length=*/3);
    row->SetField(14, map);
    // do not set value at pos 15, therefore, pos 15 is null

    std::vector<int32_t> mapping = {15, 14, 13, 12, 11, 10, 9,  8,  7,  6,
                                    5,  4,  3,  2,  1,  0,  -1, -1, -1, -1};

    ProjectedRow projected_row(row, mapping);
    ASSERT_EQ(projected_row.GetFieldCount(), 20);
    ASSERT_EQ(projected_row.GetRowKind().value(), RowKind::Insert());

    ASSERT_TRUE(projected_row.IsNullAt(19));
    ASSERT_TRUE(projected_row.IsNullAt(18));
    ASSERT_TRUE(projected_row.IsNullAt(17));
    ASSERT_TRUE(projected_row.IsNullAt(16));

    ASSERT_EQ(projected_row.GetBoolean(15), true);
    ASSERT_EQ(projected_row.GetByte(14), static_cast<char>(1));
    ASSERT_EQ(projected_row.GetShort(13), static_cast<int16_t>(2));
    ASSERT_EQ(projected_row.GetInt(12), static_cast<int32_t>(3));
    ASSERT_EQ(projected_row.GetDate(12), static_cast<int32_t>(3));
    ASSERT_EQ(projected_row.GetLong(11), static_cast<int64_t>(4));
    ASSERT_EQ(projected_row.GetFloat(10), static_cast<float>(5.1));
    ASSERT_EQ(projected_row.GetDouble(9), static_cast<double>(6.12));
    ASSERT_EQ(projected_row.GetString(8), str);
    ASSERT_EQ(*projected_row.GetBinary(7), *bytes);
    ASSERT_EQ(std::string(projected_row.GetStringView(6)), str9);
    ASSERT_EQ(projected_row.GetTimestamp(5, /*precision=*/9), ts);
    ASSERT_EQ(projected_row.GetDecimal(4, /*precision=*/30, /*scale=*/20), decimal);
    ASSERT_EQ(projected_row.GetArray(3)->ToLongArray().value(), array->ToLongArray().value());

    auto binary_row_result = std::dynamic_pointer_cast<BinaryRow>(projected_row.GetRow(2, 2));
    auto binary_row_expected = std::dynamic_pointer_cast<BinaryRow>(binary_row);
    ASSERT_EQ(*binary_row_result, *binary_row_expected);

    ASSERT_EQ(projected_row.GetMap(1)->KeyArray()->ToIntArray().value(),
              map->KeyArray()->ToIntArray().value());
    ASSERT_EQ(projected_row.GetMap(1)->ValueArray()->ToLongArray().value(),
              map->ValueArray()->ToLongArray().value());

    ASSERT_TRUE(projected_row.IsNullAt(0));

    ASSERT_EQ(
        "+I { indexMapping=15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, -1, -1, -1, -1, "
        "mutableRow=(true,1,2,3,4,5.100000,6.120000,abcd,efgh,apple,1970-01-01 "
        "00:00:00.100000020,123.45678998765432145678,array,row,map,null) }",
        projected_row.ToString());
}
}  // namespace paimon::test
