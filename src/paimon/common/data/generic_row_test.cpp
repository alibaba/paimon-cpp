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

#include "paimon/common/data/generic_row.h"

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/columnar/columnar_map.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"

namespace paimon::test {

TEST(GenericRowTest, TestSimple) {
    auto pool = GetDefaultPool();
    GenericRow row(16);
    row.SetField(0, true);
    row.SetField(1, static_cast<char>(1));
    row.SetField(2, static_cast<int16_t>(2));
    row.SetField(3, static_cast<int32_t>(3));
    row.SetField(4, static_cast<int64_t>(4));
    row.SetField(5, static_cast<float>(5.1));
    row.SetField(6, 6.12);
    auto str = BinaryString::FromString("abcd", pool.get());
    row.SetField(7, str);
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes("efgh", pool.get());
    row.SetField(8, bytes);
    std::string str9 = "apple";
    row.SetField(9, std::string_view(str9.data(), str9.size()));

    Timestamp ts(100, 20);
    row.SetField(10, ts);
    Decimal decimal(/*precision=*/30, /*scale=*/20,
                    DecimalUtils::StrToInt128("12345678998765432145678").value());
    row.SetField(11, decimal);

    auto array = std::make_shared<BinaryArray>(BinaryArray::FromLongArray(
        {static_cast<int64_t>(10), static_cast<int64_t>(20)}, pool.get()));
    row.SetField(12, array);

    std::shared_ptr<InternalRow> binary_row =
        BinaryRowGenerator::GenerateRowPtr({100, 200}, pool.get());
    row.SetField(13, binary_row);

    auto key = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 3]").ValueOrDie();
    auto value =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[2, 4, 6]").ValueOrDie();
    auto map = std::make_shared<ColumnarMap>(key, value, pool, /*offset=*/0, /*length=*/3);
    row.SetField(14, map);
    // do not set value at pos 15, therefore, pos 15 is null

    ASSERT_EQ(row.GetFieldCount(), 16);
    ASSERT_EQ(row.GetRowKind().value(), RowKind::Insert());
    row.SetRowKind(RowKind::Delete());
    ASSERT_EQ(row.GetRowKind().value(), RowKind::Delete());

    ASSERT_TRUE(row == row);
    // test get
    ASSERT_FALSE(row.IsNullAt(0));
    ASSERT_EQ(row.GetBoolean(0), true);
    ASSERT_EQ(row.GetByte(1), static_cast<char>(1));
    ASSERT_EQ(row.GetShort(2), static_cast<int16_t>(2));
    ASSERT_EQ(row.GetInt(3), static_cast<int32_t>(3));
    ASSERT_EQ(row.GetDate(3), static_cast<int32_t>(3));
    ASSERT_EQ(row.GetLong(4), static_cast<int64_t>(4));
    ASSERT_EQ(row.GetFloat(5), static_cast<float>(5.1));
    ASSERT_EQ(row.GetDouble(6), static_cast<double>(6.12));
    ASSERT_EQ(row.GetString(7), str);
    ASSERT_EQ(*row.GetBinary(8), *bytes);
    ASSERT_EQ(std::string(row.GetStringView(9)), str9);
    ASSERT_EQ(row.GetTimestamp(10, /*precision=*/9), ts);
    ASSERT_EQ(row.GetDecimal(11, /*precision=*/30, /*scale=*/20), decimal);
    ASSERT_EQ(row.GetArray(12)->ToLongArray().value(), array->ToLongArray().value());
    auto binary_row_result = std::dynamic_pointer_cast<BinaryRow>(row.GetRow(13, 2));
    auto binary_row_expected = std::dynamic_pointer_cast<BinaryRow>(binary_row);
    ASSERT_EQ(*binary_row_result, *binary_row_expected);
    ASSERT_EQ(row.GetMap(14)->KeyArray()->ToIntArray().value(),
              map->KeyArray()->ToIntArray().value());
    ASSERT_EQ(row.GetMap(14)->ValueArray()->ToLongArray().value(),
              map->ValueArray()->ToLongArray().value());
    ASSERT_TRUE(row.IsNullAt(15));
    ASSERT_EQ(
        "(true,1,2,3,4,5.100000,6.120000,abcd,efgh,apple,1970-01-01 "
        "00:00:00.100000020,123.45678998765432145678,array,row,map,null)",
        row.ToString());
}
}  // namespace paimon::test
