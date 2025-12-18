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

#include "paimon/core/utils/offset_row.h"

#include <utility>
#include <variant>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"

namespace paimon::test {
TEST(OffsetRowTest, TestSimple) {
    auto pool = GetDefaultPool().get();
    auto bytes = std::make_shared<Bytes>("world", pool);
    Timestamp ts(/*millisecond=*/1000, /*nano_of_millisecond=*/10);
    Decimal decimal(/*precision=*/20, /*scale=*/3, 1234567l);
    auto inner_row = BinaryRowGenerator::GenerateRow(
        {static_cast<int8_t>(0), static_cast<int8_t>(1), static_cast<int16_t>(11),
         static_cast<int32_t>(111), static_cast<int64_t>(1111), static_cast<float>(12.3), 12.34,
         false, std::string("hello"), bytes, TimestampType(ts, Timestamp::MAX_PRECISION), decimal,
         NullType()},
        pool);
    OffsetRow row(inner_row, /*arity=*/12, /*offset=*/1);
    ASSERT_EQ(row.GetRowKind().value(), RowKind::Insert());
    ASSERT_EQ(row.GetFieldCount(), 12);
    ASSERT_FALSE(row.IsNullAt(0));
    ASSERT_EQ(row.GetByte(0), static_cast<char>(1));
    ASSERT_EQ(row.GetShort(1), static_cast<int16_t>(11));
    ASSERT_EQ(row.GetInt(2), static_cast<int32_t>(111));
    ASSERT_EQ(row.GetDate(2), static_cast<int32_t>(111));
    ASSERT_EQ(row.GetLong(3), static_cast<int64_t>(1111));
    ASSERT_EQ(row.GetFloat(4), static_cast<float>(12.3));
    ASSERT_EQ(row.GetDouble(5), static_cast<double>(12.34));
    ASSERT_EQ(row.GetBoolean(6), false);
    ASSERT_EQ(row.GetString(7).ToString(), "hello");
    ASSERT_EQ(*row.GetBinary(8), *bytes);
    ASSERT_EQ(row.GetTimestamp(9, Timestamp::MAX_PRECISION), ts);
    ASSERT_EQ(row.GetDecimal(10, /*precision=*/20, /*scale=*/3), decimal);
    ASSERT_TRUE(row.IsNullAt(11));
    ASSERT_EQ(row.ToString(), "OffsetRow, arity 12, offset 1");
}

}  // namespace paimon::test
