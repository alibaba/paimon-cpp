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

#include "paimon/common/reader/data_evolution_row.h"

#include <variant>

#include "gtest/gtest.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(DataEvolutionRowTest, TestSimple) {
    // f0:int32
    // f1:int32
    // f2:string
    // f3:int32
    // f4:string
    // f5:int32
    std::vector<int32_t> row_offsets = {0, 2, 0, 1, 2, 1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0};

    auto pool = GetDefaultPool();
    BinaryRow row1 = BinaryRowGenerator::GenerateRow({0, std::string("00")}, pool.get());
    BinaryRow row2 = BinaryRowGenerator::GenerateRow({10, 110}, pool.get());
    BinaryRow row3 = BinaryRowGenerator::GenerateRow({20, std::string("20")}, pool.get());

    DataEvolutionRow row({row1, row2, row3}, row_offsets, field_offsets);
    ASSERT_EQ(row.GetFieldCount(), 6);
    ASSERT_FALSE(row.IsNullAt(0));
    ASSERT_EQ(row.GetInt(0), 0);
    ASSERT_EQ(row.GetInt(1), 20);
    ASSERT_EQ(row.GetString(2).ToString(), "00");
    ASSERT_EQ(row.GetInt(3), 110);
    ASSERT_EQ(row.GetString(4).ToString(), "20");
    ASSERT_EQ(row.GetInt(5), 10);

    // test set and get row kind
    ASSERT_OK_AND_ASSIGN(const RowKind* row_kind, row.GetRowKind());
    ASSERT_EQ(*row_kind, *RowKind::Insert());
    row.SetRowKind(RowKind::UpdateAfter());
    ASSERT_OK_AND_ASSIGN(const RowKind* new_row_kind, row.GetRowKind());
    ASSERT_EQ(*new_row_kind, *RowKind::UpdateAfter());
    ASSERT_EQ(row.ToString(), "DataEvolutionRow");
}

TEST(DataEvolutionRowTest, TestNull) {
    // f0:int32
    // f1:int32
    // f2:string
    // f3:int32
    // f4:string
    // f5:int32
    // f6:non-exist
    std::vector<int32_t> row_offsets = {0, 2, 0, 1, 2, 1, -1, -2};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0, -1, -1};

    auto pool = GetDefaultPool();
    BinaryRow row1 = BinaryRowGenerator::GenerateRow({0, std::string("00")}, pool.get());
    BinaryRow row2 = BinaryRowGenerator::GenerateRow({10, 110}, pool.get());
    BinaryRow row3 = BinaryRowGenerator::GenerateRow({20, NullType()}, pool.get());

    DataEvolutionRow row({row1, row2, row3}, row_offsets, field_offsets);
    ASSERT_EQ(row.GetFieldCount(), 8);
    ASSERT_FALSE(row.IsNullAt(0));
    ASSERT_EQ(row.GetInt(0), 0);
    ASSERT_EQ(row.GetInt(1), 20);
    ASSERT_EQ(row.GetString(2).ToString(), "00");
    ASSERT_EQ(row.GetInt(3), 110);
    ASSERT_TRUE(row.IsNullAt(4));
    ASSERT_EQ(row.GetInt(5), 10);
    ASSERT_TRUE(row.IsNullAt(6));
    ASSERT_TRUE(row.IsNullAt(7));
}

}  // namespace paimon::test
