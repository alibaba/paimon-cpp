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

#include "paimon/catalog/identifier.h"

#include <stdexcept>

#include "gtest/gtest.h"

namespace paimon::test {

TEST(IdentifierTest, ConstructorAndGetters) {
    Identifier id("test_db", "test_table");
    EXPECT_EQ(id.GetDatabaseName(), "test_db");
    EXPECT_EQ(id.GetTableName(), "test_table");
}

TEST(IdentifierTest, SingleArgumentConstructorUsesUnknownDatabase) {
    Identifier id("test_table");
    EXPECT_EQ(id.GetDatabaseName(), Identifier::kUnknownDatabase);
    EXPECT_EQ(id.GetTableName(), "test_table");
}

TEST(IdentifierTest, EqualityOperator) {
    Identifier id1("db1", "table1");
    Identifier id2("db1", "table1");
    Identifier id3("db2", "table2");

    EXPECT_TRUE(id1 == id2);
    EXPECT_FALSE(id1 == id3);
}

TEST(IdentifierTest, ToString) {
    Identifier id("my_db", "my_table");
    EXPECT_EQ(id.ToString(), "Identifier{database='my_db', table='my_table'}");
}

TEST(IdentifierTest, EmptyDatabaseRemainsEmpty) {
    Identifier id("", "my_table");
    EXPECT_EQ(id.GetDatabaseName(), "");
    EXPECT_EQ(id.GetTableName(), "my_table");
}

TEST(IdentifierTest, ParseSystemTable) {
    Identifier id("db", "tbl$options");
    EXPECT_EQ(id.GetTableName(), "tbl$options");
    EXPECT_EQ(id.GetDataTableName(), "tbl");
    EXPECT_FALSE(id.GetBranchName());
    ASSERT_TRUE(id.GetSystemTableName());
    EXPECT_EQ(id.GetSystemTableName().value(), "options");
    EXPECT_TRUE(id.IsSystemTable());
}

TEST(IdentifierTest, ParseBranchTable) {
    Identifier id("db", "tbl$branch_dev");
    EXPECT_EQ(id.GetDataTableName(), "tbl");
    ASSERT_TRUE(id.GetBranchName());
    EXPECT_EQ(id.GetBranchName().value(), "dev");
    EXPECT_EQ(id.GetBranchNameOrDefault(), "dev");
    EXPECT_FALSE(id.GetSystemTableName());
    EXPECT_FALSE(id.IsSystemTable());
}

TEST(IdentifierTest, ParseBranchSystemTable) {
    Identifier id("db", "tbl$branch_dev$options");
    EXPECT_EQ(id.GetDataTableName(), "tbl");
    ASSERT_TRUE(id.GetBranchName());
    EXPECT_EQ(id.GetBranchName().value(), "dev");
    ASSERT_TRUE(id.GetSystemTableName());
    EXPECT_EQ(id.GetSystemTableName().value(), "options");
    EXPECT_TRUE(id.IsSystemTable());
}

TEST(IdentifierTest, InvalidSystemTableName) {
    Identifier invalid_middle("db", "tbl$bad$options");
    EXPECT_THROW(invalid_middle.IsSystemTable(), std::invalid_argument);

    Identifier too_many("db", "tbl$branch_dev$options$extra");
    EXPECT_THROW(too_many.IsSystemTable(), std::invalid_argument);
}

}  // namespace paimon::test
