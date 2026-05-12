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

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(IdentifierTest, ConstructorAndGetters) {
    Identifier id("test_db", "test_table");
    ASSERT_EQ(id.GetDatabaseName(), "test_db");
    ASSERT_EQ(id.GetTableName(), "test_table");
}

TEST(IdentifierTest, SingleArgumentConstructorUsesUnknownDatabase) {
    Identifier id("test_table");
    ASSERT_EQ(id.GetDatabaseName(), Identifier::kUnknownDatabase);
    ASSERT_EQ(id.GetTableName(), "test_table");
}

TEST(IdentifierTest, EqualityOperator) {
    Identifier id1("db1", "table1");
    Identifier id2("db1", "table1");
    Identifier id3("db2", "table2");

    ASSERT_TRUE(id1 == id2);
    ASSERT_FALSE(id1 == id3);
}

TEST(IdentifierTest, ToString) {
    Identifier id("my_db", "my_table");
    ASSERT_EQ(id.ToString(), "Identifier{database='my_db', table='my_table'}");
}

TEST(IdentifierTest, EmptyDatabaseRemainsEmpty) {
    Identifier id("", "my_table");
    ASSERT_EQ(id.GetDatabaseName(), "");
    ASSERT_EQ(id.GetTableName(), "my_table");
}

TEST(IdentifierTest, ParseDataTable) {
    Identifier id("db", "tbl");
    ASSERT_EQ(id.GetTableName(), "tbl");
    ASSERT_OK_AND_ASSIGN(std::string data_table_name, id.GetDataTableName());
    ASSERT_EQ(data_table_name, "tbl");
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> branch_name, id.GetBranchName());
    ASSERT_FALSE(branch_name);
    ASSERT_OK_AND_ASSIGN(std::string branch_name_or_default, id.GetBranchNameOrDefault());
    ASSERT_EQ(branch_name_or_default, Identifier::kDefaultMainBranch);
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> system_table_name, id.GetSystemTableName());
    ASSERT_FALSE(system_table_name);
    ASSERT_OK_AND_ASSIGN(bool is_system_table, id.IsSystemTable());
    ASSERT_FALSE(is_system_table);
}

TEST(IdentifierTest, ParseSystemTable) {
    Identifier id("db", "tbl$options");
    ASSERT_EQ(id.GetTableName(), "tbl$options");
    ASSERT_OK_AND_ASSIGN(std::string data_table_name, id.GetDataTableName());
    ASSERT_EQ(data_table_name, "tbl");
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> branch_name, id.GetBranchName());
    ASSERT_FALSE(branch_name);
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> system_table_name, id.GetSystemTableName());
    ASSERT_TRUE(system_table_name);
    ASSERT_EQ(system_table_name.value(), "options");
    ASSERT_OK_AND_ASSIGN(bool is_system_table, id.IsSystemTable());
    ASSERT_TRUE(is_system_table);
}

TEST(IdentifierTest, ParseBranchTable) {
    Identifier id("db", "tbl$branch_dev");
    ASSERT_OK_AND_ASSIGN(std::string data_table_name, id.GetDataTableName());
    ASSERT_EQ(data_table_name, "tbl");
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> branch_name, id.GetBranchName());
    ASSERT_TRUE(branch_name);
    ASSERT_EQ(branch_name.value(), "dev");
    ASSERT_OK_AND_ASSIGN(std::string branch_name_or_default, id.GetBranchNameOrDefault());
    ASSERT_EQ(branch_name_or_default, "dev");
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> system_table_name, id.GetSystemTableName());
    ASSERT_FALSE(system_table_name);
    ASSERT_OK_AND_ASSIGN(bool is_system_table, id.IsSystemTable());
    ASSERT_FALSE(is_system_table);
}

TEST(IdentifierTest, ParseBranchSystemTable) {
    Identifier id("db", "tbl$branch_dev$options");
    ASSERT_OK_AND_ASSIGN(std::string data_table_name, id.GetDataTableName());
    ASSERT_EQ(data_table_name, "tbl");
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> branch_name, id.GetBranchName());
    ASSERT_TRUE(branch_name);
    ASSERT_EQ(branch_name.value(), "dev");
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> system_table_name, id.GetSystemTableName());
    ASSERT_TRUE(system_table_name);
    ASSERT_EQ(system_table_name.value(), "options");
    ASSERT_OK_AND_ASSIGN(bool is_system_table, id.IsSystemTable());
    ASSERT_TRUE(is_system_table);
}

TEST(IdentifierTest, InvalidSystemTableName) {
    Identifier invalid_middle("db", "tbl$bad$options");
    ASSERT_NOK_WITH_MSG(invalid_middle.IsSystemTable(),
                        "System table can only contain one '$' separator");

    Identifier too_many("db", "tbl$branch_dev$options$extra");
    ASSERT_NOK_WITH_MSG(too_many.IsSystemTable(), "Invalid table name");
}

TEST(IdentifierTest, InvalidEmptySystemTableNameParts) {
    ASSERT_NOK_WITH_MSG(Identifier("db", "$options").IsSystemTable(), "Invalid table name");
    ASSERT_NOK_WITH_MSG(Identifier("db", "tbl$").IsSystemTable(), "Invalid table name");
    ASSERT_NOK_WITH_MSG(Identifier("db", "tbl$branch_").IsSystemTable(), "Invalid table name");
    ASSERT_NOK_WITH_MSG(Identifier("db", "tbl$branch_dev$").IsSystemTable(), "Invalid table name");
    ASSERT_NOK_WITH_MSG(Identifier("db", "tbl$$options").IsSystemTable(),
                        "System table can only contain one '$' separator");
}

}  // namespace paimon::test
