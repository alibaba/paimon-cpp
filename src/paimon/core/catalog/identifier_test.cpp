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

}  // namespace paimon::test
