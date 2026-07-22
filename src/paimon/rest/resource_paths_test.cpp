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

#include "paimon/rest/resource_paths.h"

#include "gtest/gtest.h"

namespace paimon::test {

TEST(ResourcePathsTest, WithPrefix) {
    ResourcePaths paths("my prefix");
    ASSERT_EQ("/v1/config", ResourcePaths::Config());
    ASSERT_EQ("/v1/my+prefix/databases", paths.Databases());
    ASSERT_EQ("/v1/my+prefix/databases/db%231", paths.Database("db#1"));
    ASSERT_EQ("/v1/my+prefix/databases/db/tables", paths.Tables("db"));
    ASSERT_EQ("/v1/my+prefix/databases/db/tables/t1", paths.Table("db", "t1"));
    ASSERT_EQ("/v1/my+prefix/tables/rename", paths.RenameTable());
    ASSERT_EQ("/v1/my+prefix/databases/db/tables/t1/snapshots", paths.Snapshots("db", "t1"));
}

TEST(ResourcePathsTest, WithoutPrefix) {
    ResourcePaths paths("");
    ASSERT_EQ("/v1/databases", paths.Databases());
    ASSERT_EQ("/v1/tables/rename", paths.RenameTable());
}

}  // namespace paimon::test
