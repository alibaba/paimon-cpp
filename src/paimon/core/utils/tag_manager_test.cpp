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

#include "paimon/core/utils/tag_manager.h"

#include "gtest/gtest.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(TagManagerTest, TestGet) {
    ASSERT_OK_AND_ASSIGN(auto tag_opt,
                         TagManager(std::make_shared<LocalFileSystem>(),
                                    paimon::test::GetDataDir() +
                                        "/orc/append_table_with_tag.db/append_table_with_tag")
                             .Get("1"));
    ASSERT_TRUE(tag_opt.has_value());
    const auto& tag = *tag_opt;
    ASSERT_EQ(std::vector<int64_t>({2026, 2, 4, 6, 8, 10, 12}), tag.TagCreateTime());
    ASSERT_EQ(3.0, tag.TagTimeRetained());
}

TEST(TagManagerTest, TestTagPath) {
    ASSERT_EQ("/root/tag/tag-data", TagManager(nullptr, "/root").TagPath("data"));
    ASSERT_EQ("/root/branch/branch-data/tag/tag-data",
              TagManager(nullptr, "/root", "data").TagPath("data"));
}

}  // namespace paimon::test
