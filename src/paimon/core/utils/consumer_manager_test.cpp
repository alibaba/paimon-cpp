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

#include "paimon/core/utils/consumer_manager.h"

#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(ConsumerManagerTest, TestBranchConsumerPath) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = std::make_shared<LocalFileSystem>();
    std::string table_path = PathUtil::JoinPath(dir->Str(), "table");
    std::string consumer_dir =
        PathUtil::JoinPath(BranchManager::BranchPath(table_path, "dev"), "consumer");
    ASSERT_OK(fs->Mkdirs(consumer_dir));
    ASSERT_OK(fs->WriteFile(PathUtil::JoinPath(consumer_dir, "consumer-c1"),
                            R"({"nextSnapshot":42})", /*overwrite=*/true));

    ConsumerManager manager(fs, table_path, "dev");
    ASSERT_EQ(manager.ConsumerDirectory(), consumer_dir);
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> consumers, manager.ListConsumers());
    ASSERT_EQ(consumers, (std::vector<std::string>{"c1"}));
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> next_snapshot_id, manager.GetNextSnapshotId("c1"));
    ASSERT_EQ(next_snapshot_id, 42);
    ASSERT_OK_AND_ASSIGN(auto all_consumers, manager.Consumers());
    ASSERT_EQ(all_consumers, (std::map<std::string, int64_t>{{"c1", 42}}));
}

}  // namespace paimon::test
