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

#include "paimon/core/utils/snapshot_manager.h"

#include <filesystem>
#include <limits>

#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(SnapshotManagerTest, TestSnapshotDirectory) {
    auto fs = std::make_shared<LocalFileSystem>();
    SnapshotManager manager(fs, paimon::test::GetDataDir() + "/append_09.db/append_09");
    ASSERT_EQ(manager.SnapshotDirectory(),
              paimon::test::GetDataDir() + "/append_09.db/append_09/snapshot");
    ASSERT_EQ(manager.Branch(), BranchManager::DEFAULT_MAIN_BRANCH);
}

TEST(SnapshotManagerTest, TestSnapshotDirectoryWithBranch) {
    auto fs = std::make_shared<LocalFileSystem>();
    SnapshotManager manager(
        fs,
        paimon::test::GetDataDir() + "/append_table_with_rt_branch.db/append_table_with_rt_branch",
        /*branch=*/"rt");
    ASSERT_EQ(manager.SnapshotDirectory(),
              paimon::test::GetDataDir() +
                  "/append_table_with_rt_branch.db/append_table_with_rt_branch/branch/branch-rt/"
                  "snapshot");
    ASSERT_EQ(manager.Branch(), "rt");
}

TEST(SnapshotManagerTest, TestSimple) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id1, mgr.EarliestSnapshotId());
    ASSERT_EQ(id1.value(), 1);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id2, mgr.LatestSnapshotId());
    ASSERT_EQ(id2.value(), 5);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.LatestSnapshotOfUser("b02e4322-9c5f-41e1-a560-c0156fdf7b9c"));
    ASSERT_EQ(snapshot.value().CommitUser(), "b02e4322-9c5f-41e1-a560-c0156fdf7b9c");
    ASSERT_EQ(snapshot.value().CommitIdentifier(), 9223372036854775807);
}

TEST(SnapshotManagerTest, TestSimpleWithBranch) {
    std::string test_data_path = paimon::test::GetDataDir() +
                                 "/orc/append_table_with_rt_branch.db/append_table_with_rt_branch";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path, /*branch=*/"rt");
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id1, mgr.EarliestSnapshotId());
    ASSERT_EQ(id1.value(), 1);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id2, mgr.LatestSnapshotId());
    ASSERT_EQ(id2.value(), 1);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.LatestSnapshotOfUser("884df499-c17a-4c78-a865-6fbbfea02f0b"));
    ASSERT_EQ(snapshot.value().CommitUser(), "884df499-c17a-4c78-a865-6fbbfea02f0b");
    ASSERT_EQ(snapshot.value().CommitIdentifier(), 1);
}

TEST(SnapshotManagerTest, TestGetAllSnapshots) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK_AND_ASSIGN(std::vector<Snapshot> snapshots, mgr.GetAllSnapshots());
    ASSERT_EQ(snapshots.size(), 5u);
}

TEST(SnapshotManagerTest, TestGetAllSnapshotsWithBranch) {
    std::string test_data_path =
        paimon::test::GetDataDir() +
        "/orc/append_table_with_append_pt_branch.db/append_table_with_append_pt_branch";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path, /*branch=*/"test");
    ASSERT_OK_AND_ASSIGN(std::vector<Snapshot> snapshots, mgr.GetAllSnapshots());
    ASSERT_EQ(snapshots.size(), 2u);
}

TEST(SnapshotManagerTest, TestGetNonSnapshotFiles) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string table_path = dir->Str();
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));

    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, table_path);
    ASSERT_OK(file_system->Mkdirs(PathUtil::JoinPath(table_path, "snapshot/orphan_dir")));
    ASSERT_OK(file_system->WriteFile(PathUtil::JoinPath(table_path, "snapshot/orphan_file"),
                                     "orphan", true));
    auto check_result = [](const std::set<std::string>& actual,
                           const std::set<std::string>& expected) -> bool {
        std::set<std::string> file_names;
        for (const auto& file_path : actual) {
            file_names.insert(PathUtil::GetName(file_path));
        }
        return file_names == expected;
    };
    ASSERT_OK_AND_ASSIGN(std::set<std::string> non_snapshot_files,
                         mgr.TryGetNonSnapshotFiles(std::numeric_limits<int64_t>::max()));
    ASSERT_TRUE(check_result(non_snapshot_files, {"orphan_dir", "orphan_file"}));
    ASSERT_OK_AND_ASSIGN(non_snapshot_files,
                         mgr.TryGetNonSnapshotFiles(std::numeric_limits<int64_t>::min()));
    ASSERT_EQ(non_snapshot_files.size(), 0);
}

TEST(SnapshotManagerTest, TestPathNotExist) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/not_exist";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id1, mgr.EarliestSnapshotId());
    ASSERT_EQ(id1, std::nullopt);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id2, mgr.LatestSnapshotId());
    ASSERT_EQ(id2, std::nullopt);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.LatestSnapshotOfUser("b02e4322-9c5f-41e1-a560-c0156fdf7b9c"));
    ASSERT_EQ(snapshot, std::nullopt);
}

TEST(SnapshotManagerTest, TestEarlierOrEqualTimeMillisExactMatch) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // snapshot-3 has timeMillis = 1721614515032
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierOrEqualTimeMillis(1721614515032));
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(snapshot->Id(), 3);
}

TEST(SnapshotManagerTest, TestEarlierOrEqualTimeMillisBetweenSnapshots) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // Between snapshot-3 (1721614515032) and snapshot-4 (1721615035363), should return snapshot-3
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierOrEqualTimeMillis(1721614600000));
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(snapshot->Id(), 3);
}

TEST(SnapshotManagerTest, TestEarlierOrEqualTimeMillisBeforeAll) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // Before snapshot-1 (1721614343270), should return nullopt
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierOrEqualTimeMillis(1721614343269));
    ASSERT_FALSE(snapshot.has_value());
}

TEST(SnapshotManagerTest, TestEarlierOrEqualTimeMillisAfterAll) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // After snapshot-5 (1721615035453), should return snapshot-5
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierOrEqualTimeMillis(1721615099999));
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(snapshot->Id(), 5);
}

TEST(SnapshotManagerTest, TestEarlierOrEqualTimeMillisFirstSnapshot) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // Exact match on snapshot-1
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierOrEqualTimeMillis(1721614343270));
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(snapshot->Id(), 1);
}

TEST(SnapshotManagerTest, TestEarlierOrEqualTimeMillisNoSnapshots) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/not_exist";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierOrEqualTimeMillis(1721614343270));
    ASSERT_FALSE(snapshot.has_value());
}

TEST(SnapshotManagerTest, TestEarlierThanTimeMillisExactMatch) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // Exact match on snapshot-1 (1721614343270) should not include snapshot-1 for strict <
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierThanTimeMillis(1721614343270));
    ASSERT_FALSE(snapshot.has_value());
}

TEST(SnapshotManagerTest, TestEarlierThanTimeMillisBetweenSnapshots) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // Between snapshot-1 (1721614343270) and snapshot-2 (1721614468258), should return snapshot-1
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierThanTimeMillis(1721614400000));
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(snapshot->Id(), 1);
}

TEST(SnapshotManagerTest, TestEarlierThanTimeMillisBeforeAll) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // Before snapshot-1 (1721614343270), should return no snapshot
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierThanTimeMillis(1721614343269));
    ASSERT_FALSE(snapshot.has_value());
}

TEST(SnapshotManagerTest, TestCommitLatestHint) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string test_data_path = dir->Str();
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK(mgr.CommitLatestHint(1));
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> latest_snapshot_id, mgr.LatestSnapshotId());
    ASSERT_EQ(latest_snapshot_id.value(), 1);

    ASSERT_OK_AND_ASSIGN(bool exists, file_system->Exists(test_data_path));
    ASSERT_TRUE(exists);
}

}  // namespace paimon::test
