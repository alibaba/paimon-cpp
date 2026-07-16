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

#include "paimon/core/catalog/renaming_snapshot_commit.h"

#include <cstdint>
#include <map>
#include <optional>

#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(RenamingSnapshotCommitTest, TestSimple) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto snapshot_manager = std::make_shared<SnapshotManager>(fs, dir->Str());

    auto commit = std::make_shared<RenamingSnapshotCommit>(fs, snapshot_manager);
    ASSERT_NOK_WITH_MSG(commit->GetLastCommitTableRequest(),
                        "renaming snapshot commit do not support get last commit table request");
    Snapshot snapshot(
        /*version=*/3, /*id=*/1, /*schema_id=*/0,
        /*base_manifest_list=*/"manifest-list-3879e56f-2f27-49ae-a2f3-3dcbb8eb0beb-0",
        /*base_manifest_list_size=*/291,
        /*delta_manifest_list=*/"manifest-list-3879e56f-2f27-49ae-a2f3-3dcbb8eb0beb-1",
        /*delta_manifest_list_size=*/1342, /*changelog_manifest_list=*/std::nullopt,
        /*changelog_manifest_list_size=*/std::nullopt, /*index_manifest=*/std::nullopt,
        /*commit_user=*/"commit_user_1", /*commit_identifier=*/9223372036854775807,
        /*commit_kind=*/Snapshot::CommitKind::Append(), /*time_millis=*/1758097357597,
        /*total_record_count=*/5,
        /*delta_record_count=*/5, /*changelog_record_count=*/0, /*watermark=*/std::nullopt,
        /*statistics=*/std::nullopt, /*properties=*/std::nullopt, /*next_row_id=*/0);
    ASSERT_OK_AND_ASSIGN(bool success, commit->Commit(snapshot, /*statistics=*/{}));
    ASSERT_TRUE(success);
    ASSERT_OK_AND_ASSIGN(bool exist,
                         fs->Exists(PathUtil::JoinPath(dir->Str(), "snapshot/snapshot-1")));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(bool exist1,
                         fs->Exists(PathUtil::JoinPath(dir->Str(), "snapshot/LATEST")));
    ASSERT_TRUE(exist1);
    // duplicate commit for snapshot-1
    ASSERT_OK_AND_ASSIGN(bool success1, commit->Commit(snapshot, /*statistics=*/{}));
    ASSERT_FALSE(success1);
}

}  // namespace paimon::test
