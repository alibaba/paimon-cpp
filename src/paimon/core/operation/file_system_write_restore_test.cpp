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

#include "paimon/core/operation/file_system_write_restore.h"

#include <limits>
#include <memory>

#include "gtest/gtest.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(FileSystemWriteRestoreTest, LatestCommittedIdentifierNoSnapshot) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto snapshot_manager = std::make_shared<SnapshotManager>(
        fs, paimon::test::GetDataDir() + "/orc/append_09.db/not_exist");

    FileSystemWriteRestore restore(snapshot_manager, /*scan=*/nullptr,
                                   /*index_file_handler=*/nullptr);

    ASSERT_OK_AND_ASSIGN(int64_t latest_identifier,
                         restore.LatestCommittedIdentifier("unknown_user"));
    ASSERT_EQ(latest_identifier, std::numeric_limits<int64_t>::min());
}

TEST(FileSystemWriteRestoreTest, LatestCommittedIdentifierWithSnapshot) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto snapshot_manager = std::make_shared<SnapshotManager>(
        fs, paimon::test::GetDataDir() + "/orc/append_09.db/append_09");

    FileSystemWriteRestore restore(snapshot_manager, /*scan=*/nullptr,
                                   /*index_file_handler=*/nullptr);

    ASSERT_OK_AND_ASSIGN(int64_t latest_identifier,
                         restore.LatestCommittedIdentifier("b02e4322-9c5f-41e1-a560-c0156fdf7b9c"));
    ASSERT_EQ(latest_identifier, std::numeric_limits<int64_t>::max());
}

TEST(FileSystemWriteRestoreTest, GetRestoreFilesReturnsEmptyWhenNoLatestSnapshot) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto snapshot_manager = std::make_shared<SnapshotManager>(
        fs, paimon::test::GetDataDir() + "/orc/append_09.db/not_exist");

    FileSystemWriteRestore restore(snapshot_manager, /*scan=*/nullptr,
                                   /*index_file_handler=*/nullptr);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RestoreFiles> files,
                         restore.GetRestoreFiles(BinaryRow::EmptyRow(), /*bucket=*/0,
                                                 /*scan_deletion_vectors_index=*/true));
    ASSERT_FALSE(files->GetSnapshot().has_value());
    ASSERT_FALSE(files->TotalBuckets().has_value());
    ASSERT_TRUE(files->DataFiles().empty());
    ASSERT_TRUE(files->DeleteVectorsIndex().empty());
}

}  // namespace paimon::test
