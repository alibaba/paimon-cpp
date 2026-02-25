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

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "paimon/core/catalog/snapshot_commit.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class PartitionStatistics;

/// A `SnapshotCommit` using file renaming to commit.
///
/// @note When the file system is local or hdfs, rename is atomic. But if the file system is object
/// storage, we need additional lock protection.
// TODO(jinli.zjw): add additional lock protection for object storage.
class RenamingSnapshotCommit : public SnapshotCommit {
 public:
    RenamingSnapshotCommit(const std::shared_ptr<FileSystem>& fs,
                           const std::shared_ptr<SnapshotManager>& snapshot_manager)
        : fs_(fs), snapshot_manager_(snapshot_manager) {}

    Result<bool> Commit(const Snapshot& snapshot,
                        const std::vector<PartitionStatistics>& statistics) override {
        PAIMON_ASSIGN_OR_RAISE(std::string json_str, snapshot.ToJsonString());
        std::string snapshot_path = snapshot_manager_->SnapshotPath(snapshot.Id());
        PAIMON_ASSIGN_OR_RAISE(bool is_exist, fs_->Exists(snapshot_path));
        if (is_exist) {
            return false;
        }
        // To prevent the case where an atomic write times out but actually succeeds,
        // retrying the commit could lead to the snapshot file being committed multiple times.
        // Therefore, retries should be handled by the upper layer,
        // which should call FilterAndCommit to avoid duplicate commits.
        // Therefore, we should not trigger cleanup here,
        // as it may delete meta files from a snapshot that was just written by ourselves,
        // leading to an incomplete or corrupted snapshot.
        PAIMON_RETURN_NOT_OK(fs_->AtomicStore(snapshot_path, json_str));
        PAIMON_RETURN_NOT_OK(snapshot_manager_->CommitLatestHint(snapshot.Id()));
        return true;
    }

    Result<std::string> GetLastCommitTableRequest() override {
        return Status::Invalid(
            "renaming snapshot commit do not support get last commit table request");
    }

 private:
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
};

}  // namespace paimon
