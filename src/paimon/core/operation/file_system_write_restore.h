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

#pragma once

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/restore_files.h"
#include "paimon/core/operation/write_restore.h"
#include "paimon/core/utils/snapshot_manager.h"

namespace paimon {

/// `WriteRestore` to restore files directly from file system.
class FileSystemWriteRestore : public WriteRestore {
 public:
    FileSystemWriteRestore(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                           std::unique_ptr<FileStoreScan>&& scan)
        : snapshot_manager_(snapshot_manager), scan_(std::move(scan)) {
        // TODO(yonghao.fyh): support index file handler
    }

    Result<int64_t> LatestCommittedIdentifier(const std::string& user) const override {
        // TODO(yonghao.fyh): in java paimon is LatestSnapshotOfUserFromFileSystem
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                               snapshot_manager_->LatestSnapshotOfUser(user));
        if (latest_snapshot) {
            return latest_snapshot.value().CommitIdentifier();
        }
        return std::numeric_limits<int64_t>::min();
    }

    Result<std::shared_ptr<RestoreFiles>> GetRestoreFiles() const override {
        // TODO(yonghao.fyh): java paimon doesn't use snapshot_manager.LatestSnapshot() here,
        // because they don't want to flood the catalog with high concurrency
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot,
                               snapshot_manager_->LatestSnapshot());
        if (snapshot == std::nullopt) {
            return RestoreFiles::Empty();
        }

        // Plan scan
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> plan,
                               scan_->WithSnapshot(snapshot.value())->CreatePlan());
        std::vector<ManifestEntry> entries = plan->Files();
        std::vector<std::shared_ptr<DataFileMeta>> restore_files;
        PAIMON_ASSIGN_OR_RAISE(std::optional<int32_t> total_buckets,
                               WriteRestore::ExtractDataFiles(entries, &restore_files));

        std::shared_ptr<IndexFileMeta> dynamic_bucket_index;
        std::vector<std::shared_ptr<IndexFileMeta>> delete_vectors_index;

        return std::make_shared<RestoreFiles>(
            snapshot, total_buckets, restore_files,
            /*dynamic_bucket_index=*/nullptr,
            /*delete_vectors_index=*/std::vector<std::shared_ptr<IndexFileMeta>>{});
    }

 private:
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::unique_ptr<FileStoreScan> scan_;
};

}  // namespace paimon
