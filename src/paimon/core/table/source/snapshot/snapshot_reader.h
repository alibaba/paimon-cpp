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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/core/table/source/plan_impl.h"
#include "paimon/core/table/source/scan_mode.h"
#include "paimon/core/table/source/split_generator.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/result.h"
#include "paimon/table/source/data_split.h"
#include "paimon/table/source/plan.h"

namespace paimon {
class BinaryRow;
class FileStorePathFactory;
class IndexFileMeta;
class Snapshot;
class SnapshotManager;
struct DataFileMeta;

class SnapshotReader {
 public:
    SnapshotReader(const std::shared_ptr<FileStoreScan>& scan,
                   const std::shared_ptr<FileStorePathFactory>& path_factory,
                   std::unique_ptr<SplitGenerator>&& split_generator,
                   std::unique_ptr<IndexFileHandler>&& index_file_handler)
        : scan_(scan),
          path_factory_(path_factory),
          split_generator_(std::move(split_generator)),
          index_file_handler_(std::move(index_file_handler)) {}

    SnapshotReader* WithMode(const ScanMode& scan_mode) {
        scan_mode_ = scan_mode;
        scan_->WithKind(scan_mode);
        return this;
    }

    SnapshotReader* WithSnapshot(const Snapshot& snapshot) {
        scan_->WithSnapshot(snapshot);
        return this;
    }

    SnapshotReader* WithLevelFilter(const std::function<bool(int32_t)>& level_filter) {
        scan_->WithLevelFilter(level_filter);
        return this;
    }

    SnapshotReader* EnableValueFilter() {
        scan_->EnableValueFilter();
        return this;
    }

    SnapshotReader* OnlyReadRealBuckets() {
        scan_->OnlyReadRealBuckets();
        return this;
    }

    const std::shared_ptr<SnapshotManager>& GetSnapshotManager() const {
        return scan_->GetSnapshotManager();
    }

    /// Get splits from `FileKind::ADD` files.
    Result<std::shared_ptr<Plan>> Read() const;

 private:
    Result<std::vector<std::shared_ptr<Split>>> GenerateSplits(
        const std::optional<Snapshot>& snapshot, bool is_streaming,
        const std::unique_ptr<SplitGenerator>& split_generator,
        FileStoreScan::RawPlan::GroupFiles&& grouped_data_files) const;

    Result<std::vector<std::optional<DeletionFile>>> GetDeletionFiles(
        const BinaryRow& partition, int32_t bucket,
        const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
        const std::vector<std::shared_ptr<IndexFileMeta>>& index_file_metas) const;

 private:
    std::shared_ptr<FileStoreScan> scan_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::unique_ptr<SplitGenerator> split_generator_;
    std::unique_ptr<IndexFileHandler> index_file_handler_;
    ScanMode scan_mode_ = ScanMode::ALL;
};
}  // namespace paimon
