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
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/snapshot.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/orphan_files_cleaner.h"
#include "paimon/result.h"

struct ArrowSchema;

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class SnapshotManager;
class FileStorePathFactory;
class FileSystem;
class ManifestFile;
class ManifestList;
class Executor;
class MemoryPool;

class OrphanFilesCleanerImpl : public OrphanFilesCleaner {
 public:
    OrphanFilesCleanerImpl(const std::shared_ptr<MemoryPool>& memory_pool,
                           const std::shared_ptr<Executor>& executor,
                           const std::shared_ptr<arrow::Schema>& schema,
                           const std::string& root_path, const CoreOptions& options,
                           const std::shared_ptr<SnapshotManager>& snapshot_manager,
                           const std::vector<std::string>& partition_keys,
                           const std::shared_ptr<ManifestFile>& manifest_file,
                           const std::shared_ptr<ManifestList>& manifest_list,
                           int64_t older_than_ms,
                           std::function<bool(const std::string&)> should_be_retained);

    Result<std::set<std::string>> Clean() override;

    std::shared_ptr<Metrics> GetMetrics() const override {
        return metrics_;
    }

 private:
    Result<std::set<std::string>> ListPaimonFileDirs() const;
    std::vector<std::unique_ptr<FileStatus>> TryBestListingDirs(const std::string& path) const;
    std::vector<std::unique_ptr<BasicFileStatus>> MinimalTryBestListingDirs(
        const std::string& path) const;
    std::set<std::string> ListFileDirs(const std::string& path, int32_t max_level) const;
    Result<std::set<std::string>> GetUsedFiles() const;
    Result<std::set<std::string>> GetUsedFilesBySnapshot(const Snapshot& snapshot) const;
    static bool SupportToClean(const std::string& file_name);

 private:
    static const int64_t MIN_VALID_FILE_MODIFICATION_MS;

    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<arrow::Schema> schema_;
    std::string root_path_;
    CoreOptions options_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::vector<std::string> partition_keys_;
    std::shared_ptr<ManifestFile> manifest_file_;
    std::shared_ptr<ManifestList> manifest_list_;
    int64_t older_than_ms_;
    std::function<bool(const std::string&)> should_be_retained_;

    std::shared_ptr<Metrics> metrics_;
};
}  // namespace paimon
