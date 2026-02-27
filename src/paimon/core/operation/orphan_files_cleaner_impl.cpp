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

#include "paimon/core/operation/orphan_files_cleaner_impl.h"

#include <algorithm>
#include <future>
#include <map>
#include <optional>
#include <queue>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/metrics/clean_metrics.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/duration.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;

const int64_t OrphanFilesCleanerImpl::MIN_VALID_FILE_MODIFICATION_MS = 10 * 1000 * 1000 * 1000L;

OrphanFilesCleanerImpl::OrphanFilesCleanerImpl(
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<arrow::Schema>& schema, const std::string& root_path,
    const CoreOptions& options, const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::vector<std::string>& partition_keys,
    const std::shared_ptr<ManifestFile>& manifest_file,
    const std::shared_ptr<ManifestList>& manifest_list, int64_t older_than_ms,
    std::function<bool(const std::string&)> should_be_retained)
    : memory_pool_(memory_pool),
      executor_(executor),
      schema_(schema),
      root_path_(root_path),
      options_(options),
      fs_(options.GetFileSystem()),
      snapshot_manager_(snapshot_manager),
      partition_keys_(partition_keys),
      manifest_file_(manifest_file),
      manifest_list_(manifest_list),
      older_than_ms_(older_than_ms),
      should_be_retained_(should_be_retained),
      metrics_(std::make_shared<MetricsImpl>()) {}

bool OrphanFilesCleanerImpl::SupportToClean(const std::string& file_name) {
    static std::vector<std::pair<std::string, std::string>> supported_pattern = {
        {"manifest-", ""}, {"manifest-list-", ""}, {".", ".tmp"}};
    for (const auto& pattern : supported_pattern) {
        if (StringUtils::StartsWith(file_name, pattern.first) &&
            StringUtils::EndsWith(file_name, pattern.second)) {
            return true;
        }
    }
    static std::vector<std::string> supported_formats = {".orc", ".parquet", ".avro", ".lance"};
    for (const auto& format : supported_formats) {
        if (StringUtils::StartsWith(file_name, "data-") &&
            StringUtils::EndsWith(file_name, format)) {
            return true;
        }
    }
    return false;
}

Result<std::set<std::string>> OrphanFilesCleanerImpl::Clean() {
    Duration main_duration;
    if (!MinimalTryBestListingDirs(PathUtil::JoinPath(root_path_, "tag")).empty()) {
        return Status::NotImplemented("OrphanFilesCleaner do not support cleaning table with tag");
    }
    if (!MinimalTryBestListingDirs(PathUtil::JoinPath(root_path_, "branch")).empty()) {
        return Status::NotImplemented(
            "OrphanFilesCleaner do not support cleaning table with branch");
    }
    PAIMON_ASSIGN_OR_RAISE(std::set<std::string> all_dirs, ListPaimonFileDirs());
    std::vector<std::future<std::vector<std::unique_ptr<FileStatus>>>> file_statuses_futures;
    for (const auto& dir : all_dirs) {
        file_statuses_futures.push_back(
            Via(executor_.get(), [this, dir] { return TryBestListingDirs(dir); }));
    }
    PAIMON_ASSIGN_OR_RAISE(std::set<std::string> used_file_names, GetUsedFiles());

    Duration duration;
    std::set<std::string> need_to_deletes;
    std::vector<std::future<void>> futures;
    ScopeGuard guard([&futures]() { Wait(futures); });
    uint64_t file_statuses_duration = duration.Reset();
    for (const auto& file_statuses : CollectAll(file_statuses_futures)) {
        for (const auto& file_status : file_statuses) {
            if (file_status->IsDir()) {
                continue;
            }
            std::string path = file_status->GetPath();
            std::string file_name = PathUtil::GetName(path);
            if (!SupportToClean(file_name)) {
                continue;
            }
            if (file_status->GetModificationTime() < older_than_ms_ &&
                !used_file_names.count(file_name)) {
                if (should_be_retained_ && should_be_retained_(file_name)) {
                    continue;
                }
                if (file_status->GetModificationTime() <= MIN_VALID_FILE_MODIFICATION_MS) {
                    return Status::Invalid(
                        fmt::format("file '{}' modification '{}' is not in millisecond", path,
                                    file_status->GetModificationTime()));
                }
                need_to_deletes.insert(path);
                futures.push_back(Via(executor_.get(), [this, path]() {
                    auto s = fs_->Delete(path, /*recursive=*/false);
                    (void)s;
                }));
            }
        }
    }
    metrics_->SetCounter(CleanMetrics::CLEAN_DURATION, main_duration.Get());
    metrics_->SetCounter(CleanMetrics::CLEAN_SCAN_ORPHAN_FILES_DURATION, duration.Get());
    metrics_->SetCounter(CleanMetrics::CLEAN_LIST_FILE_STATUS_DURATION, file_statuses_duration);
    metrics_->SetCounter(CleanMetrics::CLEAN_LIST_FILE_STATUS_TASKS,
                         static_cast<uint64_t>(file_statuses_futures.size()));
    metrics_->SetCounter(CleanMetrics::CLEAN_ORPHAN_FILES,
                         static_cast<uint64_t>(need_to_deletes.size()));
    return need_to_deletes;
}

Result<std::set<std::string>> OrphanFilesCleanerImpl::ListPaimonFileDirs() const {
    Duration duration;
    std::set<std::string> paimon_file_dirs;
    paimon_file_dirs.insert(snapshot_manager_->SnapshotDirectory());
    paimon_file_dirs.insert(FileStorePathFactory::ManifestPath(root_path_));
    // TODO(jinli.zjw): support clean index, stats, changelog in the future
    // paimon_file_dirs.insert(FileStorePathFactory::IndexPath(root_path_));
    // paimon_file_dirs.insert(FileStorePathFactory::StatisticsPath(root_path_));
    std::set<std::string> file_dirs = ListFileDirs(root_path_, partition_keys_.size());
    paimon_file_dirs.insert(file_dirs.begin(), file_dirs.end());
    // add external data paths
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> data_file_external_paths,
                           options_.CreateExternalPaths());
    if (!data_file_external_paths.empty()) {
        return Status::Invalid(
            "OrphanFilesCleaner do not support cleaning table with external paths");
    }
    // TODO(liancheng): support clean external paths in the future
    // PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
    // options_.CreateExternalPaths());
    // for (const auto& external_path : external_paths) {
    //     std::set<std::string> external_file_dirs =
    //         ListFileDirs(external_path, partition_keys_.size());
    //     paimon_file_dirs.insert(external_file_dirs.begin(), external_file_dirs.end());
    // }
    metrics_->SetCounter(CleanMetrics::CLEAN_LIST_DIRECTORIES_DURATION, duration.Get());
    metrics_->SetCounter(CleanMetrics::CLEAN_LIST_DIRECTORIES,
                         static_cast<uint64_t>(paimon_file_dirs.size()));
    return paimon_file_dirs;
}

std::set<std::string> OrphanFilesCleanerImpl::ListFileDirs(const std::string& path,
                                                           int32_t max_level) const {
    std::queue<std::string> queue;
    queue.push(path);
    std::set<std::string> results;
    for (int32_t current_level = 0; current_level <= max_level; current_level++) {
        std::vector<std::future<std::vector<std::unique_ptr<BasicFileStatus>>>> futures;
        while (!queue.empty()) {
            auto current_path = queue.front();
            futures.push_back(Via(executor_.get(), [this, current_path] {
                return MinimalTryBestListingDirs(current_path);
            }));
            queue.pop();
        }
        for (const auto& dirs : CollectAll(futures)) {
            for (const auto& dir : dirs) {
                const auto& dir_name = PathUtil::GetName(dir->GetPath());
                if (current_level == max_level) {
                    if (StringUtils::StartsWith(
                            dir_name, std::string(FileStorePathFactory::BUCKET_PATH_PREFIX))) {
                        results.insert(dir->GetPath());
                    }
                } else {
                    if (dir_name.find("=") != std::string::npos) {
                        queue.push(dir->GetPath());
                    }
                }
            }
        }
    }
    return results;
}

std::vector<std::unique_ptr<FileStatus>> OrphanFilesCleanerImpl::TryBestListingDirs(
    const std::string& path) const {
    Result<bool> is_exist = fs_->Exists(path);
    if (!is_exist.ok()) {
        return {};
    }
    std::vector<std::unique_ptr<FileStatus>> file_statuses;
    auto status = fs_->ListFileStatus(path, &file_statuses);
    if (!status.ok()) {
        return {};
    }
    return file_statuses;
}

std::vector<std::unique_ptr<BasicFileStatus>> OrphanFilesCleanerImpl::MinimalTryBestListingDirs(
    const std::string& path) const {
    Result<bool> is_exist = fs_->Exists(path);
    if (!is_exist.ok()) {
        return {};
    }
    std::vector<std::unique_ptr<BasicFileStatus>> file_statuses;
    auto status = fs_->ListDir(path, &file_statuses);
    if (!status.ok()) {
        return {};
    }
    return file_statuses;
}

Result<std::set<std::string>> OrphanFilesCleanerImpl::GetUsedFiles() const {
    std::set<std::string> used_files;
    // TODO(jinli.zjw): consider changelog(add tests), stats
    used_files.insert(SnapshotManager::EARLIEST);
    used_files.insert(SnapshotManager::LATEST);
    Duration duration;
    PAIMON_ASSIGN_OR_RAISE(std::vector<Snapshot> snapshots, snapshot_manager_->GetAllSnapshots());
    std::vector<std::future<Result<std::set<std::string>>>> used_files_futures;
    std::vector<Result<std::set<std::string>>> used_files_results;
    {
        ScopeGuard guard([&used_files_futures, &used_files_results]() {
            used_files_results = CollectAll(used_files_futures);
        });
        for (const auto& snapshot : snapshots) {
            const std::optional<std::string>& changelog_manifest_list =
                snapshot.ChangelogManifestList();
            if (changelog_manifest_list) {
                used_files.insert(changelog_manifest_list.value());
                return Status::NotImplemented("OrphanFilesCleaner do not support clean changelog");
            }
            const std::optional<std::string>& index_manifest_name = snapshot.IndexManifest();
            if (index_manifest_name) {
                return Status::NotImplemented(
                    "OrphanFilesCleaner do not support clean index manifest");
                // TODO(jinli.zjw): support IndexManifestEntry and add tests
                // used_files.insert(index_manifest_name.value());
            }

            used_files_futures.emplace_back(Via(
                executor_.get(), [this, snapshot] { return GetUsedFilesBySnapshot(snapshot); }));
        }
    }

    for (const auto& used_files_result : used_files_results) {
        PAIMON_RETURN_NOT_OK(used_files_result);
        used_files.insert(used_files_result.value().begin(), used_files_result.value().end());
    }

    metrics_->SetCounter(CleanMetrics::CLEAN_LIST_USED_FILES_DURATION, duration.Get());
    metrics_->SetCounter(CleanMetrics::CLEAN_USED_FILES, static_cast<uint64_t>(used_files.size()));
    metrics_->SetCounter(CleanMetrics::CLEAN_SNAPSHOT_FILES,
                         static_cast<uint64_t>(snapshots.size()));
    return used_files;
}

Result<std::set<std::string>> OrphanFilesCleanerImpl::GetUsedFilesBySnapshot(
    const Snapshot& snapshot) const {
    std::set<std::string> used_files;

    used_files.insert(SnapshotManager::SNAPSHOT_PREFIX + std::to_string(snapshot.Id()));
    used_files.insert(snapshot.BaseManifestList());
    used_files.insert(snapshot.DeltaManifestList());
    std::vector<ManifestFileMeta> manifests;
    PAIMON_RETURN_NOT_OK(manifest_list_->ReadIfFileExist(snapshot.BaseManifestList(),
                                                         /*filter=*/nullptr, &manifests));
    PAIMON_RETURN_NOT_OK(manifest_list_->ReadIfFileExist(snapshot.DeltaManifestList(),
                                                         /*filter=*/nullptr, &manifests));

    for (const auto& manifest : manifests) {
        used_files.insert(manifest.FileName());
        std::vector<ManifestEntry> manifest_entries;
        PAIMON_RETURN_NOT_OK(manifest_file_->ReadIfFileExist(
            manifest.FileName(), /*filter=*/nullptr, &manifest_entries));
        for (const auto& manifest_entry : manifest_entries) {
            used_files.insert(manifest_entry.FileName());
        }
    }

    return used_files;
}

}  // namespace paimon
