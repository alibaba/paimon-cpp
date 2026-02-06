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

#include <algorithm>
#include <chrono>
#include <random>
#include <thread>
#include <utility>

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/file_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon {

SnapshotManager::SnapshotManager(const std::shared_ptr<FileSystem>& fs,
                                 const std::string& root_path)
    : SnapshotManager(fs, root_path, BranchManager::DEFAULT_MAIN_BRANCH) {}

SnapshotManager::SnapshotManager(const std::shared_ptr<FileSystem>& fs,
                                 const std::string& root_path, const std::string& branch)
    : fs_(fs), root_path_(root_path), branch_(BranchManager::NormalizeBranch(branch)) {}

SnapshotManager::~SnapshotManager() = default;

const std::shared_ptr<FileSystem>& SnapshotManager::Fs() const {
    return fs_;
}

const std::string& SnapshotManager::RootPath() const {
    return root_path_;
}

const std::string& SnapshotManager::Branch() const {
    return branch_;
}

Result<std::optional<Snapshot>> SnapshotManager::LatestSnapshotOfUser(const std::string& user) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> latest_id, LatestSnapshotId());
    if (latest_id == std::nullopt) {
        return std::optional<Snapshot>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> earliest_id, EarliestSnapshotId());
    if (earliest_id == std::nullopt) {
        return Status::Invalid(
            "Latest snapshot id is not null, but earliest snapshot id is null. This is "
            "unexpected.");
    }

    for (int64_t id = latest_id.value(); id >= earliest_id.value(); id--) {
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, LoadSnapshot(id));
        if (snapshot.CommitUser() == user) {
            return std::optional<Snapshot>(snapshot);
        }
    }
    return std::optional<Snapshot>();
}

Result<Snapshot> SnapshotManager::LoadSnapshot(int64_t snapshot_id) const {
    return Snapshot::FromPath(fs_, SnapshotPath(snapshot_id));
}

Result<std::optional<Snapshot>> SnapshotManager::LatestSnapshot() const {
    PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> snapshot_id, LatestSnapshotId());
    if (snapshot_id == std::nullopt) {
        return std::optional<Snapshot>();
    } else {
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, LoadSnapshot(snapshot_id.value()));
        return std::optional<Snapshot>(snapshot);
    }
}

Result<std::optional<int64_t>> SnapshotManager::LatestSnapshotId() const {
    return FindLatest(
        SnapshotDirectory(), std::string(SNAPSHOT_PREFIX),
        [this](int64_t snapshot_id) -> std::string { return SnapshotPath(snapshot_id); });
}

Result<std::optional<int64_t>> SnapshotManager::EarliestSnapshotId() const {
    return FindEarliest(
        SnapshotDirectory(), std::string(SNAPSHOT_PREFIX),
        [this](int64_t snapshot_id) -> std::string { return SnapshotPath(snapshot_id); });
}

std::string SnapshotManager::BranchPath() const {
    return BranchManager::BranchPath(root_path_, branch_);
}

std::string SnapshotManager::SnapshotDirectory() const {
    return PathUtil::JoinPath(BranchPath(), "/snapshot");
}

Result<bool> SnapshotManager::SnapshotExists(int64_t snapshot_id) const {
    return fs_->Exists(SnapshotPath(snapshot_id));
}
std::string SnapshotManager::SnapshotPath(int64_t snapshot_id) const {
    return PathUtil::JoinPath(
        BranchPath(), "/snapshot/" + std::string(SNAPSHOT_PREFIX) + std::to_string(snapshot_id));
}

Result<std::optional<int64_t>> SnapshotManager::FindEarliest(
    const std::string& dir, const std::string& prefix,
    const std::function<std::string(int64_t)>& path_func) const {
    PAIMON_ASSIGN_OR_RAISE(bool is_exist, fs_->Exists(dir));
    if (!is_exist) {
        return std::optional<int64_t>();
    }
    std::optional<int64_t> snapshot_id = ReadHint(EARLIEST, dir);
    if (snapshot_id != std::nullopt) {
        std::string path = path_func(snapshot_id.value());
        PAIMON_ASSIGN_OR_RAISE(bool is_exist, fs_->Exists(path));
        if (is_exist) {
            return snapshot_id;
        }
    }
    return FindByListFiles([](int64_t lhs, int64_t rhs) -> int64_t { return std::min(lhs, rhs); },
                           dir, prefix);
}

Result<std::optional<int64_t>> SnapshotManager::FindLatest(
    const std::string& dir, const std::string& prefix,
    const std::function<std::string(int64_t)>& path_func) const {
    PAIMON_ASSIGN_OR_RAISE(bool is_exist, fs_->Exists(dir));
    if (!is_exist) {
        return std::optional<int64_t>();
    }
    std::optional<int64_t> snapshot_id = ReadHint(LATEST, dir);
    if (snapshot_id != std::nullopt && snapshot_id.value() > 0) {
        int64_t next_snapshot = snapshot_id.value() + 1;
        // it is the latest only there is no next one
        std::string path = path_func(next_snapshot);
        PAIMON_ASSIGN_OR_RAISE(bool is_exist, fs_->Exists(path));
        if (!is_exist) {
            return snapshot_id;
        }
    }
    return FindByListFiles([](int64_t lhs, int64_t rhs) -> int64_t { return std::max(lhs, rhs); },
                           dir, prefix);
}

Result<std::optional<int64_t>> SnapshotManager::FindByListFiles(
    const std::function<int64_t(int64_t, int64_t)> reducer_func, const std::string& dir,
    const std::string& prefix) const {
    std::vector<int64_t> versions;
    PAIMON_RETURN_NOT_OK(FileUtils::ListVersionedFiles(fs_, dir, prefix, &versions));
    if (versions.empty()) {
        return std::optional<int64_t>();
    }
    int64_t ret = versions[0];
    for (const auto& version : versions) {
        ret = reducer_func(ret, version);
    }
    return std::optional<int64_t>(ret);
}

Result<std::set<std::string>> SnapshotManager::TryGetNonSnapshotFiles(int64_t older_than_ms) const {
    std::set<std::string> non_snapshot_files;

    std::vector<std::unique_ptr<FileStatus>> file_status_list;
    PAIMON_RETURN_NOT_OK(fs_->ListFileStatus(SnapshotDirectory(), &file_status_list));
    for (const auto& file_status : file_status_list) {
        std::string file_name = PathUtil::GetName(file_status->GetPath());
        if (!StringUtils::StartsWith(file_name, std::string(SNAPSHOT_PREFIX)) &&
            file_name != std::string(EARLIEST) && file_name != std::string(LATEST)) {
            if (file_status->GetModificationTime() < older_than_ms) {
                non_snapshot_files.insert(file_status->GetPath());
            }
        }
    }
    return non_snapshot_files;
}

// Noted that: try best to read hint to avoid unnecessary list dir operation, if failed, do not
// return error
std::optional<int64_t> SnapshotManager::ReadHint(const std::string& file_name,
                                                 const std::string& dir) const {
    std::string path = PathUtil::JoinPath(dir, file_name);
    int32_t retry_number = 0;
    while (retry_number++ < READ_HINT_RETRY_NUM) {
        std::string content;
        Status s = fs_->ReadFile(path, &content);
        if (s.ok()) {
            return StringUtils::StringToValue<int64_t>(content);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(READ_HINT_RETRY_INTERVAL));
    }
    return std::nullopt;
}

Status SnapshotManager::CommitLatestHint(int64_t snapshot_id) {
    return CommitHint(snapshot_id, LATEST, SnapshotDirectory());
}

Status SnapshotManager::CommitEarliestHint(int64_t snapshot_id) {
    return CommitHint(snapshot_id, EARLIEST, SnapshotDirectory());
}

Status SnapshotManager::CommitHint(int64_t snapshot_id, const std::string& file_name,
                                   const std::string& dir) {
    std::string path = PathUtil::JoinPath(dir, file_name);
    std::string snapshot_id_str = std::to_string(snapshot_id);
    int32_t loop_time = 3;
    Status s;
    while (loop_time-- > 0) {
        s = fs_->WriteFile(path, snapshot_id_str, /*overwrite=*/true);
        if (s.ok()) {
            return s;
        } else {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<int> dist(0, 999);
            int64_t sleep_time = dist(gen) + 500;
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
        }
    }
    return s;
}

Result<std::vector<Snapshot>> SnapshotManager::GetAllSnapshots() const {
    std::vector<Snapshot> snapshots;
    std::vector<std::unique_ptr<BasicFileStatus>> file_statuses;
    PAIMON_RETURN_NOT_OK(FileUtils::ListVersionedFileStatus(fs_, SnapshotDirectory(),
                                                            SNAPSHOT_PREFIX, &file_statuses));
    for (const auto& file_status : file_statuses) {
        auto snapshot_path = file_status->GetPath();
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, Snapshot::FromPath(fs_, snapshot_path));
        snapshots.push_back(snapshot);
    }
    return snapshots;
}

}  // namespace paimon
