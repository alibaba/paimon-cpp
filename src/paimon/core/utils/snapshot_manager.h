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
#include <set>
#include <string>
#include <vector>

#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"

namespace paimon {

class Snapshot;
class FileSystem;

/// Manager for `Snapshot`, providing utility methods related to paths and snapshot hints.
class SnapshotManager {
 public:
    static constexpr char SNAPSHOT_PREFIX[] = "snapshot-";
    static constexpr char EARLIEST[] = "EARLIEST";
    static constexpr char LATEST[] = "LATEST";

    SnapshotManager(const std::shared_ptr<FileSystem>& fs, const std::string& root_path);
    SnapshotManager(const std::shared_ptr<FileSystem>& fs, const std::string& root_path,
                    const std::string& branch);
    ~SnapshotManager();

    const std::shared_ptr<FileSystem>& Fs() const;
    const std::string& RootPath() const;
    const std::string& Branch() const;
    Result<std::optional<Snapshot>> LatestSnapshot() const;
    std::string SnapshotDirectory() const;
    std::string SnapshotPath(int64_t snapshot_id) const;
    Result<std::optional<Snapshot>> LatestSnapshotOfUser(const std::string& user);
    Status CommitLatestHint(int64_t snapshot_id);
    Status CommitEarliestHint(int64_t snapshot_id);
    Result<Snapshot> LoadSnapshot(int64_t snapshot_id) const;
    Result<std::optional<int64_t>> EarliestSnapshotId() const;
    Result<std::optional<int64_t>> LatestSnapshotId() const;
    Result<bool> SnapshotExists(int64_t snapshot_id) const;
    Result<std::set<std::string>> TryGetNonSnapshotFiles(int64_t older_than_ms) const;
    Result<std::vector<Snapshot>> GetAllSnapshots() const;

 private:
    static constexpr int32_t READ_HINT_RETRY_NUM = 3;
    static constexpr int32_t READ_HINT_RETRY_INTERVAL = 1;

    std::string BranchPath() const;

    Result<std::optional<int64_t>> FindEarliest(
        const std::string& dir, const std::string& prefix,
        const std::function<std::string(int64_t)>& path_func) const;
    Result<std::optional<int64_t>> FindLatest(
        const std::string& dir, const std::string& prefix,
        const std::function<std::string(int64_t)>& path_func) const;
    Result<std::optional<int64_t>> FindByListFiles(
        const std::function<int64_t(int64_t, int64_t)> reducer_func, const std::string& dir,
        const std::string& prefix) const;
    std::optional<int64_t> ReadHint(const std::string& file_name, const std::string& dir) const;
    Status CommitHint(int64_t snapshot_id, const std::string& file_name, const std::string& dir);

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string root_path_;
    std::string branch_;
};

}  // namespace paimon
