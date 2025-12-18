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

#include "paimon/core/table/source/snapshot/starting_scanner.h"

namespace paimon {
/// `StartingScanner` for the `StartupMode::Latest()` startup mode of a
/// streaming read.
class ContinuousLatestStartingScanner : public StartingScanner {
 public:
    static Result<std::unique_ptr<ContinuousLatestStartingScanner>> Create(
        const std::shared_ptr<SnapshotManager>& snapshot_manager) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> starting_snapshot_id,
                               snapshot_manager->LatestSnapshotId());
        return std::unique_ptr<ContinuousLatestStartingScanner>(
            new ContinuousLatestStartingScanner(snapshot_manager, starting_snapshot_id));
    }

    Result<std::shared_ptr<ScanResult>> Scan(
        const std::shared_ptr<SnapshotReader>& snapshot_reader) override {
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> latest_snapshot_id,
                               snapshot_manager_->LatestSnapshotId());
        if (latest_snapshot_id == std::nullopt) {
            return std::make_shared<StartingScanner::NoSnapshot>();
        }
        // If there's no snapshot before the reading job starts,
        // then the first snapshot should be considered as an incremental snapshot
        int64_t next_snapshot_id = starting_snapshot_id_ == std::nullopt
                                       ? Snapshot::FIRST_SNAPSHOT_ID
                                       : latest_snapshot_id.value() + 1;
        return std::make_shared<StartingScanner::NextSnapshot>(next_snapshot_id);
    }

 private:
    ContinuousLatestStartingScanner(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                                    const std::optional<int64_t>& starting_snapshot_id)
        : StartingScanner(snapshot_manager) {
        starting_snapshot_id_ = starting_snapshot_id;
    }
};
}  // namespace paimon
