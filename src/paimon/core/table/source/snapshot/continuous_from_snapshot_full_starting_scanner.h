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

#include <algorithm>
#include <memory>

#include "paimon/core/table/source/snapshot/starting_scanner.h"

namespace paimon {
/// `StartingScanner` for the `StartupMode::FromSnapshotFull()` startup mode
/// of a batch read.
class ContinuousFromSnapshotFullStartingScanner : public StartingScanner {
 public:
    ContinuousFromSnapshotFullStartingScanner(
        const std::shared_ptr<SnapshotManager>& snapshot_manager, int64_t starting_snapshot_id)
        : StartingScanner(snapshot_manager) {
        starting_snapshot_id_ = starting_snapshot_id;
    }

    Result<std::shared_ptr<ScanResult>> Scan(
        const std::shared_ptr<SnapshotReader>& snapshot_reader) override {
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> earliest_id,
                               snapshot_manager_->EarliestSnapshotId());
        if (earliest_id == std::nullopt) {
            return std::make_shared<StartingScanner::NoSnapshot>();
        }
        int64_t ceiled_snapshot_id = std::max(earliest_id.value(), starting_snapshot_id_.value());
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot,
                               snapshot_manager_->LoadSnapshot(ceiled_snapshot_id));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<Plan> plan,
            snapshot_reader->WithMode(ScanMode::ALL)->WithSnapshot(snapshot)->Read());
        return std::make_shared<StartingScanner::CurrentSnapshot>(plan);
    }
};
}  // namespace paimon
