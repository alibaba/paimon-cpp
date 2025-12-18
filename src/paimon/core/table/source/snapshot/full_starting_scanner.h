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
/// `StartingScanner` for the `StartupMode::LatestFull()` startup mode.
class FullStartingScanner : public StartingScanner {
 public:
    explicit FullStartingScanner(const std::shared_ptr<SnapshotManager>& snapshot_manager)
        : StartingScanner(snapshot_manager) {}

    Result<std::shared_ptr<ScanResult>> Scan(
        const std::shared_ptr<SnapshotReader>& snapshot_reader) override {
        if (starting_snapshot_id_ == std::nullopt) {
            // try to get first snapshot
            PAIMON_ASSIGN_OR_RAISE(starting_snapshot_id_, snapshot_manager_->LatestSnapshotId());
        }
        if (starting_snapshot_id_ == std::nullopt) {
            return std::make_shared<StartingScanner::NoSnapshot>();
        }
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot,
                               snapshot_manager_->LoadSnapshot(starting_snapshot_id_.value()));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<Plan> plan,
            snapshot_reader->WithMode(ScanMode::ALL)->WithSnapshot(snapshot)->Read());
        return std::make_shared<StartingScanner::CurrentSnapshot>(plan);
    }
};
}  // namespace paimon
