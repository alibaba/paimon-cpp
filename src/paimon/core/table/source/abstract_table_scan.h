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

#include "paimon/core/core_options.h"
#include "paimon/core/table/source/snapshot/continuous_from_snapshot_full_starting_scanner.h"
#include "paimon/core/table/source/snapshot/continuous_from_snapshot_starting_scanner.h"
#include "paimon/core/table/source/snapshot/continuous_latest_starting_scanner.h"
#include "paimon/core/table/source/snapshot/full_starting_scanner.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/core/table/source/snapshot/static_from_snapshot_starting_scanner.h"
#include "paimon/table/source/startup_mode.h"
#include "paimon/table/source/table_scan.h"
namespace paimon {
/// An abstraction layer above `FileStoreScan` to provide input split generation.
class AbstractTableScan : public TableScan {
 public:
    AbstractTableScan(const CoreOptions& core_options,
                      const std::shared_ptr<SnapshotReader>& snapshot_reader)
        : core_options_(core_options), snapshot_reader_(snapshot_reader) {}

 protected:
    Result<std::shared_ptr<StartingScanner>> CreateStartingScanner(bool is_streaming) const {
        const auto& snapshot_manager = snapshot_reader_->GetSnapshotManager();
        auto startup_mode = core_options_.GetStartupMode();
        std::optional<int64_t> specified_snapshot_id = core_options_.GetScanSnapshotId();
        if (startup_mode == StartupMode::LatestFull()) {
            return std::make_shared<FullStartingScanner>(snapshot_manager);
        } else if (startup_mode == StartupMode::Latest()) {
            if (is_streaming) {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<StartingScanner> starting_scanner,
                                       ContinuousLatestStartingScanner::Create(snapshot_manager));
                return starting_scanner;
            } else {
                return std::shared_ptr<StartingScanner>(new FullStartingScanner(snapshot_manager));
            }
        } else if (startup_mode == StartupMode::FromSnapshot()) {
            if (specified_snapshot_id != std::nullopt) {
                return is_streaming
                           ? std::shared_ptr<StartingScanner>(
                                 new ContinuousFromSnapshotStartingScanner(
                                     snapshot_manager, specified_snapshot_id.value()))
                           : std::shared_ptr<StartingScanner>(new StaticFromSnapshotStartingScanner(
                                 snapshot_manager, specified_snapshot_id.value()));
            } else {
                return Status::Invalid(
                    "scan.snapshot-id must be set when startup mode is FROM_SNAPSHOT");
            }
        } else if (startup_mode == StartupMode::FromSnapshotFull()) {
            if (specified_snapshot_id != std::nullopt) {
                return is_streaming
                           ? std::shared_ptr<StartingScanner>(
                                 new ContinuousFromSnapshotFullStartingScanner(
                                     snapshot_manager, specified_snapshot_id.value()))
                           : std::shared_ptr<StartingScanner>(new StaticFromSnapshotStartingScanner(
                                 snapshot_manager, specified_snapshot_id.value()));
            } else {
                return Status::Invalid(
                    "scan.snapshot-id must be set when startup mode is FROM_SNAPSHOT_FULL");
            }
        }
        return Status::Invalid(
            fmt::format("Unsupported snapshot startup mode {}", startup_mode.ToString()));
    }

 protected:
    CoreOptions core_options_;
    std::shared_ptr<SnapshotReader> snapshot_reader_;
};
}  // namespace paimon
