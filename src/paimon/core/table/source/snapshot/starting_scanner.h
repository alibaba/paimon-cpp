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
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
namespace paimon {

/// Helper class for the first planning of `TableScan`.
class StartingScanner {
 public:
    /// Scan result of `Scan()`.
    class ScanResult {
     public:
        virtual ~ScanResult() = default;
    };

    /// Currently, there is no snapshot, need to wait for the snapshot to be generated.
    class NoSnapshot : public ScanResult {};

    /// ScanResult with scanned snapshot. Next snapshot should be the current snapshot plus 1.
    class CurrentSnapshot : public ScanResult {
     public:
        explicit CurrentSnapshot(const std::shared_ptr<Plan>& plan) : plan_(plan) {}

        Result<int64_t> SnapshotId() const {
            if (plan_->SnapshotId() == std::nullopt) {
                return Status::Invalid("CurrentSnapshot must have a snapshot id");
            }
            return plan_->SnapshotId().value();
        }

        const std::vector<std::shared_ptr<Split>>& Splits() const {
            return plan_->Splits();
        }

        const std::shared_ptr<Plan>& GetPlan() const {
            return plan_;
        }

     private:
        std::shared_ptr<Plan> plan_;
    };

    /// Return the next snapshot for followup scanning. The current snapshot is not scanned (even
    /// doesn't exist), so there are no splits.
    class NextSnapshot : public ScanResult {
     public:
        explicit NextSnapshot(int64_t next_snapshot_id) : next_snapshot_id_(next_snapshot_id) {}

        int64_t NextSnapshotId() const {
            return next_snapshot_id_;
        }

     private:
        int64_t next_snapshot_id_;
    };

    explicit StartingScanner(const std::shared_ptr<SnapshotManager>& snapshot_manager)
        : snapshot_manager_(snapshot_manager) {}

    virtual ~StartingScanner() = default;

    virtual Result<std::shared_ptr<ScanResult>> Scan(
        const std::shared_ptr<SnapshotReader>& snapshot_reader) = 0;

 protected:
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::optional<int64_t> starting_snapshot_id_;
};
}  // namespace paimon
