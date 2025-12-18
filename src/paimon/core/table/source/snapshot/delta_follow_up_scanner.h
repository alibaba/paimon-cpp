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

#include "paimon/core/table/source/snapshot/follow_up_scanner.h"

namespace paimon {
class DeltaFollowUpScanner : public FollowUpScanner {
 public:
    bool NeedScanSnapshot(const Snapshot& snapshot) const override {
        if (snapshot.GetCommitKind() == Snapshot::CommitKind::Append()) {
            return true;
        }
        // TODO(liancheng.lsz): log
        return false;
    }
    Result<std::shared_ptr<Plan>> Scan(
        const Snapshot& snapshot,
        const std::shared_ptr<SnapshotReader>& snapshot_reader) const override {
        return snapshot_reader->WithMode(ScanMode::DELTA)->WithSnapshot(snapshot)->Read();
    }
};
}  // namespace paimon
