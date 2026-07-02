/*
 * Copyright 2026-present Alibaba Inc.
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
#include <vector>

#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/result.h"

namespace paimon {

/// Utils for row tracking commit.
class RowTrackingCommitUtils {
 public:
    struct RowTrackingAssigned {
        int64_t next_row_id_start;
        std::vector<ManifestEntry> assigned_entries;
    };

    // Assign sequence numbers and row ids for row-tracking commit.
    static Result<RowTrackingAssigned> AssignRowTracking(
        int64_t new_snapshot_id, int64_t first_row_id_start,
        const std::vector<ManifestEntry>& delta_files);

 private:
    static void AssignSnapshotId(int64_t snapshot_id, const std::vector<ManifestEntry>& delta_files,
                                 std::vector<ManifestEntry>* snapshot_assigned);

    static Result<int64_t> AssignRowTrackingMeta(int64_t first_row_id_start,
                                                 const std::vector<ManifestEntry>& delta_files,
                                                 std::vector<ManifestEntry>* row_id_assigned);
};

}  // namespace paimon
