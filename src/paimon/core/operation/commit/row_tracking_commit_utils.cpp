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

#include "paimon/core/operation/commit/row_tracking_commit_utils.h"

#include <algorithm>
#include <map>
#include <string>

#include "fmt/format.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/status.h"

namespace paimon {

namespace {

bool IsVectorStoreFile(const std::string& file_name) {
    return file_name.find(".vector.") != std::string::npos;
}

ManifestEntry CloneEntryWithClonedFileMeta(const ManifestEntry& entry) {
    auto cloned_file = std::make_shared<DataFileMeta>(*entry.File());
    return ManifestEntry(entry.Kind(), entry.Partition(), entry.Bucket(), entry.TotalBuckets(),
                         cloned_file);
}

}  // namespace

Result<RowTrackingCommitUtils::RowTrackingAssigned> RowTrackingCommitUtils::AssignRowTracking(
    int64_t new_snapshot_id, int64_t first_row_id_start,
    const std::vector<ManifestEntry>& delta_files) {
    // assigned snapshot id to delta files
    std::vector<ManifestEntry> snapshot_assigned;
    AssignSnapshotId(new_snapshot_id, delta_files, &snapshot_assigned);
    // assign row id for new files
    std::vector<ManifestEntry> row_id_assigned;
    PAIMON_ASSIGN_OR_RAISE(
        int64_t next_row_id_start,
        AssignRowTrackingMeta(first_row_id_start, snapshot_assigned, &row_id_assigned));
    return RowTrackingAssigned{next_row_id_start, std::move(row_id_assigned)};
}

void RowTrackingCommitUtils::AssignSnapshotId(int64_t snapshot_id,
                                              const std::vector<ManifestEntry>& delta_files,
                                              std::vector<ManifestEntry>* snapshot_assigned) {
    for (const auto& entry : delta_files) {
        ManifestEntry assigned_entry = CloneEntryWithClonedFileMeta(entry);
        int64_t min_seq_number = assigned_entry.File()->min_sequence_number;
        if (min_seq_number == 0L) {
            // Case 1: New file (e.g., from INSERT)
            // All records in this file get the current snapshot ID as sequence number
            assigned_entry.AssignSequenceNumber(snapshot_id, snapshot_id);
        } else {
            // Preserve original min/max sequence numbers from source files.
            // This follows Java release-1.4 row-tracking commit semantics.
        }
        snapshot_assigned->emplace_back(std::move(assigned_entry));
    }
}

Result<int64_t> RowTrackingCommitUtils::AssignRowTrackingMeta(
    int64_t first_row_id_start, const std::vector<ManifestEntry>& delta_files,
    std::vector<ManifestEntry>* row_id_assigned) {
    if (delta_files.empty()) {
        return first_row_id_start;
    }
    // assign row id for new files
    int64_t start = first_row_id_start;
    int64_t blob_start_default = first_row_id_start;
    std::map<std::string, int64_t> blob_starts;
    int64_t vector_store_start = first_row_id_start;

    for (const auto& entry : delta_files) {
        ManifestEntry assigned_entry = CloneEntryWithClonedFileMeta(entry);
        if (!entry.File()->file_source) {
            return Status::Invalid(
                "This is a bug, file source field for row-tracking table must present.");
        }

        bool contains_row_id =
            entry.File()->write_cols.has_value() &&
            std::find(entry.File()->write_cols->begin(), entry.File()->write_cols->end(),
                      SpecialFields::RowId().Name()) != entry.File()->write_cols->end();

        if (entry.File()->file_source.value() == FileSource::Append() &&
            entry.File()->first_row_id == std::nullopt && !contains_row_id) {
            int64_t row_count = entry.File()->row_count;
            if (BlobUtils::IsBlobFile(entry.File()->file_name)) {
                if (!entry.File()->write_cols || entry.File()->write_cols->empty()) {
                    return Status::Invalid(fmt::format(
                        "invalid blob file {}: does not have write_cols", entry.File()->file_name));
                }
                std::string blob_field_name = entry.File()->write_cols->at(0);
                int64_t blob_start = blob_starts.count(blob_field_name)
                                         ? blob_starts[blob_field_name]
                                         : blob_start_default;
                if (blob_start >= start) {
                    return Status::Invalid(
                        fmt::format("This is a bug, blobStart {} should be less than start {} when "
                                    "assigning a blob entry file.",
                                    blob_start, start));
                }
                assigned_entry.AssignFirstRowId(blob_start);
                blob_starts[blob_field_name] = blob_start + row_count;
            } else if (IsVectorStoreFile(entry.File()->file_name)) {
                if (vector_store_start >= start) {
                    return Status::Invalid(fmt::format(
                        "This is a bug, vectorStoreStart {} should be less than start {} "
                        "when assigning a vector-store entry file.",
                        vector_store_start, start));
                }
                assigned_entry.AssignFirstRowId(vector_store_start);
                vector_store_start += row_count;
            } else {
                assigned_entry.AssignFirstRowId(start);
                blob_start_default = start;
                blob_starts.clear();
                start += row_count;
            }
        } else {
            // for compact file, do not assign first row id.
        }
        row_id_assigned->emplace_back(std::move(assigned_entry));
    }
    return start;
}

}  // namespace paimon
