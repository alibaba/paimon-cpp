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

#include "paimon/core/operation/commit/conflict_detection.h"

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <set>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/manifest/file_entry.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/commit/manifest_entry_changes.h"
#include "paimon/core/operation/commit/row_id_column_conflict_checker.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/utils/range.h"

namespace paimon {

namespace {

bool IsVectorStoreFile(const std::string& file_name) {
    return file_name.find(".vector.") != std::string::npos;
}

bool IsDedicatedStorageFile(const std::string& file_name) {
    return BlobUtils::IsBlobFile(file_name) || IsVectorStoreFile(file_name);
}

}  // namespace

ConflictDetection::ConflictDetection(std::shared_ptr<TableSchema> table_schema,
                                     const CoreOptions& options,
                                     std::shared_ptr<SnapshotManager> snapshot_manager,
                                     std::shared_ptr<ManifestList> manifest_list,
                                     std::shared_ptr<ManifestFile> manifest_file)
    : table_schema_(std::move(table_schema)),
      options_(options),
      snapshot_manager_(std::move(snapshot_manager)),
      manifest_list_(std::move(manifest_list)),
      manifest_file_(std::move(manifest_file)) {}

void ConflictDetection::SetRowIdCheckFromSnapshot(
    const std::optional<int64_t>& row_id_check_from_snapshot) {
    row_id_check_from_snapshot_ = row_id_check_from_snapshot;
}

bool ConflictDetection::HasRowIdCheckFromSnapshot() const {
    return row_id_check_from_snapshot_.has_value();
}

Status ConflictDetection::CheckConflicts(
    const Snapshot& latest_snapshot, const std::vector<ManifestEntry>& base_entries,
    const std::vector<ManifestEntry>& delta_entries,
    const std::vector<IndexManifestEntry>& delta_index_entries,
    const std::optional<std::shared_ptr<RowIdColumnConflictChecker>>&
        row_id_column_conflict_checker,
    const Snapshot::CommitKind& commit_kind) const {
    if (options_.DeletionVectorsEnabled() &&
        ResolveBucketMode(options_.GetBucket(), table_schema_) == BucketMode::BUCKET_UNAWARE) {
        return Status::NotImplemented(
            "check conflicts failed. not yet support dv with BUCKET_UNAWARE mode");
    }

    std::vector<ManifestEntry> all_entries = base_entries;
    all_entries.insert(all_entries.end(), delta_entries.begin(), delta_entries.end());
    PAIMON_RETURN_NOT_OK(CheckBucketKeepSame(all_entries, commit_kind));

    // check the delta, it is important not to delete and add the same file. Since scan
    // relies on map for deduplication, this may result in the loss of this file
    std::vector<ManifestEntry> merged_delta_entries;
    PAIMON_RETURN_NOT_OK(FileEntry::MergeEntries(delta_entries, &merged_delta_entries));

    std::vector<ManifestEntry> merged_entries;
    // merge manifest entries and also check if the files we want to delete are still there
    PAIMON_RETURN_NOT_OK(FileEntry::MergeEntries(all_entries, &merged_entries));
    PAIMON_RETURN_NOT_OK(CheckDeleteInEntries(merged_entries));
    PAIMON_RETURN_NOT_OK(CheckKeyRange(merged_entries));
    if (commit_kind != Snapshot::CommitKind::Compact()) {
        PAIMON_RETURN_NOT_OK(
            CheckRowIdExistence(base_entries, delta_entries, latest_snapshot.NextRowId()));
    }
    PAIMON_RETURN_NOT_OK(CheckRowIdRangeConflicts(commit_kind, merged_entries));
    PAIMON_RETURN_NOT_OK(CheckGlobalIndexRowIdExistence(base_entries, delta_index_entries));
    PAIMON_RETURN_NOT_OK(CheckForRowIdFromSnapshot(
        latest_snapshot, delta_entries, delta_index_entries, row_id_column_conflict_checker));
    return Status::OK();
}

bool ConflictDetection::ShouldBeOverwriteCommit(
    const std::vector<ManifestEntry>& append_table_files,
    const std::vector<IndexManifestEntry>& append_index_files) const {
    for (const ManifestEntry& entry : append_table_files) {
        if (entry.Kind() == FileKind::Delete()) {
            return true;
        }
    }

    for (const IndexManifestEntry& entry : append_index_files) {
        if (entry.index_file->IndexType() == DeletionVectorsIndexFile::DELETION_VECTORS_INDEX) {
            return true;
        }
    }

    return false;
}

Status ConflictDetection::CheckBucketKeepSame(const std::vector<ManifestEntry>& all_entries,
                                              const Snapshot::CommitKind& commit_kind) const {
    if (commit_kind == Snapshot::CommitKind::Overwrite()) {
        return Status::OK();
    }

    // total buckets within the same partition should remain the same
    std::unordered_map<BinaryRow, int32_t> total_buckets;
    for (const ManifestEntry& entry : all_entries) {
        if (entry.TotalBuckets() <= 0) {
            continue;
        }
        if (same_bucket_checked_partitions_.find(entry.Partition()) !=
            same_bucket_checked_partitions_.end()) {
            continue;
        }

        auto [iter, inserted] = total_buckets.emplace(entry.Partition(), entry.TotalBuckets());
        if (inserted || iter->second == entry.TotalBuckets()) {
            continue;
        }

        return BucketNumMismatch(entry.Partition(), entry.TotalBuckets(), iter->second);
    }

    MarkBucketCheckedPartitions(total_buckets);
    return Status::OK();
}

Status ConflictDetection::CollectUncheckedBucketPartitions(
    const std::vector<ManifestEntry>& delta_entries,
    std::unordered_map<BinaryRow, int32_t>* total_buckets) const {
    total_buckets->clear();
    for (const ManifestEntry& entry : delta_entries) {
        if (!(entry.Kind() == FileKind::Add()) || entry.TotalBuckets() <= 0 ||
            same_bucket_checked_partitions_.find(entry.Partition()) !=
                same_bucket_checked_partitions_.end()) {
            continue;
        }

        auto [iter, inserted] = total_buckets->emplace(entry.Partition(), entry.TotalBuckets());
        if (!inserted && iter->second != entry.TotalBuckets()) {
            return BucketNumMismatch(entry.Partition(), entry.TotalBuckets(), iter->second);
        }
    }

    return Status::OK();
}

Status ConflictDetection::CheckSameBucketByTotalBuckets(
    const std::unordered_map<BinaryRow, int32_t>& expected_total_buckets,
    const std::unordered_map<BinaryRow, int32_t>& previous_total_buckets) const {
    for (const auto& [partition, total_buckets] : expected_total_buckets) {
        auto iter = previous_total_buckets.find(partition);
        if (iter != previous_total_buckets.end() && iter->second != total_buckets) {
            return BucketNumMismatch(partition, total_buckets, iter->second);
        }
    }

    MarkBucketCheckedPartitions(expected_total_buckets);
    return Status::OK();
}

Status ConflictDetection::BucketNumMismatch(const BinaryRow& partition, int32_t num_buckets,
                                            int32_t previous_num_buckets) const {
    return Status::Invalid(fmt::format(
        "Total buckets of partition {} changed from {} to {} without overwrite. Give up "
        "committing.",
        partition.ToString(), previous_num_buckets, num_buckets));
}

void ConflictDetection::MarkBucketCheckedPartitions(
    const std::unordered_map<BinaryRow, int32_t>& total_buckets) const {
    if (total_buckets.empty()) {
        return;
    }

    for (const auto& [partition, _] : total_buckets) {
        same_bucket_checked_partitions_.insert_or_assign(partition, true);
        while (same_bucket_checked_partitions_.size() > kSameBucketCheckCacheMaxSize) {
            same_bucket_checked_partitions_.erase(same_bucket_checked_partitions_.begin()->first);
        }
    }
}

Status ConflictDetection::CheckDeleteInEntries(
    const std::vector<ManifestEntry>& merged_entries) const {
    for (const auto& entry : merged_entries) {
        if (entry.Kind() == FileKind::Delete()) {
            return Status::Invalid(fmt::format(
                "Trying to delete file {} which is not previously added.", entry.FileName()));
        }
    }

    return Status::OK();
}

Status ConflictDetection::CheckKeyRange(const std::vector<ManifestEntry>& merged_entries) const {
    if (table_schema_->PrimaryKeys().empty()) {
        return Status::OK();
    }

    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> trimmed_primary_key_fields,
                           table_schema_->TrimmedPrimaryKeyFields());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldsComparator> key_comparator,
                           FieldsComparator::Create(trimmed_primary_key_fields,
                                                    options_.SequenceFieldSortOrderIsAscending()));

    // group entries by partitions, buckets and levels
    std::unordered_map<std::tuple<BinaryRow, int32_t, int32_t>, std::vector<ManifestEntry>> levels;
    for (const auto& entry : merged_entries) {
        if (!(entry.Kind() == FileKind::Add())) {
            continue;
        }
        int32_t level = entry.Level();
        if (level < 1) {
            continue;
        }

        levels[std::make_tuple(entry.Partition(), entry.Bucket(), level)].push_back(entry);
    }

    // check for all LSM level >= 1, key ranges of files do not intersect
    for (auto& [_, entries] : levels) {
        std::sort(entries.begin(), entries.end(),
                  [&key_comparator](const ManifestEntry& a, const ManifestEntry& b) {
                      return key_comparator->CompareTo(a.MinKey(), b.MinKey()) < 0;
                  });
        for (size_t i = 0; i + 1 < entries.size(); ++i) {
            const ManifestEntry& a = entries[i];
            const ManifestEntry& b = entries[i + 1];
            if (key_comparator->CompareTo(a.MaxKey(), b.MinKey()) >= 0) {
                return Status::Invalid(fmt::format(
                    "LSM conflicts detected! Give up committing. Conflict files are {} and {}.",
                    a.FileName(), b.FileName()));
            }
        }
    }
    return Status::OK();
}

Status ConflictDetection::CheckRowIdExistence(const std::vector<ManifestEntry>& base_entries,
                                              const std::vector<ManifestEntry>& delta_entries,
                                              const std::optional<int64_t>& next_row_id) const {
    if (!options_.DataEvolutionEnabled()) {
        return Status::OK();
    }

    std::vector<ManifestEntry> files_to_check;
    files_to_check.reserve(delta_entries.size());
    for (const ManifestEntry& entry : delta_entries) {
        if (!(entry.Kind() == FileKind::Add()) || !entry.File()->first_row_id || !next_row_id ||
            entry.File()->first_row_id.value() >= next_row_id.value()) {
            continue;
        }
        files_to_check.push_back(entry);
    }
    if (files_to_check.empty()) {
        return Status::OK();
    }

    std::vector<Range> existing_ranges;
    std::set<std::pair<int64_t, int64_t>> exact_ranges;
    existing_ranges.reserve(base_entries.size());
    for (const ManifestEntry& entry : base_entries) {
        if (!entry.File()->first_row_id || IsDedicatedStorageFile(entry.FileName())) {
            continue;
        }
        int64_t range_from = entry.File()->first_row_id.value();
        int64_t range_to = range_from + entry.File()->row_count - 1;
        existing_ranges.emplace_back(range_from, range_to);
        exact_ranges.emplace(range_from, range_to);
    }
    std::vector<Range> merged_ranges = Range::SortAndMergeOverlap(existing_ranges,
                                                                  /*adjacent=*/false);

    for (const ManifestEntry& entry : files_to_check) {
        int64_t range_from = entry.File()->first_row_id.value();
        int64_t range_to = range_from + entry.File()->row_count - 1;
        Range row_range(range_from, range_to);

        bool exists = false;
        if (IsDedicatedStorageFile(entry.FileName())) {
            for (const Range& existing_range : merged_ranges) {
                if (existing_range.from <= row_range.from && existing_range.to >= row_range.to) {
                    exists = true;
                    break;
                }
            }
        } else {
            exists = exact_ranges.find({row_range.from, row_range.to}) != exact_ranges.end();
        }

        if (!exists) {
            return Status::Invalid(fmt::format(
                "Row ID existence conflict: file '{}' references firstRowId={}, rowCount={} in "
                "bucket {}, but no matching file exists in the current snapshot.",
                entry.FileName(), entry.File()->first_row_id.value(), entry.File()->row_count,
                entry.Bucket()));
        }
    }

    return Status::OK();
}

Status ConflictDetection::CheckRowIdRangeConflicts(
    const Snapshot::CommitKind& commit_kind,
    const std::vector<ManifestEntry>& merged_entries) const {
    if (!options_.DataEvolutionEnabled()) {
        return Status::OK();
    }
    if (!row_id_check_from_snapshot_ && !(commit_kind == Snapshot::CommitKind::Compact())) {
        return Status::OK();
    }

    std::vector<std::pair<Range, ManifestEntry>> entries_with_ranges;
    entries_with_ranges.reserve(merged_entries.size());
    for (const ManifestEntry& entry : merged_entries) {
        if (!entry.File()->first_row_id) {
            continue;
        }
        int64_t range_from = entry.File()->first_row_id.value();
        int64_t range_to = range_from + entry.File()->row_count - 1;
        entries_with_ranges.emplace_back(Range(range_from, range_to), entry);
    }
    if (entries_with_ranges.empty()) {
        return Status::OK();
    }

    std::sort(entries_with_ranges.begin(), entries_with_ranges.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first.from < rhs.first.from; });

    size_t group_start = 0;
    int64_t group_max_to = entries_with_ranges[0].first.to;
    for (size_t i = 1; i <= entries_with_ranges.size(); ++i) {
        bool overlap_group_end =
            (i == entries_with_ranges.size()) || (entries_with_ranges[i].first.from > group_max_to);
        if (!overlap_group_end) {
            group_max_to = std::max(group_max_to, entries_with_ranges[i].first.to);
            continue;
        }

        std::optional<std::pair<int64_t, int64_t>> expected_range;
        for (size_t j = group_start; j < i; ++j) {
            const ManifestEntry& entry = entries_with_ranges[j].second;
            if (IsDedicatedStorageFile(entry.FileName())) {
                continue;
            }
            std::pair<int64_t, int64_t> current = {entries_with_ranges[j].first.from,
                                                   entries_with_ranges[j].first.to};
            if (!expected_range) {
                expected_range = current;
            } else if (expected_range.value() != current) {
                return Status::Invalid(
                    "For Data Evolution table, multiple MERGE INTO/COMPACT operations have "
                    "encountered row-id range conflicts.");
            }
        }

        if (i < entries_with_ranges.size()) {
            group_start = i;
            group_max_to = entries_with_ranges[i].first.to;
        }
    }

    return Status::OK();
}

Status ConflictDetection::CheckForRowIdFromSnapshot(
    const Snapshot& latest_snapshot, const std::vector<ManifestEntry>& delta_entries,
    const std::vector<IndexManifestEntry>& delta_index_entries,
    const std::optional<std::shared_ptr<RowIdColumnConflictChecker>>&
        row_id_column_conflict_checker) const {
    if (!options_.DataEvolutionEnabled() || !row_id_check_from_snapshot_ || !snapshot_manager_ ||
        !manifest_list_ || !manifest_file_ || !row_id_column_conflict_checker ||
        !row_id_column_conflict_checker.value() ||
        row_id_column_conflict_checker.value()->IsEmpty()) {
        return Status::OK();
    }

    if (row_id_check_from_snapshot_.value() > latest_snapshot.Id()) {
        return Status::OK();
    }

    PAIMON_ASSIGN_OR_RAISE(Snapshot check_snapshot,
                           snapshot_manager_->LoadSnapshot(row_id_check_from_snapshot_.value()));
    if (!check_snapshot.NextRowId()) {
        return Status::OK();
    }
    int64_t check_next_row_id = check_snapshot.NextRowId().value();

    int64_t from_snapshot_id = row_id_check_from_snapshot_.value() + 1;
    if (from_snapshot_id < Snapshot::FIRST_SNAPSHOT_ID) {
        from_snapshot_id = Snapshot::FIRST_SNAPSHOT_ID;
    }
    if (from_snapshot_id > latest_snapshot.Id()) {
        return Status::OK();
    }

    std::vector<BinaryRow> changed_partitions =
        ManifestEntryChanges::ChangedPartitions(delta_entries, delta_index_entries);
    if (changed_partitions.empty()) {
        return Status::OK();
    }
    std::unordered_set<BinaryRow> changed_partition_set(changed_partitions.begin(),
                                                        changed_partitions.end());

    for (int64_t snapshot_id = from_snapshot_id; snapshot_id <= latest_snapshot.Id();
         ++snapshot_id) {
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, snapshot_manager_->LoadSnapshot(snapshot_id));
        if (snapshot.GetCommitKind() == Snapshot::CommitKind::Compact()) {
            continue;
        }

        std::vector<ManifestFileMeta> delta_manifests;
        PAIMON_RETURN_NOT_OK(manifest_list_->ReadDeltaManifests(snapshot, &delta_manifests));
        for (const ManifestFileMeta& manifest_meta : delta_manifests) {
            std::vector<ManifestEntry> history_entries;
            PAIMON_RETURN_NOT_OK(manifest_file_->Read(manifest_meta.FileName(), /*filter=*/nullptr,
                                                      &history_entries));
            for (const ManifestEntry& history_entry : history_entries) {
                if (!(history_entry.Kind() == FileKind::Add()) ||
                    !history_entry.File()->first_row_id ||
                    changed_partition_set.find(history_entry.Partition()) ==
                        changed_partition_set.end()) {
                    continue;
                }
                int64_t history_first_row_id = history_entry.File()->first_row_id.value();
                if (history_first_row_id >= check_next_row_id) {
                    continue;
                }
                PAIMON_ASSIGN_OR_RAISE(
                    bool conflicts,
                    row_id_column_conflict_checker.value()->ConflictsWith(history_entry.File()));
                if (conflicts) {
                    return Status::Invalid(
                        "For Data Evolution table, multiple MERGE INTO operations have "
                        "encountered conflicts while checking row-id history from "
                        "snapshot.");
                }
            }
        }
    }

    return Status::OK();
}

Status ConflictDetection::CheckGlobalIndexRowIdExistence(
    const std::vector<ManifestEntry>& base_entries,
    const std::vector<IndexManifestEntry>& delta_index_entries) const {
    if (!options_.DataEvolutionEnabled()) {
        return Status::OK();
    }

    std::vector<IndexManifestEntry> indexes_to_check;
    for (const IndexManifestEntry& index_entry : delta_index_entries) {
        if (!(index_entry.kind == FileKind::Add()) ||
            !index_entry.index_file->GetGlobalIndexMeta()) {
            continue;
        }
        indexes_to_check.push_back(index_entry);
    }
    if (indexes_to_check.empty()) {
        return Status::OK();
    }

    for (const IndexManifestEntry& index_entry : indexes_to_check) {
        std::vector<Range> data_ranges;
        for (const ManifestEntry& base_entry : base_entries) {
            if (!(base_entry.Kind() == FileKind::Add()) ||
                !(base_entry.Partition() == index_entry.partition) ||
                base_entry.Bucket() != index_entry.bucket || !base_entry.File()->first_row_id) {
                continue;
            }

            int64_t first_row_id = base_entry.File()->first_row_id.value();
            data_ranges.emplace_back(first_row_id, first_row_id + base_entry.File()->row_count - 1);
        }

        const GlobalIndexMeta& global_index = index_entry.index_file->GetGlobalIndexMeta().value();
        Range index_range(global_index.row_range_start, global_index.row_range_end);
        std::vector<Range> merged_ranges = Range::SortAndMergeOverlap(data_ranges,
                                                                      /*adjacent=*/true);
        bool covered = false;
        for (const Range& data_range : merged_ranges) {
            if (data_range.from <= index_range.from && data_range.to >= index_range.to) {
                covered = true;
                break;
            }
        }
        if (!covered) {
            return Status::Invalid(fmt::format(
                "Global index row ID existence conflict: index file '{}' references row range {}, "
                "but this range is not fully covered by current data files.",
                index_entry.index_file->FileName(), index_range.ToString()));
        }
    }

    return Status::OK();
}

}  // namespace paimon
