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
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/range_helper.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/manifest/file_entry.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/commit/commit_scanner.h"
#include "paimon/core/operation/commit/manifest_entry_changes.h"
#include "paimon/core/operation/commit/row_id_column_conflict_checker.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/utils/range.h"
#include "paimon/utils/row_range_index.h"

namespace paimon {

namespace {

bool IsVectorStoreFile(const std::string& file_name) {
    return file_name.find(".vector.") != std::string::npos;
}

bool IsDedicatedStorageFile(const std::string& file_name) {
    return BlobUtils::IsBlobFile(file_name) || IsVectorStoreFile(file_name);
}

struct PartitionBucketKey {
    BinaryRow partition;
    int32_t bucket;

    bool operator==(const PartitionBucketKey& other) const {
        return partition == other.partition && bucket == other.bucket;
    }
};

struct PartitionBucketKeyHash {
    size_t operator()(const PartitionBucketKey& key) const {
        return std::hash<BinaryRow>()(key.partition) ^ (std::hash<int32_t>()(key.bucket) << 1);
    }
};

}  // namespace

ConflictDetection::ConflictDetection(std::shared_ptr<TableSchema> table_schema,
                                     const CoreOptions& options,
                                     std::shared_ptr<SnapshotManager> snapshot_manager,
                                     std::shared_ptr<ManifestList> manifest_list,
                                     std::shared_ptr<ManifestFile> manifest_file,
                                     std::shared_ptr<CommitScanner> commit_scanner,
                                     const std::string& commit_user, const std::string& table_name,
                                     const std::shared_ptr<FileStorePathFactory>& path_factory)
    : table_schema_(std::move(table_schema)),
      options_(options),
      snapshot_manager_(std::move(snapshot_manager)),
      manifest_list_(std::move(manifest_list)),
      manifest_file_(std::move(manifest_file)),
      commit_scanner_(std::move(commit_scanner)),
      path_factory_(path_factory),
      commit_user_(commit_user),
      table_name_(table_name) {}

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
    std::string base_commit_user = latest_snapshot.CommitUser();
    if (options_.DeletionVectorsEnabled() &&
        ResolveBucketMode(options_.GetBucket(), table_schema_) == BucketMode::BUCKET_UNAWARE) {
        return Status::NotImplemented(
            "check conflicts failed. not yet support dv with BUCKET_UNAWARE mode");
    }

    std::vector<ManifestEntry> all_entries = base_entries;
    all_entries.insert(all_entries.end(), delta_entries.begin(), delta_entries.end());
    PAIMON_RETURN_NOT_OK(CheckBucketKeepSame(all_entries, commit_kind, base_commit_user,
                                             base_entries, delta_entries));

    // check the delta, it is important not to delete and add the same file. Since scan
    // relies on map for deduplication, this may result in the loss of this file
    std::vector<ManifestEntry> merged_delta_entries;
    if (Status status = FileEntry::MergeEntries(delta_entries, &merged_delta_entries);
        !status.ok()) {
        return Status::Invalid(
            BuildConflictMessage("File deletion conflicts detected! Give up committing.",
                                 base_commit_user, base_entries, delta_entries, status.ToString()));
    }

    std::vector<ManifestEntry> merged_entries;
    // merge manifest entries and also check if the files we want to delete are still there
    if (Status status = FileEntry::MergeEntries(all_entries, &merged_entries); !status.ok()) {
        return Status::Invalid(
            BuildConflictMessage("File deletion conflicts detected! Give up committing.",
                                 base_commit_user, base_entries, delta_entries, status.ToString()));
    }
    PAIMON_RETURN_NOT_OK(
        CheckDeleteInEntries(merged_entries, base_commit_user, base_entries, delta_entries));
    PAIMON_RETURN_NOT_OK(
        CheckKeyRange(merged_entries, base_commit_user, base_entries, delta_entries));
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

Status ConflictDetection::CheckBucketKeepSame(
    const std::vector<ManifestEntry>& all_entries, const Snapshot::CommitKind& commit_kind,
    const std::string& base_commit_user, const std::vector<ManifestEntry>& base_entries,
    const std::vector<ManifestEntry>& delta_entries) const {
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

        return TotalBucketsChanged(entry.Partition(), entry.TotalBuckets(), iter->second,
                                   base_commit_user, base_entries, delta_entries);
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
    std::string part_info;
    if (table_schema_->PartitionKeys().empty()) {
        part_info = "table";
    } else {
        PAIMON_ASSIGN_OR_RAISE(std::string partition_string,
                               path_factory_->GetPartitionString(partition));
        part_info = fmt::format("partition {{{}}}", partition_string);
    }
    return Status::Invalid(fmt::format(
        "Try to write {} with a new bucket num {}, but the previous bucket num is {}. Please "
        "switch to batch mode, and perform INSERT OVERWRITE to rescale current data layout first.",
        part_info, num_buckets, previous_num_buckets));
}

Status ConflictDetection::TotalBucketsChanged(
    const BinaryRow& partition, int32_t num_buckets, int32_t previous_num_buckets,
    const std::string& base_commit_user, const std::vector<ManifestEntry>& base_entries,
    const std::vector<ManifestEntry>& delta_entries) const {
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(table_schema_->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, table_schema_->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(
        std::string partition_string,
        BinaryRowPartitionComputer::PartToSimpleString(partition_schema, partition, "-", 200,
                                                       /*legacy_partition_name_enabled=*/false));
    std::string message = fmt::format(
        "Total buckets of partition {} changed from {} to {} without overwrite. Give up "
        "committing.",
        partition_string, previous_num_buckets, num_buckets);
    return Status::Invalid(
        BuildConflictMessage(message, base_commit_user, base_entries, delta_entries));
}

std::string ConflictDetection::BuildConflictMessage(const std::string& message,
                                                    const std::string& base_commit_user,
                                                    const std::vector<ManifestEntry>& base_entries,
                                                    const std::vector<ManifestEntry>& delta_entries,
                                                    const std::string& cause) const {
    static constexpr const char* kPossibleCauses =
        "Don't panic!\n"
        "Conflicts during commits are normal and this failure is intended to resolve the "
        "conflicts.\n"
        "Conflicts are mainly caused by the following scenarios:\n"
        "1. Multiple jobs are writing into the same partition at the same time, or you use "
        "STATEMENT SET to execute multiple INSERT statements into the same Paimon table.\n"
        "   You'll probably see different base commit user and current commit user below.\n"
        "   You can use dedicated compaction job to support multiple writing.\n"
        "2. You're recovering from an old savepoint, or you're creating multiple jobs from a "
        "savepoint.\n"
        "   The job will fail continuously in this scenario to protect metadata from corruption.\n"
        "   You can either recover from the latest savepoint, or you can revert the table to the "
        "snapshot corresponding to the old savepoint.";

    constexpr size_t kMaxEntry = 50;
    auto join_entries = [](const std::vector<ManifestEntry>& entries,
                           size_t max_entry) -> std::string {
        std::string joined;
        size_t limit = std::min(entries.size(), max_entry);
        for (size_t i = 0; i < limit; ++i) {
            if (i > 0) {
                joined += "\n";
            }
            joined += entries[i].ToString();
        }
        return joined;
    };

    std::string commit_user_string = fmt::format(
        "Base commit user is: {}; Current commit user is: {}", base_commit_user, commit_user_);
    std::string base_entries_string = "Base entries are:\n" + join_entries(base_entries, kMaxEntry);
    std::string changes_string = "Changes are:\n" + join_entries(delta_entries, kMaxEntry);

    std::string result = fmt::format("{}\n\n{}\n\n{}\n\n{}\n\n{}", message, kPossibleCauses,
                                     commit_user_string, base_entries_string, changes_string);
    if (base_entries.size() > kMaxEntry || delta_entries.size() > kMaxEntry) {
        result +=
            "\n\nThe entry list above are not fully displayed, please refer to logs for more "
            "information.";
    }
    if (!cause.empty()) {
        result += "\n\nCaused by: " + cause;
    }
    return result;
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
    const std::vector<ManifestEntry>& merged_entries, const std::string& base_commit_user,
    const std::vector<ManifestEntry>& base_entries,
    const std::vector<ManifestEntry>& delta_entries) const {
    for (const auto& entry : merged_entries) {
        if (entry.Kind() == FileKind::Delete()) {
            std::string message = fmt::format(
                "File deletion conflicts detected! Give up committing. Trying to delete file {} "
                "for table {} which is not previously added.",
                entry.FileName(), table_name_);
            return Status::Invalid(
                BuildConflictMessage(message, base_commit_user, base_entries, delta_entries));
        }
    }

    return Status::OK();
}

Status ConflictDetection::CheckKeyRange(const std::vector<ManifestEntry>& merged_entries,
                                        const std::string& base_commit_user,
                                        const std::vector<ManifestEntry>& base_entries,
                                        const std::vector<ManifestEntry>& delta_entries) const {
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
                PAIMON_ASSIGN_OR_RAISE(std::string a_partition_string,
                                       path_factory_->GetPartitionString(a.Partition()));
                PAIMON_ASSIGN_OR_RAISE(std::string b_partition_string,
                                       path_factory_->GetPartitionString(b.Partition()));
                std::string message = fmt::format(
                    "LSM conflicts detected! Give up committing. Conflict files are:\n"
                    "{}, bucket {}, level {}, file {}\n"
                    "{}, bucket {}, level {}, file {}",
                    a_partition_string, a.Bucket(), a.Level(), a.FileName(), b_partition_string,
                    b.Bucket(), b.Level(), b.FileName());
                return Status::Invalid(
                    BuildConflictMessage(message, base_commit_user, base_entries, delta_entries));
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

    std::vector<Range> existing_data_ranges;
    existing_data_ranges.reserve(base_entries.size());
    for (const ManifestEntry& entry : base_entries) {
        if (!entry.File()->first_row_id || IsDedicatedStorageFile(entry.FileName())) {
            continue;
        }
        int64_t range_from = entry.File()->first_row_id.value();
        int64_t range_to = range_from + entry.File()->row_count - 1;
        existing_data_ranges.emplace_back(range_from, range_to);
    }

    PAIMON_ASSIGN_OR_RAISE(RowRangeIndex existing_index,
                           RowRangeIndex::Create(existing_data_ranges,
                                                 /*merge_adjacent=*/false));

    for (const ManifestEntry& entry : files_to_check) {
        int64_t range_from = entry.File()->first_row_id.value();
        int64_t range_to = range_from + entry.File()->row_count - 1;
        Range row_range(range_from, range_to);

        bool exists = false;
        if (IsDedicatedStorageFile(entry.FileName())) {
            exists = existing_index.Contains(row_range);
        } else {
            exists = existing_index.ContainsExactly(row_range);
        }

        if (!exists) {
            return Status::Invalid(fmt::format(
                "Row ID existence conflict: file '{}' references firstRowId={}, rowCount={} in "
                "bucket {}, but no matching file exists in the current snapshot. The referenced "
                "file may have been rewritten by a concurrent compaction or removed by an "
                "overwrite.",
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

    std::vector<ManifestEntry> entries_with_ranges;
    entries_with_ranges.reserve(merged_entries.size());
    for (const ManifestEntry& entry : merged_entries) {
        if (entry.File()->first_row_id) {
            entries_with_ranges.push_back(entry);
        }
    }
    if (entries_with_ranges.empty()) {
        return Status::OK();
    }

    RangeHelper<ManifestEntry> range_helper(
        [](const ManifestEntry& entry) -> Result<int64_t> {
            return entry.File()->first_row_id.value();
        },
        [](const ManifestEntry& entry) -> Result<int64_t> {
            return entry.File()->first_row_id.value() + entry.File()->row_count - 1;
        });
    std::vector<ManifestEntry> data_files;
    std::vector<ManifestEntry> dedicated_files;
    data_files.reserve(entries_with_ranges.size());
    dedicated_files.reserve(entries_with_ranges.size());
    for (const ManifestEntry& entry : entries_with_ranges) {
        if (IsDedicatedStorageFile(entry.FileName())) {
            dedicated_files.push_back(entry);
        } else {
            data_files.push_back(entry);
        }
    }

    PAIMON_RETURN_NOT_OK(CheckDataFileRowIdRangeConflicts(range_helper, data_files));
    PAIMON_RETURN_NOT_OK(CheckDedicatedFileRowIdRangeConflicts(data_files, dedicated_files));

    return Status::OK();
}

Status ConflictDetection::CheckDataFileRowIdRangeConflicts(
    RangeHelper<ManifestEntry>& range_helper, const std::vector<ManifestEntry>& data_files) const {
    std::vector<ManifestEntry> data_files_copy = data_files;
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<ManifestEntry>> data_file_groups,
                           range_helper.MergeOverlappingRanges(std::move(data_files_copy)));
    for (const std::vector<ManifestEntry>& data_file_group : data_file_groups) {
        PAIMON_ASSIGN_OR_RAISE(bool all_data_ranges_same,
                               range_helper.AreAllRangesSame(data_file_group));
        if (!all_data_ranges_same) {
            std::string data_files_str;
            for (size_t i = 0; i < data_file_group.size(); ++i) {
                if (i > 0) {
                    data_files_str += ", ";
                }
                data_files_str += data_file_group[i].ToString();
            }
            return Status::Invalid(fmt::format(
                "For Data Evolution table, multiple 'MERGE INTO' and 'COMPACT' operations have "
                "encountered conflicts, data files: [{}]",
                data_files_str));
        }
    }

    return Status::OK();
}

Status ConflictDetection::CheckDedicatedFileRowIdRangeConflicts(
    const std::vector<ManifestEntry>& data_files,
    const std::vector<ManifestEntry>& dedicated_files) const {
    if (dedicated_files.empty()) {
        return Status::OK();
    }

    std::vector<Range> data_ranges;
    data_ranges.reserve(data_files.size());
    for (const ManifestEntry& data_file : data_files) {
        int64_t data_range_from = data_file.File()->first_row_id.value();
        int64_t data_range_to = data_range_from + data_file.File()->row_count - 1;
        data_ranges.emplace_back(data_range_from, data_range_to);
    }

    PAIMON_ASSIGN_OR_RAISE(RowRangeIndex data_file_row_range_index,
                           RowRangeIndex::Create(data_ranges, /*merge_adjacent=*/false));

    for (const ManifestEntry& dedicated_file : dedicated_files) {
        int64_t dedicated_from = dedicated_file.File()->first_row_id.value();
        int64_t dedicated_to = dedicated_from + dedicated_file.File()->row_count - 1;
        Range dedicated_range(dedicated_from, dedicated_to);

        std::vector<Range> intersecting_ranges =
            data_file_row_range_index.IntersectedRanges(dedicated_range.from, dedicated_range.to);
        bool covered_by_one_data_range = intersecting_ranges.size() == 1 &&
                                         intersecting_ranges[0].from <= dedicated_range.from &&
                                         intersecting_ranges[0].to >= dedicated_range.to;
        if (!covered_by_one_data_range) {
            std::string conflict_reason = intersecting_ranges.size() > 1
                                              ? "spans multiple data file ranges"
                                              : "is not covered by one data file range";
            std::string intersecting_files_str;
            bool first = true;
            for (const ManifestEntry& data_file : data_files) {
                int64_t data_from = data_file.File()->first_row_id.value();
                int64_t data_to = data_from + data_file.File()->row_count - 1;
                if (data_from <= dedicated_range.to && dedicated_range.from <= data_to) {
                    if (!first) {
                        intersecting_files_str += ", ";
                    }
                    intersecting_files_str += data_file.ToString();
                    first = false;
                }
            }
            return Status::Invalid(fmt::format(
                "For Data Evolution table, multiple 'MERGE INTO' and 'COMPACT' operations have "
                "encountered conflicts, dedicated file {} {} {}: [{}]",
                dedicated_file.ToString(), dedicated_range.ToString(), conflict_reason,
                intersecting_files_str));
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
        !row_id_column_conflict_checker || !row_id_column_conflict_checker.value() ||
        row_id_column_conflict_checker.value()->IsEmpty()) {
        return Status::OK();
    }

    if (row_id_check_from_snapshot_.value() > latest_snapshot.Id()) {
        return Status::OK();
    }

    PAIMON_ASSIGN_OR_RAISE(Snapshot check_snapshot,
                           snapshot_manager_->LoadSnapshot(row_id_check_from_snapshot_.value()));
    if (!check_snapshot.NextRowId()) {
        return Status::Invalid(fmt::format("Next row id cannot be null for snapshot {}.",
                                           row_id_check_from_snapshot_.value()));
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

        PAIMON_ASSIGN_OR_RAISE(
            std::vector<ManifestEntry> history_entries,
            commit_scanner_->ReadIncrementalEntries(snapshot, changed_partitions));
        for (const ManifestEntry& history_entry : history_entries) {
            if (!(history_entry.Kind() == FileKind::Add()) || !history_entry.File()->first_row_id ||
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
                    "For Data Evolution table, multiple 'MERGE INTO' operations have "
                    "encountered conflicts, updating the same file, which can render some "
                    "updates ineffective.");
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

    std::unordered_map<PartitionBucketKey, std::vector<Range>, PartitionBucketKeyHash>
        data_ranges_by_group;
    for (const ManifestEntry& base_entry : base_entries) {
        if (!(base_entry.Kind() == FileKind::Add()) || !base_entry.File()->first_row_id) {
            continue;
        }

        int64_t first_row_id = base_entry.File()->first_row_id.value();
        int64_t last_row_id = first_row_id + base_entry.File()->row_count - 1;
        data_ranges_by_group[{base_entry.Partition(), base_entry.Bucket()}].emplace_back(
            first_row_id, last_row_id);
    }

    std::unordered_map<PartitionBucketKey, RowRangeIndex, PartitionBucketKeyHash>
        range_index_by_group;
    range_index_by_group.reserve(data_ranges_by_group.size());
    for (const auto& [group, data_ranges] : data_ranges_by_group) {
        PAIMON_ASSIGN_OR_RAISE(RowRangeIndex row_range_index,
                               RowRangeIndex::Create(data_ranges, /*merge_adjacent=*/true));
        range_index_by_group.emplace(group, std::move(row_range_index));
    }

    for (const IndexManifestEntry& index_entry : indexes_to_check) {
        PartitionBucketKey group_key{index_entry.partition, index_entry.bucket};
        auto group_iter = range_index_by_group.find(group_key);
        if (group_iter == range_index_by_group.end()) {
            return Status::Invalid(fmt::format(
                "Global index row ID existence conflict: index file '{}' references row range {}, "
                "but this range is not fully covered by current data files. The referenced row "
                "IDs may have been reassigned or removed by a concurrent commit.",
                index_entry.index_file->FileName(),
                Range(index_entry.index_file->GetGlobalIndexMeta().value().row_range_start,
                      index_entry.index_file->GetGlobalIndexMeta().value().row_range_end)
                    .ToString()));
        }

        const GlobalIndexMeta& global_index = index_entry.index_file->GetGlobalIndexMeta().value();
        Range index_range(global_index.row_range_start, global_index.row_range_end);

        std::vector<Range> intersected =
            group_iter->second.IntersectedRanges(index_range.from, index_range.to);
        bool covered = intersected.size() == 1 && intersected[0].from <= index_range.from &&
                       intersected[0].to >= index_range.to;
        if (!covered) {
            return Status::Invalid(fmt::format(
                "Global index row ID existence conflict: index file '{}' references row range {}, "
                "but this range is not fully covered by current data files. The referenced row "
                "IDs may have been reassigned or removed by a concurrent commit.",
                index_entry.index_file->FileName(), index_range.ToString()));
        }
    }

    return Status::OK();
}

}  // namespace paimon
