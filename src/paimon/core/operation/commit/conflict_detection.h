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

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/range_helper.h"
#include "paimon/core/core_options.h"
#include "paimon/core/snapshot.h"
#include "paimon/status.h"

namespace paimon {

class ManifestEntry;
struct IndexManifestEntry;
class CommitScanner;
class FileStorePathFactory;
class ManifestFile;
class ManifestList;
class RowIdColumnConflictChecker;
class SnapshotManager;
class TableSchema;

/// Util class for detecting conflicts between base and delta files.
class ConflictDetection {
 public:
    ConflictDetection(std::shared_ptr<TableSchema> table_schema, const CoreOptions& options,
                      std::shared_ptr<SnapshotManager> snapshot_manager,
                      std::shared_ptr<ManifestList> manifest_list,
                      std::shared_ptr<ManifestFile> manifest_file,
                      std::shared_ptr<CommitScanner> commit_scanner, const std::string& commit_user,
                      const std::string& table_name,
                      const std::shared_ptr<FileStorePathFactory>& path_factory);

    Status CheckConflicts(const Snapshot& latest_snapshot,
                          const std::vector<ManifestEntry>& base_entries,
                          const std::vector<ManifestEntry>& delta_entries,
                          const std::vector<IndexManifestEntry>& delta_index_entries,
                          const std::optional<std::shared_ptr<RowIdColumnConflictChecker>>&
                              row_id_column_conflict_checker,
                          const Snapshot::CommitKind& commit_kind) const;

    void SetRowIdCheckFromSnapshot(const std::optional<int64_t>& row_id_check_from_snapshot);

    bool HasRowIdCheckFromSnapshot() const;

    bool ShouldBeOverwriteCommit(const std::vector<ManifestEntry>& append_table_files,
                                 const std::vector<IndexManifestEntry>& append_index_files) const;

    Status CollectUncheckedBucketPartitions(
        const std::vector<ManifestEntry>& delta_entries,
        std::unordered_map<BinaryRow, int32_t>* total_buckets) const;

    Status CheckSameBucketByTotalBuckets(
        const std::unordered_map<BinaryRow, int32_t>& expected_total_buckets,
        const std::unordered_map<BinaryRow, int32_t>& previous_total_buckets) const;

 private:
    Status CheckBucketKeepSame(const std::vector<ManifestEntry>& all_entries,
                               const Snapshot::CommitKind& commit_kind,
                               const std::string& base_commit_user,
                               const std::vector<ManifestEntry>& base_entries,
                               const std::vector<ManifestEntry>& delta_entries) const;

    Status BucketNumMismatch(const BinaryRow& partition, int32_t num_buckets,
                             int32_t previous_num_buckets) const;

    Status TotalBucketsChanged(const BinaryRow& partition, int32_t num_buckets,
                               int32_t previous_num_buckets, const std::string& base_commit_user,
                               const std::vector<ManifestEntry>& base_entries,
                               const std::vector<ManifestEntry>& delta_entries) const;

    std::string BuildConflictMessage(const std::string& message,
                                     const std::string& base_commit_user,
                                     const std::vector<ManifestEntry>& base_entries,
                                     const std::vector<ManifestEntry>& delta_entries,
                                     const std::string& cause = "") const;

    void MarkBucketCheckedPartitions(
        const std::unordered_map<BinaryRow, int32_t>& total_buckets) const;

    Status CheckDeleteInEntries(const std::vector<ManifestEntry>& merged_entries,
                                const std::string& base_commit_user,
                                const std::vector<ManifestEntry>& base_entries,
                                const std::vector<ManifestEntry>& delta_entries) const;

    Status CheckKeyRange(const std::vector<ManifestEntry>& merged_entries,
                         const std::string& base_commit_user,
                         const std::vector<ManifestEntry>& base_entries,
                         const std::vector<ManifestEntry>& delta_entries) const;

    Status CheckRowIdExistence(const std::vector<ManifestEntry>& base_entries,
                               const std::vector<ManifestEntry>& delta_entries,
                               const std::optional<int64_t>& next_row_id) const;

    Status CheckRowIdRangeConflicts(const Snapshot::CommitKind& commit_kind,
                                    const std::vector<ManifestEntry>& merged_entries) const;

    Status CheckDataFileRowIdRangeConflicts(RangeHelper<ManifestEntry>& range_helper,
                                            const std::vector<ManifestEntry>& data_files) const;

    Status CheckDedicatedFileRowIdRangeConflicts(
        const std::vector<ManifestEntry>& data_files,
        const std::vector<ManifestEntry>& dedicated_files) const;

    Status CheckForRowIdFromSnapshot(
        const Snapshot& latest_snapshot, const std::vector<ManifestEntry>& delta_entries,
        const std::vector<IndexManifestEntry>& delta_index_entries,
        const std::optional<std::shared_ptr<RowIdColumnConflictChecker>>&
            row_id_column_conflict_checker) const;

    Status CheckGlobalIndexRowIdExistence(
        const std::vector<ManifestEntry>& base_entries,
        const std::vector<IndexManifestEntry>& delta_index_entries) const;

 private:
    static constexpr size_t kSameBucketCheckCacheMaxSize = 1000;

    std::shared_ptr<TableSchema> table_schema_;
    CoreOptions options_;
    std::optional<int64_t> row_id_check_from_snapshot_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<ManifestList> manifest_list_;
    std::shared_ptr<ManifestFile> manifest_file_;
    std::shared_ptr<CommitScanner> commit_scanner_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::string commit_user_;
    std::string table_name_;
    mutable LinkedHashMap<BinaryRow, bool> same_bucket_checked_partitions_;
};

}  // namespace paimon
