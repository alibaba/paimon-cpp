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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "paimon/common/options/memory_size.h"
#include "paimon/core/catalog/snapshot_commit.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/partition_entry.h"
#include "paimon/core/snapshot.h"
#include "paimon/file_store_commit.h"
#include "paimon/logging.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
class Schema;
}  // namespace arrow

namespace paimon {

class CommitContext;
class CommitMessageImpl;
struct DataFileMeta;
class ExpireSnapshots;
class FileKind;
class FileStorePathFactory;
class ManifestEntry;
class ManifestCommittable;
class ManifestFile;
class IndexManifestFile;
struct IndexManifestEntry;
class ManifestList;
class ManifestFileMeta;
class SnapshotManager;
class SchemaManager;
class TableSchema;
class BinaryRowPartitionComputer;
class CommitMessage;
class Executor;
class FileSystem;
class Logger;
class MemoryPool;
class Metrics;
class PartitionEntry;
class SnapshotCommit;

/// Commit operation which provides commit and overwrite.
class FileStoreCommitImpl : public FileStoreCommit {
 public:
    FileStoreCommitImpl(const std::shared_ptr<MemoryPool>& pool,
                        const std::shared_ptr<Executor>& executor,
                        const std::shared_ptr<arrow::Schema>& schema, const std::string& root_path,
                        const std::string& commit_user, const CoreOptions& options,
                        const std::shared_ptr<FileStorePathFactory>& path_factory,
                        std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
                        const std::shared_ptr<SnapshotManager>& snapshot_manager,
                        bool ignore_empty_commit, bool use_rest_catalog_commit,
                        const std::shared_ptr<TableSchema>& table_schema,
                        const std::shared_ptr<ManifestFile>& manifest_file,
                        const std::shared_ptr<ManifestList>& manifest_list,
                        const std::shared_ptr<IndexManifestFile>& index_manifest_file,
                        const std::shared_ptr<ExpireSnapshots>& expire_snapshots,
                        const std::shared_ptr<SchemaManager>& schema_manager);
    ~FileStoreCommitImpl() override;

    Status Commit(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                  int64_t commit_identifier,
                  std::optional<int64_t> watermark = std::nullopt) override;

    Result<int32_t> FilterAndCommit(
        const std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>>&
            commit_identifier_and_messages,
        std::optional<int64_t> watermark = std::nullopt) override;

    Status Overwrite(const std::vector<std::map<std::string, std::string>>& partitions,
                     const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                     int64_t commit_identifier,
                     std::optional<int64_t> watermark = std::nullopt) override;

    Result<int32_t> FilterAndOverwrite(
        const std::vector<std::map<std::string, std::string>>& partitions,
        const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
        int64_t commit_identifier, std::optional<int64_t> watermark = std::nullopt) override;

    Result<std::string> GetLastCommitTableRequest() override;

    Result<int32_t> Expire() override;

    Status DropPartition(const std::vector<std::map<std::string, std::string>>& partitions,
                         int64_t commit_identifier) override;

    std::shared_ptr<Metrics> GetCommitMetrics() const override {
        return metrics_;
    }

    Status Init(std::unique_ptr<CommitContext> ctx);

 private:
    Status Commit(const std::shared_ptr<ManifestCommittable>& manifest_committable,
                  bool check_append_files);

    Status TryOverwrite(const std::vector<std::map<std::string, std::string>>& partition,
                        const std::vector<ManifestEntry>& changes, int64_t commit_identifier,
                        std::optional<int64_t> watermark);

    Result<std::vector<ManifestEntry>> GetAllFiles(
        const Snapshot& snapshot,
        const std::vector<std::map<std::string, std::string>>& partitions);

    Result<std::vector<std::shared_ptr<ManifestCommittable>>> FilterCommitted(
        const std::vector<std::shared_ptr<ManifestCommittable>>& committables);

    std::shared_ptr<ManifestCommittable> CreateManifestCommittable(
        int64_t identifier, const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
        std::optional<int64_t> watermark);

    ManifestEntry MakeEntry(const FileKind& kind,
                            const std::shared_ptr<CommitMessageImpl>& commit_message,
                            const std::shared_ptr<DataFileMeta>& file) const;

    Status CollectChanges(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                          std::vector<ManifestEntry>* append_table_files,
                          std::vector<ManifestEntry>* append_changelog_files,
                          std::vector<ManifestEntry>* compact_table_files,
                          std::vector<ManifestEntry>* compact_changelog_files,
                          std::vector<IndexManifestEntry>* append_table_index_files,
                          std::vector<IndexManifestEntry>* compact_table_index_files);

    Result<int32_t> TryCommit(const std::vector<ManifestEntry>& delta_files,
                              const std::vector<IndexManifestEntry>& index_entries,
                              int64_t identifier, std::optional<int64_t> watermark,
                              std::map<int32_t, int64_t> log_offsets,
                              const std::map<std::string, std::string>& properties,
                              Snapshot::CommitKind commit_kind, bool check_append_files);
    Result<bool> TryCommitOnce(const std::vector<ManifestEntry>& delta_files,
                               const std::vector<IndexManifestEntry>& index_entries,
                               int64_t commit_identifier, std::optional<int64_t> watermark,
                               std::map<int32_t, int64_t> log_offsets,
                               const std::map<std::string, std::string>& properties,
                               Snapshot::CommitKind commit_kind,
                               const std::optional<Snapshot>& latest_snapshot,
                               bool need_conflict_check);

    Result<bool> CommitSnapshotImpl(const Snapshot& new_snapshot,
                                    const std::vector<PartitionEntry>& delta_statistics);

    void CleanUpTmpManifests(const std::string& previous_changes_list_name,
                             const std::string& new_changes_list_name,
                             const std::vector<ManifestFileMeta>& old_metas,
                             const std::vector<ManifestFileMeta>& new_metas,
                             const std::optional<std::string>& old_index_manifest,
                             const std::optional<std::string>& new_index_manifest);

    Result<std::vector<ManifestEntry>> ReadAllEntriesFromChangedPartitions(
        const Snapshot& latest_snapshot,
        const std::set<std::map<std::string, std::string>>& partitions) const;

    Status NoConflictsOrFail(const std::string& base_commit_user,
                             const std::vector<ManifestEntry>& base_entries,
                             const std::vector<ManifestEntry>& changes) const;

    Status CheckFilesExistence(
        const std::vector<std::shared_ptr<ManifestCommittable>>& committables) const;

    void AssignSnapshotId(int64_t snapshot_id, std::vector<ManifestEntry>* delta_files) const;

    Result<int64_t> AssignRowTrackingMeta(int64_t first_row_id_start,
                                          std::vector<ManifestEntry>* delta_files) const;

    Result<std::set<std::map<std::string, std::string>>> ChangedPartitions(
        const std::vector<ManifestEntry>& data_files,
        const std::vector<IndexManifestEntry>& index_files) const;

    static int64_t RowCounts(const std::vector<ManifestEntry>& files);

    static int64_t NumChangedPartitions(const std::vector<std::vector<ManifestEntry>>& changes);

    static int64_t NumChangedBuckets(const std::vector<std::vector<ManifestEntry>>& changes);

 private:
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<arrow::Schema> schema_;
    std::string root_path_;
    std::string commit_user_;
    CoreOptions options_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::shared_ptr<FileSystem> fs_;

    std::unique_ptr<BinaryRowPartitionComputer> partition_computer_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<SnapshotCommit> snapshot_commit_;
    bool ignore_empty_commit_ = true;
    int32_t num_bucket_ = 0;
    std::shared_ptr<TableSchema> table_schema_;

    std::shared_ptr<ManifestFile> manifest_file_;
    std::shared_ptr<ManifestList> manifest_list_;
    std::shared_ptr<IndexManifestFile> index_manifest_file_;

    std::shared_ptr<ExpireSnapshots> expire_snapshots_;
    std::shared_ptr<SchemaManager> schema_manager_;

    std::shared_ptr<Metrics> metrics_;
    std::shared_ptr<Logger> logger_;
};

}  // namespace paimon
