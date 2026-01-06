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

#include "paimon/core/operation/file_store_commit_impl.h"

#include <algorithm>
#include <cstddef>
#include <future>
#include <list>
#include <set>
#include <unordered_map>
#include <utility>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/commit_message.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/catalog/catalog_snapshot_commit.h"
#include "paimon/core/catalog/renaming_snapshot_commit.h"
#include "paimon/core/catalog/snapshot_commit.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/manifest/file_entry.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_committable.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/manifest/partition_entry.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/operation/expire_snapshots.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/manifest_file_merger.h"
#include "paimon/core/operation/metrics/commit_metrics.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"
#include "paimon/metrics.h"
#include "paimon/scan_context.h"

namespace paimon {
class Executor;
class MemoryPool;

FileStoreCommitImpl::FileStoreCommitImpl(
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<arrow::Schema>& schema, const std::string& root_path,
    const std::string& commit_user, const CoreOptions& options,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
    const std::shared_ptr<SnapshotManager>& snapshot_manager, bool ignore_empty_commit,
    bool use_rest_catalog_commit, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<ManifestFile>& manifest_file,
    const std::shared_ptr<ManifestList>& manifest_list,
    const std::shared_ptr<IndexManifestFile>& index_manifest_file,
    const std::shared_ptr<ExpireSnapshots>& expire_snapshots,
    const std::shared_ptr<SchemaManager>& schema_manager)
    : memory_pool_(pool),
      executor_(executor),
      schema_(schema),
      root_path_(root_path),
      commit_user_(commit_user),
      options_(options),
      path_factory_(path_factory),
      fs_(options.GetFileSystem()),
      partition_computer_(std::move(partition_computer)),
      snapshot_manager_(snapshot_manager),
      ignore_empty_commit_(ignore_empty_commit),
      num_bucket_(options.GetBucket()),
      table_schema_(table_schema),
      manifest_file_(manifest_file),
      manifest_list_(manifest_list),
      index_manifest_file_(index_manifest_file),
      expire_snapshots_(expire_snapshots),
      schema_manager_(schema_manager),
      metrics_(std::make_shared<MetricsImpl>()),
      logger_(Logger::GetLogger("FileStoreCommitImpl")) {
    if (use_rest_catalog_commit) {
        snapshot_commit_ = std::make_shared<CatalogSnapshotCommit>();
    } else {
        snapshot_commit_ = std::make_shared<RenamingSnapshotCommit>(fs_, snapshot_manager_);
    }
}

FileStoreCommitImpl::~FileStoreCommitImpl() = default;

Result<int32_t> FileStoreCommitImpl::Expire() {
    return expire_snapshots_->Expire();
}

Status FileStoreCommitImpl::DropPartition(
    const std::vector<std::map<std::string, std::string>>& partitions, int64_t commit_identifier) {
    if (partitions.empty()) {
        return Status::Invalid("Drop partition failed: partitions list cannot be empty.");
    }
    std::string log_msg = fmt::format("Ready to drop partitions {}", partitions);
    PAIMON_LOG_DEBUG(logger_, "%s", log_msg.c_str());
    return TryOverwrite(partitions, {}, commit_identifier, std::nullopt);
}

Result<int32_t> FileStoreCommitImpl::FilterAndCommit(
    const std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>>&
        commit_identifier_and_messages,
    std::optional<int64_t> watermark) {
    std::vector<std::shared_ptr<ManifestCommittable>> committables;
    for (const auto& [identifier, msgs] : commit_identifier_and_messages) {
        committables.push_back(CreateManifestCommittable(identifier, msgs, watermark));
    }

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<ManifestCommittable>> retry_committables,
                           FilterCommitted(committables));
    if (!retry_committables.empty()) {
        PAIMON_RETURN_NOT_OK(CheckFilesExistence(retry_committables));
        for (const auto& committable : retry_committables) {
            PAIMON_RETURN_NOT_OK(Commit(committable, /*check_append_files=*/true));
        }
    }
    return retry_committables.size();
}

Status FileStoreCommitImpl::CheckFilesExistence(
    const std::vector<std::shared_ptr<ManifestCommittable>>& committables) const {
    std::vector<std::string> all_paths;
    for (const auto& committable : committables) {
        for (const auto& message : committable->FileCommittables()) {
            auto msg = dynamic_cast<CommitMessageImpl*>(message.get());
            if (msg) {
                PAIMON_ASSIGN_OR_RAISE(
                    std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                    path_factory_->CreateDataFilePathFactory(msg->Partition(), msg->Bucket()));
                auto collect_files =
                    [&all_paths, data_file_path_factory](
                        const std::vector<std::shared_ptr<DataFileMeta>>& file_metas) {
                        for (const auto& file_meta : file_metas) {
                            auto paths = data_file_path_factory->CollectFiles(file_meta);
                            all_paths.insert(all_paths.end(), paths.begin(), paths.end());
                        }
                    };
                // skip compact before files, deleted index files
                DataIncrement new_files_increment = msg->GetNewFilesIncrement();
                collect_files(new_files_increment.NewFiles());
                collect_files(new_files_increment.ChangelogFiles());
                auto new_data_index_metas = new_files_increment.NewIndexFiles();
                for (const auto& data_index_meta : new_data_index_metas) {
                    all_paths.push_back(
                        path_factory_->ToIndexFilePath(data_index_meta->FileName()));
                }

                CompactIncrement compact_increment = msg->GetCompactIncrement();
                collect_files(compact_increment.CompactBefore());
                collect_files(compact_increment.CompactAfter());
                auto new_compact_index_metas = compact_increment.NewIndexFiles();
                for (const auto& compact_index_meta : new_compact_index_metas) {
                    all_paths.push_back(
                        path_factory_->ToIndexFilePath(compact_index_meta->FileName()));
                }
            } else {
                return Status::Invalid("fail to cast commit message to impl");
            }
        }
    }
    std::vector<std::future<Result<std::pair<bool, std::string>>>> file_exists_futures;
    for (const auto& path : all_paths) {
        file_exists_futures.push_back(
            Via(executor_.get(), [this, path]() -> Result<std::pair<bool, std::string>> {
                PAIMON_ASSIGN_OR_RAISE(bool exist, fs_->Exists(path));
                return std::pair(exist, path);
            }));
    }
    int32_t not_exist_files_count = 0;
    std::vector<Result<std::pair<bool, std::string>>> file_exists = CollectAll(file_exists_futures);
    std::vector<std::string> non_exist_files;
    for (auto file_exist : file_exists) {
        if (!file_exist.ok()) {
            return file_exist.status();
        }
        if (!file_exist.value().first) {
            not_exist_files_count++;
            non_exist_files.push_back(file_exist.value().second);
        }
    }

    if (not_exist_files_count > 0) {
        return Status::Invalid(fmt::format(
            "Cannot recover from this checkpoint because some files in the snapshot that need to "
            "be resubmitted have been deleted: {}. The most likely reason is because you are "
            "recovering from a very old savepoint that contains some uncommitted files that have "
            "already been deleted.",
            fmt::join(non_exist_files, ", ")));
    }
    return Status::OK();
}

Result<std::vector<std::shared_ptr<ManifestCommittable>>> FileStoreCommitImpl::FilterCommitted(
    const std::vector<std::shared_ptr<ManifestCommittable>>& committables) {
    // nothing to filter, fast exit
    if (committables.empty()) {
        return committables;
    }

    for (size_t i = 1; i < committables.size(); i++) {
        if (committables[i]->Identifier() < committables[i - 1]->Identifier()) {
            return Status::Invalid(
                "Committables must be sorted according to identifiers before filtering. This is "
                "unexpected.");
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                           snapshot_manager_->LatestSnapshotOfUser(commit_user_));
    if (latest_snapshot) {
        std::vector<std::shared_ptr<ManifestCommittable>> result;
        for (const auto& committable : committables) {
            // if committable is newer than latest snapshot, then it hasn't been committed
            if (committable->Identifier() > latest_snapshot.value().CommitIdentifier()) {
                result.push_back(committable);
            } else {
                // TODO(yonghao.fyh): callback
            }
        }
        return result;
    } else {
        // if there is no previous snapshots then nothing should be filtered
        return committables;
    }
}

Status FileStoreCommitImpl::Overwrite(
    const std::vector<std::map<std::string, std::string>>& partitions,
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages, int64_t identifier,
    std::optional<int64_t> watermark) {
    std::shared_ptr<ManifestCommittable> committable =
        CreateManifestCommittable(identifier, commit_messages, watermark);
    std::vector<ManifestEntry> append_table_files;
    std::vector<IndexManifestEntry> append_table_index_files;
    PAIMON_RETURN_NOT_OK(CollectChanges(committable->FileCommittables(), &append_table_files,
                                        &append_table_index_files));
    if (!append_table_index_files.empty()) {
        return Status::NotImplemented("Overwrite not support index for now");
    }
    return TryOverwrite(partitions, append_table_files, identifier, watermark);
}

Result<int32_t> FileStoreCommitImpl::FilterAndOverwrite(
    const std::vector<std::map<std::string, std::string>>& partitions,
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages, int64_t identifier,
    std::optional<int64_t> watermark) {
    std::shared_ptr<ManifestCommittable> committable =
        CreateManifestCommittable(identifier, commit_messages, watermark);
    std::vector<std::shared_ptr<ManifestCommittable>> committables;
    committables.push_back(committable);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<ManifestCommittable>> actual_committables,
                           FilterCommitted(committables));
    if (!actual_committables.empty()) {
        std::vector<ManifestEntry> append_table_files;
        std::vector<IndexManifestEntry> append_table_index_files;
        PAIMON_RETURN_NOT_OK(CollectChanges(actual_committables[0]->FileCommittables(),
                                            &append_table_files, &append_table_index_files));
        if (!append_table_index_files.empty()) {
            return Status::NotImplemented("FilterAndOverwrite not support index for now");
        }
        PAIMON_RETURN_NOT_OK(TryOverwrite(partitions, append_table_files, identifier, watermark));
    }
    return actual_committables.size();
}

Result<std::string> FileStoreCommitImpl::GetLastCommitTableRequest() {
    return snapshot_commit_->GetLastCommitTableRequest();
}

Result<std::vector<ManifestEntry>> FileStoreCommitImpl::GetAllFiles(
    const Snapshot& snapshot, const std::vector<std::map<std::string, std::string>>& partitions) {
    auto scan_filter =
        std::make_shared<ScanFilter>(/*predicate=*/nullptr, partitions,
                                     /*bucket_filter=*/std::nullopt, /*vector_search=*/nullptr);
    PAIMON_ASSIGN_OR_RAISE(
        auto scan, AppendOnlyFileStoreScan::Create(
                       snapshot_manager_, schema_manager_, manifest_list_, manifest_file_,
                       table_schema_, schema_, scan_filter, options_, executor_, memory_pool_));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> plan,
                           scan->WithSnapshot(snapshot)->CreatePlan());
    // scan existing file metas
    return plan->Files();
}

Status FileStoreCommitImpl::TryOverwrite(
    const std::vector<std::map<std::string, std::string>>& partitions,
    const std::vector<ManifestEntry>& changes, int64_t commit_identifier,
    std::optional<int64_t> watermark) {
    int32_t retry_count = 0;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                               snapshot_manager_->LatestSnapshot());
        std::vector<ManifestEntry> changes_with_overwrite;
        if (latest_snapshot) {
            PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> entries,
                                   GetAllFiles(latest_snapshot.value(), partitions));
            for (const auto& entry : entries) {
                changes_with_overwrite.emplace_back(FileKind::Delete(), entry.Partition(),
                                                    entry.Bucket(), entry.TotalBuckets(),
                                                    entry.File());
            }
        }
        changes_with_overwrite.insert(changes_with_overwrite.end(), changes.begin(), changes.end());
        PAIMON_ASSIGN_OR_RAISE(bool commit_success,
                               TryCommitOnce(changes_with_overwrite, /*index_entries=*/{},
                                             commit_identifier, watermark,
                                             /*log_offsets=*/{}, /*properties=*/{},
                                             Snapshot::CommitKind::Overwrite(), latest_snapshot,
                                             /*need_conflict_check=*/true));
        if (commit_success) {
            break;
        }
        if (retry_count >= options_.GetCommitMaxRetries()) {
            return Status::Invalid(
                fmt::format("Commit failed after {} attempts, there maybe exist commit conflicts "
                            "between multiple jobs.",
                            options_.GetCommitMaxRetries()));
        }
        retry_count++;
    }
    return Status::OK();
}

Status FileStoreCommitImpl::Commit(const std::shared_ptr<ManifestCommittable>& committable,
                                   bool check_append_files) {
    std::vector<ManifestEntry> append_table_files;
    std::vector<IndexManifestEntry> append_table_index_files;
    PAIMON_RETURN_NOT_OK(CollectChanges(committable->FileCommittables(), &append_table_files,
                                        &append_table_index_files));

    int32_t attempt = 0;
    if (!ignore_empty_commit_ || !append_table_files.empty() || !append_table_index_files.empty()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t cnt,
                               TryCommit(append_table_files, append_table_index_files,
                                         committable->Identifier(), committable->Watermark(),
                                         committable->LogOffsets(), committable->Properties(),
                                         Snapshot::CommitKind::Append(), check_append_files));
        attempt += cnt;
    }
    metrics_->SetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS, attempt);
    return Status::OK();
}

Status FileStoreCommitImpl::Commit(
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages, int64_t identifier,
    std::optional<int64_t> watermark) {
    std::shared_ptr<ManifestCommittable> committable =
        CreateManifestCommittable(identifier, commit_messages, watermark);
    return Commit(committable, /*check_append_files=*/false);
}

Result<int32_t> FileStoreCommitImpl::TryCommit(const std::vector<ManifestEntry>& delta_files,
                                               const std::vector<IndexManifestEntry>& index_entries,
                                               int64_t identifier, std::optional<int64_t> watermark,
                                               std::map<int32_t, int64_t> log_offsets,
                                               const std::map<std::string, std::string>& properties,
                                               Snapshot::CommitKind commit_kind,
                                               bool check_append_files) {
    int32_t retry_count = 0;
    int64_t start_millis = DateTimeUtils::GetCurrentUTCTimeUs() / 1000;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                               snapshot_manager_->LatestSnapshot());
        PAIMON_ASSIGN_OR_RAISE(
            bool commit_success,
            TryCommitOnce(delta_files, index_entries, identifier, watermark, log_offsets,
                          properties, commit_kind, latest_snapshot, check_append_files));
        if (commit_success) {
            break;
        }
        int64_t current_millis = DateTimeUtils::GetCurrentUTCTimeUs() / 1000;
        if (current_millis - start_millis > options_.GetCommitTimeout() ||
            retry_count >= options_.GetCommitMaxRetries()) {
            return Status::Invalid(
                fmt::format("Commit failed after {} millis with {} retries, there maybe exist "
                            "commit conflicts between multiple jobs.",
                            options_.GetCommitTimeout(), options_.GetCommitMaxRetries()));
        }
        retry_count++;
    }
    return retry_count + 1;
}

Result<std::set<std::map<std::string, std::string>>> FileStoreCommitImpl::ChangedPartitions(
    const std::vector<ManifestEntry>& data_files,
    const std::vector<IndexManifestEntry>& index_files) const {
    std::set<std::map<std::string, std::string>> partitions;
    auto add_partition = [&, this](const BinaryRow& partition_row) -> Status {
        std::vector<std::pair<std::string, std::string>> part_values;
        PAIMON_ASSIGN_OR_RAISE(part_values,
                               partition_computer_->GeneratePartitionVector(partition_row));
        if (part_values.empty()) {
            return Status::OK();
        }
        std::map<std::string, std::string> part_values_map;
        for (const auto& [key, value] : part_values) {
            part_values_map[key] = value;
        }
        partitions.insert(part_values_map);
        return Status::OK();
    };

    for (const ManifestEntry& entry : data_files) {
        PAIMON_RETURN_NOT_OK(add_partition(entry.Partition()));
    }
    for (const IndexManifestEntry& entry : index_files) {
        if (entry.index_file->IndexType() == DeletionVectorsIndexFile::DELETION_VECTORS_INDEX) {
            PAIMON_RETURN_NOT_OK(add_partition(entry.partition));
        }
    }
    return partitions;
}

Result<std::vector<ManifestEntry>> FileStoreCommitImpl::ReadAllEntriesFromChangedPartitions(
    const Snapshot& latest_snapshot,
    const std::set<std::map<std::string, std::string>>& partitions) const {
    std::vector<std::map<std::string, std::string>> partition_filters(partitions.begin(),
                                                                      partitions.end());
    auto scan_filter =
        std::make_shared<ScanFilter>(/*predicate=*/nullptr, partition_filters,
                                     /*bucket_filter=*/std::nullopt, /*vector_search=*/nullptr);
    PAIMON_ASSIGN_OR_RAISE(
        auto scan, AppendOnlyFileStoreScan::Create(
                       snapshot_manager_, schema_manager_, manifest_list_, manifest_file_,
                       table_schema_, schema_, scan_filter, options_, executor_, memory_pool_));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> plan,
                           scan->WithSnapshot(latest_snapshot)->CreatePlan());
    // scan existing file metas
    return plan->Files();
}

Status FileStoreCommitImpl::NoConflictsOrFail(const std::string& base_commit_user,
                                              const std::vector<ManifestEntry>& base_entries,
                                              const std::vector<ManifestEntry>& changes) const {
    ScopeGuard guard([&]() {
        PAIMON_LOG_WARN(logger_, "File deletion conflicts detected! Give up committing. %s",
                        base_commit_user.c_str());
    });
    std::vector<ManifestEntry> all_entries = base_entries;
    all_entries.insert(all_entries.end(), changes.begin(), changes.end());
    std::vector<ManifestEntry> merged_entries;
    PAIMON_RETURN_NOT_OK(FileEntry::MergeEntries(all_entries, &merged_entries));
    for (const auto& entry : merged_entries) {
        if (entry.Kind() == FileKind::Delete()) {
            return Status::Invalid(fmt::format(
                "Trying to delete file {} which is not previously added.", entry.FileName()));
        }
    }
    // TODO(yonghao.fyh): check for all LSM level >= 1, key ranges of files do not intersect
    guard.Release();
    return Status::OK();
}

Result<bool> FileStoreCommitImpl::TryCommitOnce(
    const std::vector<ManifestEntry>& delta_entries,
    const std::vector<IndexManifestEntry>& index_entries, int64_t identifier,
    std::optional<int64_t> watermark, std::map<int32_t, int64_t> log_offsets,
    const std::map<std::string, std::string>& properties, Snapshot::CommitKind commit_kind,
    const std::optional<Snapshot>& latest_snapshot, bool need_conflict_check) {
    std::vector<ManifestEntry> delta_files = delta_entries;
    int64_t start_millis = DateTimeUtils::GetCurrentUTCTimeUs() / 1000;
    int64_t new_snapshot_id = Snapshot::FIRST_SNAPSHOT_ID;
    int64_t first_row_id_start = 0;
    if (latest_snapshot) {
        new_snapshot_id = latest_snapshot.value().Id() + 1;
        std::optional<int64_t> next_row_id = latest_snapshot.value().NextRowId();
        if (next_row_id) {
            first_row_id_start = next_row_id.value();
        }
    }

    PAIMON_LOG_DEBUG(logger_, "Ready to commit table files to snapshot #%ld", new_snapshot_id);
    for (const ManifestEntry& entry : delta_files) {
        PAIMON_LOG_DEBUG(logger_, "  * %s", entry.ToString().c_str());
    }

    if (need_conflict_check && latest_snapshot) {
        std::set<std::map<std::string, std::string>> changed_partitions;
        PAIMON_ASSIGN_OR_RAISE(changed_partitions, ChangedPartitions(delta_files, index_entries));
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<ManifestEntry> base_data_files,
            ReadAllEntriesFromChangedPartitions(latest_snapshot.value(), changed_partitions));
        PAIMON_RETURN_NOT_OK(
            NoConflictsOrFail(latest_snapshot.value().CommitUser(), base_data_files, delta_files));
    }

    std::vector<ManifestFileMeta> merge_before_manifests;
    std::vector<ManifestFileMeta> merge_after_manifests;
    std::pair<std::string, int64_t> base_manifest_list;
    std::pair<std::string, int64_t> delta_manifest_list;
    std::vector<PartitionEntry> delta_statistics;
    std::string new_snapshot_path;

    std::optional<std::string> old_index_manifest;
    std::optional<std::string> index_manifest_name;
    ScopeGuard guard([&]() {
        int64_t commit_time = ((DateTimeUtils::GetCurrentUTCTimeUs() / 1000) - start_millis) / 1000;
        PAIMON_LOG_WARN(logger_,
                        "Atomic commit failed for snapshot #%ld (path %s) by user %s with "
                        "identifier %ld and kind %s after %ld seconds. Clean up and try again.",
                        new_snapshot_id, new_snapshot_path.c_str(), commit_user_.c_str(),
                        identifier, Snapshot::CommitKind::ToString(commit_kind).c_str(),
                        commit_time);

        CleanUpTmpManifests(base_manifest_list.first, delta_manifest_list.first,
                            merge_before_manifests, merge_after_manifests, old_index_manifest,
                            index_manifest_name);
    });
    int64_t next_row_id_start = first_row_id_start;
    int64_t previous_total_record_count = 0;

    if (latest_snapshot) {
        old_index_manifest = latest_snapshot.value().IndexManifest();
        // TODO(yonghao.fyh): total record count should call scan when its std::nullopt
        previous_total_record_count = latest_snapshot.value().TotalRecordCount() != std::nullopt
                                          ? latest_snapshot.value().TotalRecordCount().value()
                                          : 0;
        std::vector<ManifestFileMeta> previous_manifests;
        // read all previous manifest files
        PAIMON_RETURN_NOT_OK(
            manifest_list_->ReadDataManifests(latest_snapshot.value(), &previous_manifests));
        merge_before_manifests.insert(merge_before_manifests.end(), previous_manifests.begin(),
                                      previous_manifests.end());
        // read the last snapshot to complete the bucket's offsets when logOffsets does not
        // contain all buckets
        std::optional<std::map<int32_t, int64_t>> latest_log_offsets =
            latest_snapshot.value().LogOffsets();
        if (latest_log_offsets) {
            for (const auto& [key, value] : latest_log_offsets.value()) {
                log_offsets.emplace(key, value);
            }
        }
        std::optional<int64_t> latest_watermark = latest_snapshot.value().Watermark();
        if (latest_watermark) {
            if (watermark == std::nullopt) {
                watermark = latest_watermark;
            } else {
                watermark = std::max(watermark.value(), latest_watermark.value());
            }
        }
    }

    // try to merge old manifest files to create base manifest list
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<ManifestFileMeta> merged_metas,
        ManifestFileMerger::Merge(merge_before_manifests, options_.GetManifestTargetFileSize(),
                                  options_.GetManifestMergeMinCount(),
                                  options_.GetManifestFullCompactionThresholdSize(),
                                  manifest_file_.get()));
    merge_after_manifests.insert(merge_after_manifests.end(), merged_metas.begin(),
                                 merged_metas.end());
    PAIMON_ASSIGN_OR_RAISE(base_manifest_list, manifest_list_->Write(merge_after_manifests));

    if (options_.RowTrackingEnabled()) {
        // assigned snapshot id to delta files
        AssignSnapshotId(new_snapshot_id, &delta_files);
        // assign row id for new files
        PAIMON_ASSIGN_OR_RAISE(next_row_id_start,
                               AssignRowTrackingMeta(first_row_id_start, &delta_files));
    }

    // the added records subtract the deleted records from
    int64_t delta_record_count =
        ManifestEntry::RecordCountAdd(delta_files) - ManifestEntry::RecordCountDelete(delta_files);
    int64_t total_record_count = previous_total_record_count + delta_record_count;

    // write new delta files into manifest files
    std::unordered_map<BinaryRow, PartitionEntry> partition_entry_map;
    PAIMON_RETURN_NOT_OK(PartitionEntry::Merge(delta_files, &partition_entry_map));
    delta_statistics.reserve(partition_entry_map.size());
    for (const auto& [_, partition_entry] : partition_entry_map) {
        delta_statistics.push_back(partition_entry);
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestFileMeta> new_changes_manifests,
                           manifest_file_->Write(delta_files));
    merge_after_manifests.insert(merge_after_manifests.end(), new_changes_manifests.begin(),
                                 new_changes_manifests.end());
    PAIMON_ASSIGN_OR_RAISE(delta_manifest_list, manifest_list_->Write(new_changes_manifests));

    PAIMON_ASSIGN_OR_RAISE(index_manifest_name, index_manifest_file_->WriteIndexFiles(
                                                    old_index_manifest, index_entries));

    std::optional<std::pair<std::string, int64_t>> changelog_manifest_list;
    std::optional<std::string> statistics;
    int64_t changelog_record_count = 0;
    int64_t schema_id = 0;
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> table_schema,
                           schema_manager_->Latest());
    if (table_schema) {
        schema_id = table_schema.value()->Id();
    }

    Snapshot new_snapshot(
        new_snapshot_id, schema_id, base_manifest_list.first, base_manifest_list.second,
        delta_manifest_list.first, delta_manifest_list.second,
        changelog_manifest_list ? std::optional<std::string>(changelog_manifest_list.value().first)
                                : std::nullopt,
        changelog_manifest_list ? std::optional<int64_t>(changelog_manifest_list.value().second)
                                : std::nullopt,
        index_manifest_name, commit_user_, identifier, commit_kind,
        DateTimeUtils::GetCurrentUTCTimeUs() / 1000, log_offsets, total_record_count,
        delta_record_count, changelog_record_count, watermark, statistics,
        properties.empty() ? std::nullopt
                           : std::optional<std::map<std::string, std::string>>(properties),
        next_row_id_start);

    Result<bool> commit_result = CommitSnapshotImpl(new_snapshot, delta_statistics);
    if (!commit_result.ok()) {
        // commit exception, not sure about the situation and should not clean up the files.
        PAIMON_LOG_WARN(logger_, "You need call FilterAndCommit to retry commit for exception. %s",
                        commit_result.status().ToString().c_str());

        // To prevent the case where an atomic write times out but actually succeeds,
        // retrying the commit could lead to the snapshot file being committed multiple times.
        // Therefore, retries should be handled by the upper layer,
        // which should call FilterAndCommit to avoid duplicate commits.
        // Therefore, we should not trigger cleanup here,
        // as it may delete meta files from a snapshot that was just written by ourselves,
        // leading to an incomplete or corrupted snapshot.
        guard.Release();
        return Status::Invalid("You need call FilterAndCommit to retry commit for exception. ",
                               commit_result.status().ToString());
    }
    bool commit_success = commit_result.value();
    if (commit_success) {
        PAIMON_LOG_INFO(logger_,
                        "Successfully commit snapshot %ld to table %s by user %s with identifier "
                        "%ld and kind %s.",
                        new_snapshot.Id(), root_path_.c_str(), commit_user_.c_str(),
                        new_snapshot.CommitIdentifier(),
                        Snapshot::CommitKind::ToString(new_snapshot.GetCommitKind()).c_str());
        guard.Release();
        return true;
    } else {
        // commit fails, should clean up the files
        return false;
    }
}

void FileStoreCommitImpl::AssignSnapshotId(int64_t snapshot_id,
                                           std::vector<ManifestEntry>* delta_files) const {
    for (auto& entry : *delta_files) {
        entry.AssignSequenceNumber(/*min_sequence_number=*/snapshot_id,
                                   /*max_sequence_number=*/snapshot_id);
    }
}

Result<int64_t> FileStoreCommitImpl::AssignRowTrackingMeta(
    int64_t first_row_id_start, std::vector<ManifestEntry>* delta_files) const {
    if (delta_files->empty()) {
        return first_row_id_start;
    }
    // assign row id for new files
    int64_t start = first_row_id_start;
    int64_t blob_start = first_row_id_start;
    for (auto& entry : *delta_files) {
        if (entry.File()->file_source == std::nullopt) {
            return Status::Invalid(
                "This is a bug, file source field for row-tracking table must present.");
        }
        if (entry.File()->file_source.value() == FileSource::Append() &&
            entry.File()->first_row_id == std::nullopt) {
            if (BlobUtils::IsBlobFile(entry.File()->file_name)) {
                if (blob_start >= start) {
                    return Status::Invalid(fmt::format(
                        "This is a bug, blob start {} should be less than start {} when "
                        "assigning a blob entry file.",
                        blob_start, start));
                }
                int64_t row_count = entry.File()->row_count;
                entry.AssignFirstRowId(blob_start);
                blob_start += row_count;
            } else {
                int64_t row_count = entry.File()->row_count;
                entry.AssignFirstRowId(start);
                blob_start = start;
                start += row_count;
            }
        }
        // for compact file, do not assign first row id.
    }
    return start;
}

Result<bool> FileStoreCommitImpl::CommitSnapshotImpl(
    const Snapshot& new_snapshot, const std::vector<PartitionEntry>& delta_statistics) {
    std::vector<PartitionStatistics> statistics;
    statistics.reserve(delta_statistics.size());
    for (const auto& entry : delta_statistics) {
        PAIMON_ASSIGN_OR_RAISE(PartitionStatistics partition_statistics,
                               entry.ToPartitionStatistics(partition_computer_.get()));
        statistics.emplace_back(std::move(partition_statistics));
    }
    Result<bool> commit_result = snapshot_commit_->Commit(new_snapshot, statistics);
    if (!commit_result.ok()) {
        // exception when performing the atomic rename,
        // we cannot clean up because we can't determine the success
        return Status::Invalid(fmt::format(
            "Exception occurs when committing snapshot #{} by user {} with identifier {} and kind "
            "{}. Cannot clean up because we can't determine the success. {}",
            new_snapshot.Id(), commit_user_, new_snapshot.CommitIdentifier(),
            Snapshot::CommitKind::ToString(new_snapshot.GetCommitKind()),
            commit_result.status().ToString()));
    }
    return commit_result;
}

void FileStoreCommitImpl::CleanUpTmpManifests(
    const std::string& base_manifest_list_name, const std::string& delta_manifest_list_name,
    const std::vector<ManifestFileMeta>& merge_before_manifests,
    const std::vector<ManifestFileMeta>& merge_after_manifests,
    const std::optional<std::string>& old_index_manifest,
    const std::optional<std::string>& new_index_manifest) {
    if (!base_manifest_list_name.empty()) {
        manifest_list_->DeleteQuietly(base_manifest_list_name);
        PAIMON_LOG_DEBUG(logger_, "base manifest list %s", base_manifest_list_name.c_str());
    }
    if (!delta_manifest_list_name.empty()) {
        manifest_list_->DeleteQuietly(delta_manifest_list_name);
        PAIMON_LOG_DEBUG(logger_, "delta manifest list %s", delta_manifest_list_name.c_str());
    }
    // for faster searching
    std::set<std::string> merge_before_manifest_set;
    for (const auto& merge_before_manifest : merge_before_manifests) {
        merge_before_manifest_set.emplace(merge_before_manifest.FileName());
    }
    // clean up newly merged manifest files
    for (const auto& merge_after_manifest : merge_after_manifests) {
        if (merge_before_manifest_set.find(merge_after_manifest.FileName()) ==
            merge_before_manifest_set.end()) {
            manifest_list_->DeleteQuietly(merge_after_manifest.FileName());
            PAIMON_LOG_DEBUG(logger_, "delete new file %s",
                             merge_after_manifest.FileName().c_str());
        }
    }
    // clean up index manifest
    if (new_index_manifest && old_index_manifest != new_index_manifest) {
        index_manifest_file_->DeleteQuietly(new_index_manifest.value());
        PAIMON_LOG_DEBUG(logger_, "delete new index file %s", new_index_manifest.value().c_str());
    }
}

std::shared_ptr<ManifestCommittable> FileStoreCommitImpl::CreateManifestCommittable(
    int64_t identifier, const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
    std::optional<int64_t> watermark) {
    auto committable = std::make_shared<ManifestCommittable>(identifier, watermark);
    for (const auto& commit_message : commit_messages) {
        committable->AddFileCommittable(commit_message);
    }
    return committable;
}

Status FileStoreCommitImpl::CollectChanges(
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
    std::vector<ManifestEntry>* append_table_files,
    std::vector<IndexManifestEntry>* append_table_index_files) {
    for (const auto& message : commit_messages) {
        auto commit_message = std::dynamic_pointer_cast<CommitMessageImpl>(message);
        if (commit_message) {
            DataIncrement new_files_increment = commit_message->GetNewFilesIncrement();
            for (const std::shared_ptr<DataFileMeta>& new_file : new_files_increment.NewFiles()) {
                append_table_files->push_back(MakeEntry(FileKind::Add(), commit_message, new_file));
            }
            for (const std::shared_ptr<DataFileMeta>& deleted_file :
                 new_files_increment.DeletedFiles()) {
                append_table_files->push_back(
                    MakeEntry(FileKind::Delete(), commit_message, deleted_file));
            }
            for (const std::shared_ptr<IndexFileMeta>& deleted_index_file :
                 new_files_increment.DeletedIndexFiles()) {
                append_table_index_files->emplace_back(
                    FileKind::Delete(), commit_message->Partition(), commit_message->Bucket(),
                    deleted_index_file);
            }
            for (const std::shared_ptr<IndexFileMeta>& new_index_file :
                 new_files_increment.NewIndexFiles()) {
                append_table_index_files->emplace_back(FileKind::Add(), commit_message->Partition(),
                                                       commit_message->Bucket(), new_index_file);
            }
        } else {
            return Status::Invalid("fail to cast commit message to commit message impl");
        }
    }
    return Status::OK();
}

ManifestEntry FileStoreCommitImpl::MakeEntry(
    const FileKind& kind, const std::shared_ptr<CommitMessageImpl>& commit_message,
    const std::shared_ptr<DataFileMeta>& file) const {
    int32_t total_buckets = commit_message->TotalBuckets() == std::nullopt
                                ? num_bucket_
                                : commit_message->TotalBuckets().value();
    return ManifestEntry(kind, commit_message->Partition(), commit_message->Bucket(), total_buckets,
                         file);
}

}  // namespace paimon
