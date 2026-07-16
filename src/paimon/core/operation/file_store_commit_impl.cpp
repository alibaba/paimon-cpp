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
#include <functional>
#include <future>
#include <limits>
#include <list>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/commit_message.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/path_util.h"
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
#include "paimon/core/operation/commit/commit_changes_provider.h"
#include "paimon/core/operation/commit/compacted_changelog_path_resolver.h"
#include "paimon/core/operation/commit/conflict_detection.h"
#include "paimon/core/operation/commit/row_tracking_commit_utils.h"
#include "paimon/core/operation/commit/sequence_snapshot_properties.h"
#include "paimon/core/operation/expire_snapshots.h"
#include "paimon/core/operation/manifest_file_merger.h"
#include "paimon/core/operation/metrics/commit_metrics.h"
#include "paimon/core/operation/metrics/commit_stats.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/duration.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"
#include "paimon/metrics.h"

namespace paimon {
class Executor;
class MemoryPool;

namespace {

constexpr const char* kCommitStrictModeLastSafeSnapshot = "commit.strict-mode.last-safe-snapshot";
constexpr const char* kManifestDeleteFileDropStats = "manifest.delete-file-drop-stats";
constexpr const char* kSequenceSnapshotOrdering = "sequence.snapshot-ordering";
constexpr const char* kPkClusteringOverride = "pk-clustering-override";

bool MatchPartitionSpec(const std::map<std::string, std::string>& partition,
                        const std::map<std::string, std::string>& partition_spec) {
    for (const auto& [key, value] : partition_spec) {
        auto iter = partition.find(key);
        if (iter == partition.end() || iter->second != value) {
            return false;
        }
    }
    return true;
}

}  // namespace

Status FileStoreCommitImpl::ValidateCommitOptions(const CoreOptions& options) {
    const auto& raw_options = options.ToMap();
    std::vector<std::string> unsupported_options;

    if (raw_options.find(kCommitStrictModeLastSafeSnapshot) != raw_options.end()) {
        unsupported_options.emplace_back(kCommitStrictModeLastSafeSnapshot);
    }
    if (raw_options.find(kManifestDeleteFileDropStats) != raw_options.end()) {
        unsupported_options.emplace_back(kManifestDeleteFileDropStats);
    }
    if (raw_options.find(kSequenceSnapshotOrdering) != raw_options.end()) {
        unsupported_options.emplace_back(kSequenceSnapshotOrdering);
    }
    if (raw_options.find(kPkClusteringOverride) != raw_options.end()) {
        unsupported_options.emplace_back(kPkClusteringOverride);
    }

    if (!unsupported_options.empty()) {
        return Status::Invalid(fmt::format(
            "These options are not supported by C++ commit path: {}. "
            "Please use Java commit, or remove these options before creating FileStoreCommit.",
            fmt::join(unsupported_options, ", ")));
    }

    return Status::OK();
}

FileStoreCommitImpl::FileStoreCommitImpl(
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<arrow::Schema>& schema, const std::string& root_path,
    const std::string& commit_user, const CoreOptions& options,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
    const std::shared_ptr<SnapshotManager>& snapshot_manager, bool ignore_empty_commit,
    bool use_rest_catalog_commit, bool append_commit_check_conflict,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<ManifestFile>& manifest_file,
    const std::shared_ptr<ManifestList>& manifest_list,
    const std::shared_ptr<IndexManifestFile>& index_manifest_file,
    const std::shared_ptr<ExpireSnapshots>& expire_snapshots,
    const std::shared_ptr<SchemaManager>& schema_manager, CommitScanner::ScanSupplier scan_supplier)
    : memory_pool_(pool),
      executor_(executor),
      schema_(schema),
      root_path_(root_path),
      table_name_(PathUtil::GetName(root_path)),
      commit_user_(commit_user),
      options_(options),
      path_factory_(path_factory),
      fs_(options.GetFileSystem()),
      partition_computer_(std::move(partition_computer)),
      snapshot_manager_(snapshot_manager),
      ignore_empty_commit_(ignore_empty_commit),
      append_commit_check_conflict_(append_commit_check_conflict),
      retry_waiter_(options.GetCommitMinRetryWait(), options.GetCommitMaxRetryWait()),
      num_bucket_(options.GetBucket()),
      bucket_mode_(ResolveBucketMode(options.GetBucket(), table_schema)),
      table_schema_(table_schema),
      commit_scanner_(std::make_shared<CommitScanner>(
          snapshot_manager, schema_manager, manifest_list, manifest_file, index_manifest_file,
          table_schema, schema, options, executor, pool, partition_computer_.get(),
          std::move(scan_supplier))),
      conflict_detection_(table_schema, options, snapshot_manager_, manifest_list, manifest_file,
                          commit_scanner_, commit_user, PathUtil::GetName(root_path)),
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
    PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] int32_t attempt,
                           TryOverwrite(partitions, /*changes=*/{}, /*index_entries=*/{},
                                        commit_identifier, std::nullopt, /*properties=*/{}));
    return Status::OK();
}

FileStoreCommit& FileStoreCommitImpl::RowIdCheckConflict(
    std::optional<int64_t> row_id_check_from_snapshot) {
    conflict_detection_.SetRowIdCheckFromSnapshot(row_id_check_from_snapshot);
    return *this;
}

Result<int32_t> FileStoreCommitImpl::FilterAndCommit(
    const std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>>&
        commit_identifier_and_messages,
    std::optional<int64_t> watermark) {
    std::vector<std::shared_ptr<ManifestCommittable>> committables;
    for (const auto& [identifier, msgs] : commit_identifier_and_messages) {
        committables.push_back(CreateManifestCommittable(identifier, msgs, watermark));
    }

    std::vector<std::shared_ptr<ManifestCommittable>> sorted_committables = committables;
    std::sort(sorted_committables.begin(), sorted_committables.end(),
              [](const std::shared_ptr<ManifestCommittable>& lhs,
                 const std::shared_ptr<ManifestCommittable>& rhs) {
                  return lhs->Identifier() < rhs->Identifier();
              });

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<ManifestCommittable>> retry_committables,
                           FilterCommitted(sorted_committables));
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
            if (msg == nullptr) {
                return Status::Invalid("fail to cast commit message to impl");
            }
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                path_factory_->CreateDataFilePathFactory(msg->Partition(), msg->Bucket()));
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<IndexPathFactory> index_file_path_factory,
                path_factory_->CreateIndexFileFactory(msg->Partition(), msg->Bucket()));
            auto collect_files = [&all_paths, data_file_path_factory](
                                     const std::vector<std::shared_ptr<DataFileMeta>>& file_metas) {
                for (const auto& file_meta : file_metas) {
                    auto paths = data_file_path_factory->CollectFiles(file_meta);
                    all_paths.insert(all_paths.end(), paths.begin(), paths.end());
                }
            };
            DataIncrement new_files_increment = msg->GetNewFilesIncrement();
            collect_files(new_files_increment.NewFiles());
            collect_files(new_files_increment.ChangelogFiles());
            auto new_data_index_metas = new_files_increment.NewIndexFiles();
            for (const auto& data_index_meta : new_data_index_metas) {
                all_paths.push_back(index_file_path_factory->ToPath(data_index_meta));
            }

            CompactIncrement compact_increment = msg->GetCompactIncrement();
            collect_files(compact_increment.CompactAfter());
            auto new_compact_index_metas = compact_increment.NewIndexFiles();
            for (const auto& compact_index_meta : new_compact_index_metas) {
                all_paths.push_back(index_file_path_factory->ToPath(compact_index_meta));
            }

            // skip compact before files, deleted index files
        }
    }

    // Resolve compacted changelog files to their real file paths
    std::vector<std::string> resolved_paths;
    resolved_paths.reserve(all_paths.size());
    for (const auto& path : all_paths) {
        resolved_paths.push_back(CompactedChangelogPathResolver::Resolve(path));
    }

    // Deduplicate paths as multiple compacted changelog references may resolve to the same
    // physical file
    std::unordered_set<std::string> deduplicated_paths_set;
    deduplicated_paths_set.reserve(resolved_paths.size());
    std::vector<std::string> deduplicated_paths;
    deduplicated_paths.reserve(resolved_paths.size());
    for (const auto& path : resolved_paths) {
        if (deduplicated_paths_set.insert(path).second) {
            deduplicated_paths.push_back(path);
        }
    }

    std::vector<std::future<Result<std::pair<bool, std::string>>>> file_exists_futures;
    for (const auto& path : deduplicated_paths) {
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
        if (committables[i]->Identifier() <= committables[i - 1]->Identifier()) {
            return Status::Invalid(
                "Committables must be sorted according to identifiers before filtering. This is "
                "unexpected.");
        }
    }
    // TODO(yonghao.fyh): support commit strict mode last safe snapshot
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                           snapshot_manager_->LatestSnapshotOfUser(commit_user_));
    if (latest_snapshot) {
        std::vector<std::shared_ptr<ManifestCommittable>> result;
        for (const auto& committable : committables) {
            // if committable is newer than latest snapshot, then it hasn't been committed
            if (committable->Identifier() > latest_snapshot.value().CommitIdentifier()) {
                result.push_back(committable);
            } else {
                // TODO(yonghao.fyh): support callback
            }
        }
        return result;
    } else {
        // if there is no previous snapshots then nothing should be filtered
        return committables;
    }
}

Status FileStoreCommitImpl::Overwrite(
    const std::map<std::string, std::string>& partition,
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages, int64_t identifier,
    std::optional<int64_t> watermark) {
    std::shared_ptr<ManifestCommittable> committable =
        CreateManifestCommittable(identifier, commit_messages, watermark);
    PAIMON_LOG_INFO(logger_, "Ready to overwrite to table %s, number of commit messages: %zu",
                    table_name_.c_str(), committable->FileCommittables().size());
    PAIMON_ASSIGN_OR_RAISE(std::string committable_str, committable->ToString());
    std::string partition_str = fmt::format("{}", partition);
    std::string properties_str = fmt::format("{}", committable->Properties());
    PAIMON_LOG_DEBUG(logger_,
                     "Ready to overwrite partitions %s\nManifestCommittable: %s\nProperties: "
                     "%s",
                     partition_str.c_str(), committable_str.c_str(), properties_str.c_str());

    Duration duration;
    int32_t generated_snapshot = 0;
    int32_t attempt = 0;

    std::vector<std::map<std::string, std::string>> partitions;
    if (!partition.empty()) {
        partitions.push_back(partition);
    }

    PAIMON_ASSIGN_OR_RAISE(ManifestEntryChanges changes,
                           CollectChanges(committable->FileCommittables()));
    ManifestEntryChanges report_changes = changes;
    report_changes.append_changelog.clear();
    report_changes.compact_changelog.clear();
    ScopeGuard report_guard([&]() {
        PAIMON_LOG_INFO(logger_, "Finished overwrite to table %s, duration %ld ms",
                        table_name_.c_str(), duration.Get());
        ReportCommit(report_changes, duration.Get(), generated_snapshot, attempt);
    });

    PAIMON_RETURN_NOT_OK(ExecuteOverwrite(partitions, &changes, identifier, watermark, committable,
                                          &generated_snapshot, &attempt));
    return Status::OK();
}

Result<int32_t> FileStoreCommitImpl::FilterAndOverwrite(
    const std::map<std::string, std::string>& partition,
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages, int64_t identifier,
    std::optional<int64_t> watermark) {
    std::shared_ptr<ManifestCommittable> committable =
        CreateManifestCommittable(identifier, commit_messages, watermark);
    PAIMON_LOG_INFO(logger_, "Ready to overwrite to table %s, number of commit messages: %zu",
                    table_name_.c_str(), committable->FileCommittables().size());
    PAIMON_ASSIGN_OR_RAISE(std::string committable_str, committable->ToString());
    std::string partition_str = fmt::format("{}", partition);
    std::string properties_str = fmt::format("{}", committable->Properties());
    PAIMON_LOG_DEBUG(logger_,
                     "Ready to overwrite partitions %s\nManifestCommittable: %s\nProperties: "
                     "%s",
                     partition_str.c_str(), committable_str.c_str(), properties_str.c_str());

    Duration duration;
    int32_t generated_snapshot = 0;
    int32_t attempt = 0;

    std::vector<std::map<std::string, std::string>> partitions;
    if (!partition.empty()) {
        partitions.push_back(partition);
    }

    std::vector<std::shared_ptr<ManifestCommittable>> committables;
    committables.push_back(committable);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<ManifestCommittable>> actual_committables,
                           FilterCommitted(committables));
    if (!actual_committables.empty()) {
        PAIMON_ASSIGN_OR_RAISE(ManifestEntryChanges changes,
                               CollectChanges(actual_committables[0]->FileCommittables()));
        ManifestEntryChanges report_changes = changes;
        report_changes.append_changelog.clear();
        report_changes.compact_changelog.clear();
        ScopeGuard report_guard([&]() {
            PAIMON_LOG_INFO(logger_, "Finished overwrite to table %s, duration %ld ms",
                            table_name_.c_str(), duration.Get());
            ReportCommit(report_changes, duration.Get(), generated_snapshot, attempt);
        });

        PAIMON_RETURN_NOT_OK(ExecuteOverwrite(partitions, &changes, identifier, watermark,
                                              actual_committables[0], &generated_snapshot,
                                              &attempt));
    } else {
        // Align with Java: filtered duplicate is treated as one resolved commit attempt.
        attempt = 1;
        PAIMON_LOG_INFO(logger_, "Finished overwrite to table %s, duration %ld ms",
                        table_name_.c_str(), duration.Get());
    }
    return actual_committables.size();
}

Status FileStoreCommitImpl::ExecuteOverwrite(
    const std::vector<std::map<std::string, std::string>>& partitions,
    ManifestEntryChanges* changes, int64_t identifier, std::optional<int64_t> watermark,
    const std::shared_ptr<ManifestCommittable>& committable, int32_t* generated_snapshot,
    int32_t* attempt) {
    if (!changes->append_changelog.empty() || !changes->compact_changelog.empty()) {
        std::string warning =
            "Overwrite mode currently does not commit any changelog.\n"
            "Please make sure that the partition you're overwriting is not being consumed by a "
            "streaming reader.\n"
            "Ignored changelog files are:\n";
        for (const auto& entry : changes->append_changelog) {
            warning += fmt::format("  * {}\n", entry.ToString());
        }
        for (const auto& entry : changes->compact_changelog) {
            warning += fmt::format("  * {}\n", entry.ToString());
        }
        PAIMON_LOG_WARN(logger_, "%s", warning.c_str());
    }

    bool skip_overwrite = false;
    std::vector<std::map<std::string, std::string>> overwrite_partitions = partitions;
    if (!table_schema_->PartitionKeys().empty() && options_.DynamicPartitionOverwrite()) {
        if (changes->append_table_files.empty()) {
            // in dynamic mode, if there is no changes to commit, no data will be deleted
            skip_overwrite = true;
        } else {
            std::set<std::map<std::string, std::string>> dynamic_partitions;
            for (const auto& entry : changes->append_table_files) {
                std::map<std::string, std::string> partition_map;
                PAIMON_ASSIGN_OR_RAISE(partition_map, PartitionToMap(entry.Partition()));
                dynamic_partitions.insert(std::move(partition_map));
            }
            overwrite_partitions.assign(dynamic_partitions.begin(), dynamic_partitions.end());
        }
    } else if (!partitions.empty()) {
        for (const auto& entry : changes->append_table_files) {
            std::map<std::string, std::string> partition_map;
            PAIMON_ASSIGN_OR_RAISE(partition_map, PartitionToMap(entry.Partition()));
            bool belongs_to_overwrite_partition = false;
            for (const auto& partition_spec : partitions) {
                if (MatchPartitionSpec(partition_map, partition_spec)) {
                    belongs_to_overwrite_partition = true;
                    break;
                }
            }
            if (!belongs_to_overwrite_partition) {
                return Status::Invalid(fmt::format(
                    "Trying to overwrite partitions {}, but the changes in {} does not belong to "
                    "this partition",
                    partitions, partition_map));
            }
        }
    }

    bool with_compact =
        !changes->compact_table_files.empty() || !changes->compact_index_files.empty();
    if (!with_compact) {
        // In overwrite mode, opportunistically promote non-overlapping L0 files (per
        // partition+bucket) to higher levels to reduce future compaction and read amplification,
        // without changing semantics.
        PAIMON_ASSIGN_OR_RAISE(changes->append_table_files,
                               TryUpgrade(changes->append_table_files));
    }

    // overwrite new files
    if (!skip_overwrite) {
        PAIMON_ASSIGN_OR_RAISE(int32_t cnt,
                               TryOverwrite(overwrite_partitions, changes->append_table_files,
                                            changes->append_index_files, identifier, watermark,
                                            committable->Properties()));
        *attempt += cnt;
        *generated_snapshot += 1;
    }

    if (with_compact) {
        PAIMON_ASSIGN_OR_RAISE(int32_t cnt,
                               TryCommit(changes->compact_table_files, /*changelog_files=*/{},
                                         changes->compact_index_files, identifier, watermark,
                                         committable->Properties(), Snapshot::CommitKind::Compact(),
                                         /*detect_conflicts=*/true));
        *attempt += cnt;
        *generated_snapshot += 1;
    }

    return Status::OK();
}

Result<std::string> FileStoreCommitImpl::GetLastCommitTableRequest() {
    return snapshot_commit_->GetLastCommitTableRequest();
}

Result<std::vector<ManifestEntry>> FileStoreCommitImpl::GetAllFiles(
    const Snapshot& snapshot,
    const std::vector<std::map<std::string, std::string>>& partitions) const {
    return commit_scanner_->ReadAllEntriesFromPartitions(snapshot, partitions);
}

Result<std::map<std::string, std::string>> FileStoreCommitImpl::PartitionToMap(
    const BinaryRow& partition) const {
    std::vector<std::pair<std::string, std::string>> part_values;
    PAIMON_ASSIGN_OR_RAISE(part_values, partition_computer_->GeneratePartitionVector(partition));
    std::map<std::string, std::string> partition_map;
    for (const auto& [key, value] : part_values) {
        partition_map[key] = value;
    }
    return partition_map;
}

Result<std::vector<ManifestEntry>> FileStoreCommitImpl::TryUpgrade(
    const std::vector<ManifestEntry>& append_files) const {
    if (!options_.OverwriteUpgrade()) {
        return append_files;
    }

    if (table_schema_->PrimaryKeys().empty()) {
        return append_files;
    }

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema_->TrimmedPrimaryKeys());
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> trimmed_primary_key_fields,
                           table_schema_->GetFields(trimmed_primary_keys));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldsComparator> key_comparator,
                           FieldsComparator::Create(trimmed_primary_key_fields,
                                                    options_.SequenceFieldSortOrderIsAscending()));

    for (const auto& entry : append_files) {
        if (entry.Level() > 0 || entry.Bucket() < 0) {
            return append_files;
        }
    }

    std::unordered_map<std::pair<BinaryRow, int32_t>, std::vector<ManifestEntry>> buckets;
    for (const auto& entry : append_files) {
        buckets[std::make_pair(entry.Partition(), entry.Bucket())].emplace_back(entry);
    }

    std::vector<ManifestEntry> results;
    int32_t max_level = options_.GetNumLevels() - 1;
    for (auto& [_, entries] : buckets) {
        std::vector<ManifestEntry> new_entries = entries;
        std::sort(new_entries.begin(), new_entries.end(),
                  [&key_comparator](const ManifestEntry& a, const ManifestEntry& b) {
                      return key_comparator->CompareTo(a.MinKey(), b.MinKey()) < 0;
                  });

        bool overlap = false;
        for (size_t i = 0; i + 1 < new_entries.size(); ++i) {
            if (key_comparator->CompareTo(new_entries[i].MaxKey(), new_entries[i + 1].MinKey()) >=
                0) {
                overlap = true;
                break;
            }
        }

        if (overlap) {
            results.insert(results.end(), entries.begin(), entries.end());
            continue;
        }

        PAIMON_LOG_INFO(logger_, "%s", "Upgraded for overwrite commit.");
        for (const auto& entry : new_entries) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> upgraded_file,
                                   entry.File()->Upgrade(max_level));
            results.emplace_back(entry.Kind(), entry.Partition(), entry.Bucket(),
                                 entry.TotalBuckets(), upgraded_file);
        }
    }

    return results;
}

Result<int32_t> FileStoreCommitImpl::TryOverwrite(
    const std::vector<std::map<std::string, std::string>>& partitions,
    const std::vector<ManifestEntry>& changes, const std::vector<IndexManifestEntry>& index_entries,
    int64_t commit_identifier, std::optional<int64_t> watermark,
    const std::map<std::string, std::string>& properties) {
    std::shared_ptr<CommitChangesProvider> changes_provider =
        commit_scanner_->OverwriteChangesProvider(partitions, changes, index_entries);
    return TryCommit(changes_provider, commit_identifier, watermark, properties,
                     Snapshot::CommitKind::Overwrite(), /*detect_conflicts=*/true);
}

Status FileStoreCommitImpl::Commit(const std::shared_ptr<ManifestCommittable>& committable,
                                   bool check_append_files) {
    PAIMON_LOG_INFO(logger_, "Ready to commit to table %s, number of commit messages: %zu",
                    table_name_.c_str(), committable->FileCommittables().size());
    PAIMON_ASSIGN_OR_RAISE(std::string committable_str, committable->ToString());
    PAIMON_LOG_DEBUG(logger_, "Ready to commit\n%s", committable_str.c_str());

    Duration duration;
    int32_t generated_snapshot = 0;
    int32_t attempt = 0;

    PAIMON_ASSIGN_OR_RAISE(ManifestEntryChanges changes,
                           CollectChanges(committable->FileCommittables()));
    ScopeGuard report_guard([&]() {
        PAIMON_LOG_INFO(logger_,
                        "Finished (Uncertain of success) commit to table %s, duration %ld ms",
                        table_name_.c_str(), duration.Get());
        ReportCommit(changes, duration.Get(), generated_snapshot, attempt);
    });

    if (!ignore_empty_commit_ || changes.HasAppendChanges()) {
        Snapshot::CommitKind commit_kind = Snapshot::CommitKind::Append();
        if (append_commit_check_conflict_) {
            check_append_files = true;
        }

        if (conflict_detection_.ShouldBeOverwriteCommit(changes.append_table_files,
                                                        changes.append_index_files)) {
            commit_kind = Snapshot::CommitKind::Overwrite();
            check_append_files = true;
        }
        if (conflict_detection_.HasRowIdCheckFromSnapshot()) {
            check_append_files = true;
        }
        if (changes.HasGlobalIndexFileAdditions()) {
            check_append_files = true;
        }

        PAIMON_ASSIGN_OR_RAISE(
            int32_t cnt, TryCommit(changes.append_table_files, changes.append_changelog,
                                   changes.append_index_files, committable->Identifier(),
                                   committable->Watermark(), committable->Properties(), commit_kind,
                                   check_append_files));
        attempt += cnt;
        generated_snapshot += 1;
    }

    if (changes.HasCompactChanges()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t cnt,
                               TryCommit(changes.compact_table_files, changes.compact_changelog,
                                         changes.compact_index_files, committable->Identifier(),
                                         committable->Watermark(), committable->Properties(),
                                         Snapshot::CommitKind::Compact(),
                                         /*detect_conflicts=*/true));
        attempt += cnt;
        generated_snapshot += 1;
    }
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
                                               const std::vector<ManifestEntry>& changelog_files,
                                               const std::vector<IndexManifestEntry>& index_entries,
                                               int64_t identifier, std::optional<int64_t> watermark,
                                               const std::map<std::string, std::string>& properties,
                                               Snapshot::CommitKind commit_kind,
                                               bool detect_conflicts) {
    std::shared_ptr<CommitChangesProvider> changes_provider =
        CommitChangesProvider::Provider(delta_files, changelog_files, index_entries);
    return TryCommit(changes_provider, identifier, watermark, properties, commit_kind,
                     detect_conflicts);
}

Result<int32_t> FileStoreCommitImpl::TryCommit(
    const std::shared_ptr<CommitChangesProvider>& changes_provider, int64_t identifier,
    std::optional<int64_t> watermark, const std::map<std::string, std::string>& properties,
    Snapshot::CommitKind commit_kind, bool detect_conflicts) {
    int32_t retry_count = 0;
    int64_t start_millis = DateTimeUtils::GetCurrentUTCTimeUs() / 1000;
    std::optional<int64_t> retry_start_snapshot_id;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                               snapshot_manager_->LatestSnapshot());
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CommitChanges> commit_changes,
                               changes_provider->Provide(latest_snapshot));
        PAIMON_ASSIGN_OR_RAISE(
            bool commit_success,
            TryCommitOnce(commit_changes->delta_files, commit_changes->changelog_files,
                          commit_changes->index_entries, identifier, watermark, properties,
                          commit_kind, latest_snapshot, detect_conflicts, retry_start_snapshot_id));
        if (commit_success) {
            break;
        }
        retry_start_snapshot_id = latest_snapshot
                                      ? std::optional<int64_t>(latest_snapshot.value().Id() + 1)
                                      : std::optional<int64_t>(Snapshot::FIRST_SNAPSHOT_ID);
        int64_t current_millis = DateTimeUtils::GetCurrentUTCTimeUs() / 1000;
        if (current_millis - start_millis > options_.GetCommitTimeout() ||
            retry_count >= options_.GetCommitMaxRetries()) {
            return Status::Invalid(
                fmt::format("Commit failed after {} millis with {} retries, there maybe exist "
                            "commit conflicts between multiple jobs.",
                            options_.GetCommitTimeout(), retry_count));
        }
        retry_waiter_.RetryWait(retry_count);
        retry_count++;
    }
    return retry_count + 1;
}

Result<bool> FileStoreCommitImpl::CheckCommitted(const std::optional<Snapshot>& latest_snapshot,
                                                 std::optional<int64_t> retry_start_snapshot_id,
                                                 int64_t identifier,
                                                 const Snapshot::CommitKind& commit_kind) const {
    if (!latest_snapshot || !retry_start_snapshot_id ||
        retry_start_snapshot_id.value() > latest_snapshot.value().Id()) {
        return false;
    }

    for (int64_t snapshot_id = retry_start_snapshot_id.value();
         snapshot_id <= latest_snapshot.value().Id(); ++snapshot_id) {
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, snapshot_manager_->LoadSnapshot(snapshot_id));
        if (snapshot.CommitUser() == commit_user_ && snapshot.CommitIdentifier() == identifier &&
            snapshot.GetCommitKind() == commit_kind) {
            return true;
        }
    }
    return false;
}

Status FileStoreCommitImpl::CheckSameBucketFromSnapshot(
    const std::vector<ManifestEntry>& delta_entries,
    const std::optional<Snapshot>& latest_snapshot) const {
    if (!latest_snapshot) {
        return Status::OK();
    }

    std::unordered_map<BinaryRow, int32_t> expected_total_buckets;
    PAIMON_RETURN_NOT_OK(conflict_detection_.CollectUncheckedBucketPartitions(
        delta_entries, &expected_total_buckets));
    if (expected_total_buckets.empty()) {
        return Status::OK();
    }

    std::vector<BinaryRow> changed_partitions;
    changed_partitions.reserve(expected_total_buckets.size());
    for (const auto& [partition, _] : expected_total_buckets) {
        changed_partitions.push_back(partition);
    }

    PAIMON_ASSIGN_OR_RAISE(
        auto previous_total_buckets,
        commit_scanner_->ReadTotalBuckets(latest_snapshot.value(), changed_partitions));

    return conflict_detection_.CheckSameBucketByTotalBuckets(expected_total_buckets,
                                                             previous_total_buckets);
}

bool FileStoreCommitImpl::ShouldCheckSameBucket(const Snapshot::CommitKind& commit_kind) const {
    return commit_kind == Snapshot::CommitKind::Append() &&
           bucket_mode_ == BucketMode::HASH_FIXED &&
           (IsUnorderedWriteOnlyAppend() || IsWriteOnlySnapshotSequenceAppend());
}

bool FileStoreCommitImpl::IsUnorderedWriteOnlyAppend() const {
    return options_.WriteOnly() && !options_.BucketAppendOrdered();
}

bool FileStoreCommitImpl::IsWriteOnlySnapshotSequenceAppend() const {
    return options_.WriteOnly() &&
           options_.WriteSequenceNumberInitMode() == CoreOptions::SequenceNumberInitMode::SNAPSHOT;
}

Result<std::optional<int64_t>> FileStoreCommitImpl::MaxSequenceNumber(
    const std::vector<ManifestFileMeta>& manifests) const {
    int64_t max_from_manifest = std::numeric_limits<int64_t>::min();
    bool found = false;
    for (const auto& manifest : manifests) {
        std::vector<ManifestEntry> entries;
        PAIMON_RETURN_NOT_OK(manifest_file_->Read(
            manifest.FileName(), [](const ManifestEntry&) -> Result<bool> { return true; },
            &entries));
        std::optional<int64_t> current_max =
            SequenceSnapshotProperties::MaxSequenceNumberFromFiles(entries);
        if (current_max) {
            max_from_manifest = std::max(max_from_manifest, current_max.value());
            found = true;
        }
    }

    if (!found) {
        return std::optional<int64_t>();
    }
    return std::optional<int64_t>(max_from_manifest);
}

Result<bool> FileStoreCommitImpl::TryCommitOnce(
    const std::vector<ManifestEntry>& delta_entries,
    const std::vector<ManifestEntry>& changelog_entries,
    const std::vector<IndexManifestEntry>& index_entries, int64_t identifier,
    std::optional<int64_t> watermark, const std::map<std::string, std::string>& properties,
    Snapshot::CommitKind commit_kind, const std::optional<Snapshot>& latest_snapshot,
    bool detect_conflicts, std::optional<int64_t> retry_start_snapshot_id) {
    PAIMON_ASSIGN_OR_RAISE(bool committed, CheckCommitted(latest_snapshot, retry_start_snapshot_id,
                                                          identifier, commit_kind));
    if (committed) {
        return true;
    }

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

    // TODO(yonghao.fyh): support strict mode checker

    PAIMON_LOG_DEBUG(logger_, "Ready to commit table files to snapshot %ld", new_snapshot_id);
    for (const ManifestEntry& entry : delta_files) {
        PAIMON_LOG_DEBUG(logger_, "  * %s", entry.ToString().c_str());
    }
    PAIMON_LOG_DEBUG(logger_, "Ready to commit changelog files to snapshot %ld", new_snapshot_id);
    for (const ManifestEntry& entry : changelog_entries) {
        PAIMON_LOG_DEBUG(logger_, "  * %s", entry.ToString().c_str());
    }

    bool discard_duplicate =
        options_.CommitDiscardDuplicateFiles() && commit_kind == Snapshot::CommitKind::Append();
    bool check_conflicts = latest_snapshot.has_value() && (discard_duplicate || detect_conflicts);
    // By default, if checkConflicts is required, we do not have to do the extra check bucket
    // here.
    if (!check_conflicts && ShouldCheckSameBucket(commit_kind)) {
        PAIMON_RETURN_NOT_OK(CheckSameBucketFromSnapshot(delta_files, latest_snapshot));
    }

    if (check_conflicts) {
        // latest snapshot id is different from the snapshot id we've checked for conflicts,
        // so we have to check again
        std::vector<BinaryRow> changed_partitions =
            ManifestEntryChanges::ChangedPartitions(delta_files, index_entries);
        PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> base_data_files,
                               commit_scanner_->ReadAllEntriesFromChangedPartitions(
                                   latest_snapshot.value(), changed_partitions));

        if (discard_duplicate) {
            std::unordered_set<FileEntry::Identifier> base_identifiers;
            base_identifiers.reserve(base_data_files.size());
            for (const auto& entry : base_data_files) {
                base_identifiers.insert(entry.CreateIdentifier());
            }

            delta_files.erase(
                std::remove_if(delta_files.begin(), delta_files.end(),
                               [&base_identifiers](const ManifestEntry& entry) {
                                   return base_identifiers.find(entry.CreateIdentifier()) !=
                                          base_identifiers.end();
                               }),
                delta_files.end());
        }

        std::optional<std::shared_ptr<RowIdColumnConflictChecker>> row_id_column_conflict_checker =
            std::nullopt;
        if (conflict_detection_.HasRowIdCheckFromSnapshot()) {
            std::vector<std::shared_ptr<DataFileMeta>> delta_data_files;
            delta_data_files.reserve(delta_files.size());
            for (const auto& entry : delta_files) {
                delta_data_files.push_back(entry.File());
            }
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<RowIdColumnConflictChecker> checker,
                RowIdColumnConflictChecker::FromDataFiles(schema_manager_, delta_data_files));
            row_id_column_conflict_checker = checker;
        }

        PAIMON_RETURN_NOT_OK(conflict_detection_.CheckConflicts(
            latest_snapshot.value(), base_data_files, delta_files, index_entries,
            row_id_column_conflict_checker, commit_kind));
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
        previous_total_record_count = latest_snapshot.value().TotalRecordCount();
        std::vector<ManifestFileMeta> previous_manifests;
        // read all previous manifest files
        PAIMON_RETURN_NOT_OK(
            manifest_list_->ReadDataManifests(latest_snapshot.value(), &previous_manifests));
        merge_before_manifests.insert(merge_before_manifests.end(), previous_manifests.begin(),
                                      previous_manifests.end());
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
        if (options_.RowTrackingPartitionGroupOnCommit()) {
            std::unordered_map<BinaryRow, std::vector<ManifestEntry>> delta_files_by_partition;
            for (auto& entry : delta_files) {
                delta_files_by_partition[entry.Partition()].push_back(std::move(entry));
            }
            delta_files.clear();
            for (auto& [_, entries] : delta_files_by_partition) {
                delta_files.insert(delta_files.end(), std::make_move_iterator(entries.begin()),
                                   std::make_move_iterator(entries.end()));
            }
        }

        PAIMON_ASSIGN_OR_RAISE(RowTrackingCommitUtils::RowTrackingAssigned assigned,
                               RowTrackingCommitUtils::AssignRowTracking(
                                   new_snapshot_id, first_row_id_start, delta_files));
        next_row_id_start = assigned.next_row_id_start;
        delta_files = std::move(assigned.assigned_entries);
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

    // write changelog into manifest files
    std::optional<std::pair<std::string, int64_t>> changelog_manifest_list;
    if (!changelog_entries.empty()) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestFileMeta> changelog_manifests,
                               manifest_file_->Write(changelog_entries));
        PAIMON_ASSIGN_OR_RAISE(changelog_manifest_list, manifest_list_->Write(changelog_manifests));
    }

    PAIMON_ASSIGN_OR_RAISE(index_manifest_name, index_manifest_file_->WriteIndexFiles(
                                                    old_index_manifest, index_entries));

    std::optional<std::string> statistics =
        latest_snapshot ? latest_snapshot.value().Statistics() : std::nullopt;
    std::optional<int64_t> changelog_record_count =
        ManifestEntry::NullableRecordCount(changelog_entries);
    int64_t schema_id = 0;
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> table_schema,
                           schema_manager_->Latest());
    if (table_schema) {
        schema_id = table_schema.value()->Id();
    }

    // Keep Java semantics: inherit previous stats only when schema matches.
    if (statistics && latest_snapshot && latest_snapshot.value().SchemaId() != schema_id) {
        PAIMON_LOG_WARN(logger_, "%s", "Schema changed, stats will not be inherited");
        statistics = std::nullopt;
    }

    std::map<std::string, std::string> snapshot_properties = properties;
    if (options_.WriteSequenceNumberInitMode() == CoreOptions::SequenceNumberInitMode::SNAPSHOT) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> latest_max_sequence_number,
                               SequenceSnapshotProperties::MaxSequenceNumber(latest_snapshot));
        if (!latest_max_sequence_number && latest_snapshot) {
            PAIMON_ASSIGN_OR_RAISE(latest_max_sequence_number,
                                   MaxSequenceNumber(merge_before_manifests));
        }
        snapshot_properties = SequenceSnapshotProperties::MergeMaxSequenceNumber(
            snapshot_properties, latest_max_sequence_number, delta_files);
    }

    // prepare snapshot file
    Snapshot new_snapshot(
        new_snapshot_id, schema_id, base_manifest_list.first, base_manifest_list.second,
        delta_manifest_list.first, delta_manifest_list.second,
        changelog_manifest_list ? std::optional<std::string>(changelog_manifest_list.value().first)
                                : std::nullopt,
        changelog_manifest_list ? std::optional<int64_t>(changelog_manifest_list.value().second)
                                : std::nullopt,
        index_manifest_name, commit_user_, identifier, commit_kind,
        DateTimeUtils::GetCurrentUTCTimeUs() / 1000, total_record_count, delta_record_count,
        changelog_record_count, watermark, statistics,
        snapshot_properties.empty()
            ? std::nullopt
            : std::optional<std::map<std::string, std::string>>(snapshot_properties),
        next_row_id_start);

    Result<bool> commit_result = CommitSnapshotImpl(new_snapshot, delta_statistics);
    if (!commit_result.ok()) {
        // commit exception is uncertain; retry after checking whether this commit already exists.
        PAIMON_LOG_WARN(logger_, "Retry commit for exception. %s",
                        commit_result.status().ToString().c_str());
        guard.Release();
        return false;
    }
    bool commit_success = commit_result.value();
    if (commit_success) {
        PAIMON_LOG_INFO(logger_,
                        "Successfully commit snapshot %ld to table %s by user %s with identifier "
                        "%ld and kind %s.",
                        new_snapshot.Id(), root_path_.c_str(), commit_user_.c_str(),
                        new_snapshot.CommitIdentifier(),
                        Snapshot::CommitKind::ToString(new_snapshot.GetCommitKind()).c_str());
        last_committed_snapshot_id_ = new_snapshot.Id();
        guard.Release();
        return true;
    } else {
        // commit fails, should clean up the files
        return false;
    }
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

Result<ManifestEntryChanges> FileStoreCommitImpl::CollectChanges(
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages) {
    ManifestEntryChanges changes(num_bucket_);
    for (const auto& message : commit_messages) {
        PAIMON_RETURN_NOT_OK(changes.Collect(message));
    }
    PAIMON_LOG_INFO(logger_, "Finished collecting changes, including: %s",
                    changes.ToString().c_str());
    return changes;
}

void FileStoreCommitImpl::ReportCommit(const ManifestEntryChanges& changes, int64_t commit_duration,
                                       int32_t generated_snapshot, int32_t attempt) {
    CommitStats commit_stats(changes.append_table_files, changes.append_changelog,
                             changes.compact_table_files, changes.compact_changelog,
                             commit_duration, generated_snapshot, attempt,
                             last_committed_snapshot_id_);
    CommitMetrics::ReportCommit(metrics_, commit_stats);
}

}  // namespace paimon
