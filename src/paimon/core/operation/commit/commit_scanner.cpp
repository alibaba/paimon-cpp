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

#include "paimon/core/operation/commit/commit_scanner.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/operation/commit/overwrite_changes_provider.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/scan_context.h"

namespace paimon {

CommitScanner::CommitScanner(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                             const std::shared_ptr<SchemaManager>& schema_manager,
                             const std::shared_ptr<ManifestList>& manifest_list,
                             const std::shared_ptr<ManifestFile>& manifest_file,
                             const std::shared_ptr<IndexManifestFile>& index_manifest_file,
                             const std::shared_ptr<TableSchema>& table_schema,
                             const std::shared_ptr<arrow::Schema>& schema,
                             const CoreOptions& core_options,
                             const std::shared_ptr<Executor>& executor,
                             const std::shared_ptr<MemoryPool>& pool,
                             const BinaryRowPartitionComputer* partition_computer,
                             ScanSupplier scan_supplier)
    : snapshot_manager_(snapshot_manager),
      schema_manager_(schema_manager),
      manifest_list_(manifest_list),
      manifest_file_(manifest_file),
      index_manifest_file_(index_manifest_file),
      table_schema_(table_schema),
      schema_(schema),
      core_options_(core_options),
      executor_(executor),
      pool_(pool),
      partition_computer_(partition_computer),
      scan_supplier_(std::move(scan_supplier)) {}

Result<std::vector<std::map<std::string, std::string>>> CommitScanner::ToPartitionFilters(
    const std::vector<BinaryRow>& changed_partitions) const {
    std::vector<std::map<std::string, std::string>> partition_filters;
    partition_filters.reserve(changed_partitions.size());

    for (const BinaryRow& changed_partition : changed_partitions) {
        std::vector<std::pair<std::string, std::string>> part_values;
        PAIMON_ASSIGN_OR_RAISE(part_values,
                               partition_computer_->GeneratePartitionVector(changed_partition));
        std::map<std::string, std::string> partition_filter;
        for (const auto& [key, value] : part_values) {
            partition_filter[key] = value;
        }
        partition_filters.push_back(std::move(partition_filter));
    }

    return partition_filters;
}

Result<std::vector<ManifestEntry>> CommitScanner::ReadAllEntriesFromChangedPartitions(
    const Snapshot& snapshot, const std::vector<BinaryRow>& changed_partitions) const {
    if (changed_partitions.empty()) {
        return std::vector<ManifestEntry>{};
    }

    std::vector<std::map<std::string, std::string>> partition_filters;
    PAIMON_ASSIGN_OR_RAISE(partition_filters, ToPartitionFilters(changed_partitions));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan,
                           NewScan(partition_filters, /*for_overwrite=*/false));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> plan,
                           scan->WithSnapshot(snapshot)->WithKind(ScanMode::ALL)->CreatePlan());
    return plan->Files();
}

Result<std::vector<ManifestEntry>> CommitScanner::ReadIncrementalEntries(
    const Snapshot& snapshot, const std::vector<BinaryRow>& changed_partitions) const {
    if (changed_partitions.empty()) {
        return std::vector<ManifestEntry>{};
    }

    std::unordered_set<BinaryRow> changed_partition_set(changed_partitions.begin(),
                                                        changed_partitions.end());
    std::vector<ManifestFileMeta> delta_manifests;
    PAIMON_RETURN_NOT_OK(manifest_list_->ReadDeltaManifests(snapshot, &delta_manifests));

    std::vector<ManifestEntry> incremental_entries;
    for (const ManifestFileMeta& manifest_meta : delta_manifests) {
        std::vector<ManifestEntry> manifest_entries;
        PAIMON_RETURN_NOT_OK(
            manifest_file_->Read(manifest_meta.FileName(), /*filter=*/nullptr, &manifest_entries));
        for (const ManifestEntry& entry : manifest_entries) {
            if (changed_partition_set.find(entry.Partition()) != changed_partition_set.end()) {
                incremental_entries.push_back(entry);
            }
        }
    }

    return incremental_entries;
}

Result<std::vector<ManifestEntry>> CommitScanner::ReadAllEntriesFromPartitions(
    const Snapshot& snapshot,
    const std::vector<std::map<std::string, std::string>>& partitions) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan,
                           NewScan(partitions, /*for_overwrite=*/false));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> plan,
                           scan->WithSnapshot(snapshot)->WithKind(ScanMode::ALL)->CreatePlan());
    return plan->Files();
}

Result<std::unique_ptr<FileStoreScan>> CommitScanner::NewScan(
    const std::vector<std::map<std::string, std::string>>& partitions, bool for_overwrite) const {
    auto scan_filter = std::make_shared<ScanFilter>(/*predicate=*/nullptr, partitions,
                                                    /*bucket_filter=*/std::nullopt);
    if (!scan_supplier_) {
        return Status::Invalid("CommitScanner requires non-empty scan supplier.");
    }

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan, scan_supplier_(scan_filter));
    if (for_overwrite && core_options_.GetBucket() != BucketModeDefine::POSTPONE_BUCKET) {
        scan->OnlyReadRealBuckets();
    }
    return scan;
}

Result<std::vector<IndexManifestEntry>> CommitScanner::ReadAllIndexEntriesFromPartitions(
    const Snapshot& snapshot,
    const std::vector<std::map<std::string, std::string>>& partitions) const {
    std::vector<IndexManifestEntry> index_entries;
    if (!snapshot.IndexManifest()) {
        return index_entries;
    }

    auto filter = [this, &partitions](const IndexManifestEntry& entry) -> Result<bool> {
        if (partitions.empty()) {
            return true;
        }

        std::vector<std::pair<std::string, std::string>> part_values;
        PAIMON_ASSIGN_OR_RAISE(part_values,
                               partition_computer_->GeneratePartitionVector(entry.partition));
        std::map<std::string, std::string> partition;
        for (const auto& [key, value] : part_values) {
            partition[key] = value;
        }

        for (const auto& partition_spec : partitions) {
            bool matched = true;
            for (const auto& [key, value] : partition_spec) {
                auto iter = partition.find(key);
                if (iter == partition.end() || iter->second != value) {
                    matched = false;
                    break;
                }
            }
            if (matched) {
                return true;
            }
        }
        return false;
    };

    PAIMON_RETURN_NOT_OK(
        index_manifest_file_->Read(snapshot.IndexManifest().value(), filter, &index_entries));
    return index_entries;
}

std::shared_ptr<CommitChangesProvider> CommitScanner::OverwriteChangesProvider(
    const std::vector<std::map<std::string, std::string>>& partitions,
    const std::vector<ManifestEntry>& changes,
    const std::vector<IndexManifestEntry>& index_entries) const {
    return std::make_shared<paimon::OverwriteChangesProvider>(
        changes, index_entries,
        [this, partitions](const Snapshot& snapshot) -> Result<std::vector<ManifestEntry>> {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan,
                                   NewScan(partitions, /*for_overwrite=*/true));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<FileStoreScan::RawPlan> plan,
                scan->WithSnapshot(snapshot)->WithKind(ScanMode::ALL)->CreatePlan());
            return plan->Files();
        },
        [this, partitions](const Snapshot& snapshot) {
            return ReadAllIndexEntriesFromPartitions(snapshot, partitions);
        });
}

Result<std::unordered_map<BinaryRow, int32_t>> CommitScanner::ReadTotalBuckets(
    const Snapshot& snapshot, const std::vector<BinaryRow>& changed_partitions) const {
    std::unordered_map<BinaryRow, int32_t> total_buckets;
    if (changed_partitions.empty()) {
        return total_buckets;
    }

    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> entries,
                           ReadAllEntriesFromChangedPartitions(snapshot, changed_partitions));

    std::unordered_set<BinaryRow> remaining_partitions(changed_partitions.begin(),
                                                       changed_partitions.end());
    for (const ManifestEntry& entry : entries) {
        if (remaining_partitions.empty()) {
            break;
        }
        if (!(entry.Kind() == FileKind::Add()) || entry.TotalBuckets() <= 0) {
            continue;
        }
        if (remaining_partitions.erase(entry.Partition()) > 0) {
            total_buckets.emplace(entry.Partition(), entry.TotalBuckets());
        }
    }

    return total_buckets;
}

}  // namespace paimon
