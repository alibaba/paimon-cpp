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
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/core_options.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class BinaryRowPartitionComputer;
class CommitChangesProvider;
class Executor;
class FileStoreScan;
struct IndexManifestEntry;
class IndexManifestFile;
class ManifestEntry;
class ManifestFile;
class ManifestList;
class MemoryPool;
class ScanFilter;
class SchemaManager;
class Snapshot;
class SnapshotManager;
class TableSchema;

// Manifest entries scanner for commit operations.
class CommitScanner {
 public:
    using ScanSupplier =
        std::function<Result<std::unique_ptr<FileStoreScan>>(const std::shared_ptr<ScanFilter>&)>;

    CommitScanner(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                  const std::shared_ptr<SchemaManager>& schema_manager,
                  const std::shared_ptr<ManifestList>& manifest_list,
                  const std::shared_ptr<ManifestFile>& manifest_file,
                  const std::shared_ptr<IndexManifestFile>& index_manifest_file,
                  const std::shared_ptr<TableSchema>& table_schema,
                  const std::shared_ptr<arrow::Schema>& schema, const CoreOptions& core_options,
                  const std::shared_ptr<Executor>& executor,
                  const std::shared_ptr<MemoryPool>& pool,
                  const BinaryRowPartitionComputer* partition_computer, ScanSupplier scan_supplier);

    Result<std::vector<ManifestEntry>> ReadAllEntriesFromChangedPartitions(
        const Snapshot& snapshot, const std::vector<BinaryRow>& changed_partitions) const;

    Result<std::vector<ManifestEntry>> ReadIncrementalEntries(
        const Snapshot& snapshot, const std::vector<BinaryRow>& changed_partitions) const;

    Result<std::unordered_map<BinaryRow, int32_t>> ReadTotalBuckets(
        const Snapshot& snapshot, const std::vector<BinaryRow>& changed_partitions) const;

    Result<std::vector<ManifestEntry>> ReadAllEntriesFromPartitions(
        const Snapshot& snapshot,
        const std::vector<std::map<std::string, std::string>>& partitions) const;

    Result<std::vector<IndexManifestEntry>> ReadAllIndexEntriesFromPartitions(
        const Snapshot& snapshot,
        const std::vector<std::map<std::string, std::string>>& partitions) const;

    std::shared_ptr<CommitChangesProvider> OverwriteChangesProvider(
        const std::vector<std::map<std::string, std::string>>& partitions,
        const std::vector<ManifestEntry>& changes,
        const std::vector<IndexManifestEntry>& index_entries) const;

 private:
    Result<std::vector<std::map<std::string, std::string>>> ToPartitionFilters(
        const std::vector<BinaryRow>& changed_partitions) const;

    Result<std::unique_ptr<FileStoreScan>> NewScan(
        const std::vector<std::map<std::string, std::string>>& partitions,
        bool for_overwrite) const;

 private:
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<SchemaManager> schema_manager_;
    std::shared_ptr<ManifestList> manifest_list_;
    std::shared_ptr<ManifestFile> manifest_file_;
    std::shared_ptr<IndexManifestFile> index_manifest_file_;
    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<arrow::Schema> schema_;
    CoreOptions core_options_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<MemoryPool> pool_;
    const BinaryRowPartitionComputer* partition_computer_;
    ScanSupplier scan_supplier_;
};

}  // namespace paimon
