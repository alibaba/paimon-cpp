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
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats_evolution.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class CoreOptions;
class Executor;
class ManifestEntry;
class ManifestFile;
class ManifestList;
class MemoryPool;
class ScanFilter;
class SchemaManager;
class SnapshotManager;

/// `FileStoreScan` for data-evolution enabled table.
class DataEvolutionFileStoreScan : public FileStoreScan {
 public:
    static Result<std::unique_ptr<DataEvolutionFileStoreScan>> Create(
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<SchemaManager>& schema_manager,
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::shared_ptr<ScanFilter>& scan_filters, const CoreOptions& core_options,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
        auto scan = std::unique_ptr<DataEvolutionFileStoreScan>(new DataEvolutionFileStoreScan(
            snapshot_manager, schema_manager, manifest_list, manifest_file, table_schema,
            arrow_schema, core_options, executor, pool));
        PAIMON_RETURN_NOT_OK(
            scan->SplitAndSetFilter(table_schema->PartitionKeys(), arrow_schema, scan_filters));
        return scan;
    }

    std::vector<ManifestFileMeta> PostFilterManifests(
        std::vector<ManifestFileMeta>&& manifests) const override;

    Result<std::vector<ManifestEntry>> PostFilterManifestEntries(
        std::vector<ManifestEntry>&& entries) const override;

    /// @note Keep this thread-safe.
    Result<bool> FilterByStats(const ManifestEntry& entry) const override;

 private:
    DataEvolutionFileStoreScan(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                               const std::shared_ptr<SchemaManager>& schema_manager,
                               const std::shared_ptr<ManifestList>& manifest_list,
                               const std::shared_ptr<ManifestFile>& manifest_file,
                               const std::shared_ptr<TableSchema>& table_schema,
                               const std::shared_ptr<arrow::Schema>& schema,
                               const CoreOptions& core_options,
                               const std::shared_ptr<Executor>& executor,
                               const std::shared_ptr<MemoryPool>& pool)
        : FileStoreScan(snapshot_manager, schema_manager, manifest_list, manifest_file,
                        table_schema, schema, core_options, executor, pool) {}

    Result<bool> FilterByStatsWithSameRowId(const std::vector<ManifestEntry>& entries) const;

    static bool FilterManifestByRowRanges(const ManifestFileMeta& manifest,
                                          const std::optional<std::vector<Range>>& row_ranges);
    static Result<bool> FilterEntryByRowRanges(const ManifestEntry& entry,
                                               const std::optional<std::vector<Range>>& row_ranges);

    static Result<std::pair<int64_t, SimpleStatsEvolution::EvolutionStats>> EvolutionStats(
        const std::vector<ManifestEntry>& entries, const std::shared_ptr<TableSchema>& table_schema,
        const std::function<Result<std::shared_ptr<TableSchema>>(int64_t)>& schema_fetcher);
};
}  // namespace paimon
