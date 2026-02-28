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

#include <memory>
#include <vector>

#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/stats/simple_stats_evolutions.h"
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
class SimpleStatsEvolution;
class SimpleStatsEvolutions;
class SnapshotManager;
class TableSchema;
struct DataFileMeta;

/// `FileStoreScan` for `AppendOnlyFileStore`.
class AppendOnlyFileStoreScan : public FileStoreScan {
 public:
    static Result<std::unique_ptr<AppendOnlyFileStoreScan>> Create(
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<SchemaManager>& schema_manager,
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::shared_ptr<ScanFilter>& scan_filters, const CoreOptions& core_options,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool);

    /// @note Keep this thread-safe.
    Result<bool> FilterByStats(const ManifestEntry& entry) const override;

 private:
    Result<bool> TestFileIndex(const std::shared_ptr<DataFileMeta>& meta,
                               const std::shared_ptr<SimpleStatsEvolution>& evolution,
                               const std::shared_ptr<TableSchema>& data_schema) const;

    AppendOnlyFileStoreScan(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                            const std::shared_ptr<SchemaManager>& schema_manager,
                            const std::shared_ptr<ManifestList>& manifest_list,
                            const std::shared_ptr<ManifestFile>& manifest_file,
                            const std::shared_ptr<TableSchema>& table_schema,
                            const std::shared_ptr<arrow::Schema>& schema,
                            const CoreOptions& core_options,
                            const std::shared_ptr<Executor>& executor,
                            const std::shared_ptr<MemoryPool>& pool);

 private:
    std::shared_ptr<SimpleStatsEvolutions> evolutions_;
};
}  // namespace paimon
