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
#include <string>
#include <vector>

#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/table/source/scan_mode.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class CoreOptions;
class Executor;
class ManifestFile;
class ManifestList;
class MemoryPool;
class PredicateFilter;
class ScanFilter;
class SchemaManager;
class SimpleStatsEvolutions;
class SnapshotManager;
class TableSchema;

/// `FileStoreScan` for `KeyValueFileStore`.
class KeyValueFileStoreScan : public FileStoreScan {
 public:
    static Result<std::unique_ptr<KeyValueFileStoreScan>> Create(
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<SchemaManager>& schema_manager,
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::shared_ptr<ScanFilter>& scan_filters, const CoreOptions& core_options,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool);

    FileStoreScan* EnableValueFilter() override {
        value_filter_force_enabled_ = true;
        return this;
    }

    /// @note Keep this thread-safe.
    Result<bool> FilterByStats(const ManifestEntry& entry) const override;

    bool WholeBucketFilterEnabled() const override {
        return value_filter_ != nullptr && scan_mode_ == ScanMode::ALL;
    }

    Result<std::vector<ManifestEntry>> FilterWholeBucketByStats(
        std::vector<ManifestEntry>&& entries) const override;

 private:
    KeyValueFileStoreScan* WithKeyFilter(const std::shared_ptr<PredicateFilter>& predicate) {
        key_filter_ = predicate;
        return this;
    }

    KeyValueFileStoreScan* WithValueFilter(const std::shared_ptr<PredicateFilter>& predicate) {
        value_filter_ = predicate;
        return this;
    }

    Status SplitAndSetKeyValueFilter(const std::vector<std::string>& trimmed_pk);

    Result<bool> IsValueFilterEnabled() const;

    Result<bool> FilterByValueFilter(const ManifestEntry& entry) const;

    static bool NoOverlapping(const std::vector<ManifestEntry>& entries);

    Result<std::vector<ManifestEntry>> FilterWholeBucketPerFile(
        std::vector<ManifestEntry>&& entries) const;

    Result<std::vector<ManifestEntry>> FilterWholeBucketAllFiles(
        std::vector<ManifestEntry>&& entries) const;

    KeyValueFileStoreScan(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                          const std::shared_ptr<SchemaManager>& schema_manager,
                          const std::shared_ptr<ManifestList>& manifest_list,
                          const std::shared_ptr<ManifestFile>& manifest_file,
                          const std::shared_ptr<TableSchema>& table_schema,
                          const std::shared_ptr<arrow::Schema>& schema,
                          const CoreOptions& core_options,
                          const std::shared_ptr<Executor>& executor,
                          const std::shared_ptr<MemoryPool>& pool);

 private:
    bool value_filter_force_enabled_ = false;
    std::shared_ptr<PredicateFilter> key_filter_;
    std::shared_ptr<PredicateFilter> value_filter_;
    std::shared_ptr<SimpleStatsEvolutions> evolutions_;
};
}  // namespace paimon
