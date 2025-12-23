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

#include <cassert>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/predicate/compound_predicate_impl.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/manifest/partition_entry.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/source/scan_mode.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/executor.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class Executor;
class FileKind;
class ManifestFile;
class ManifestFileMeta;
class ManifestList;
class MemoryPool;
class ScanFilter;
class SchemaManager;
class SnapshotManager;
class TableSchema;

/// Scan operation which produces a plan.
class FileStoreScan {
 public:
    virtual ~FileStoreScan() = default;
    FileStoreScan(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                  const std::shared_ptr<SchemaManager>& schema_manager,
                  const std::shared_ptr<ManifestList>& manifest_list,
                  const std::shared_ptr<ManifestFile>& manifest_file,
                  const std::shared_ptr<TableSchema>& table_schema,
                  const std::shared_ptr<arrow::Schema>& schema, const CoreOptions& core_options,
                  const std::shared_ptr<Executor>& executor,
                  const std::shared_ptr<MemoryPool>& pool)
        : pool_(pool),
          schema_manager_(schema_manager),
          schema_(schema),
          table_schema_(table_schema),
          core_options_(core_options),
          snapshot_manager_(snapshot_manager),
          manifest_list_(manifest_list),
          manifest_file_(manifest_file),
          executor_(executor) {
        assert(executor_);
    }

    FileStoreScan* WithKind(const ScanMode& scan_mode) {
        scan_mode_ = scan_mode;
        return this;
    }

    FileStoreScan* WithSnapshot(const Snapshot& snapshot) {
        specified_snapshot_ = snapshot;
        return this;
    }

    FileStoreScan* WithLevelFilter(const std::function<bool(int32_t)>& level_filter) {
        level_filter_ = level_filter;
        return this;
    }

    FileStoreScan* OnlyReadRealBuckets() {
        only_read_real_buckets_ = true;
        return this;
    }

    FileStoreScan* WithRowRanges(const std::vector<Range>& row_ranges) {
        row_ranges_ = row_ranges;
        return this;
    }

    virtual FileStoreScan* EnableValueFilter() {
        return this;
    }

    const std::shared_ptr<SnapshotManager>& GetSnapshotManager() const {
        return snapshot_manager_;
    }

    const CoreOptions& GetCoreOptions() const {
        return core_options_;
    }

    std::shared_ptr<PredicateFilter> GetNonPartitionPredicate() const {
        return predicates_;
    }

    std::shared_ptr<PredicateFilter> GetPartitionPredicate() const {
        return partition_filter_;
    }

    static Result<std::shared_ptr<PredicateFilter>> CreatePartitionPredicate(
        const std::vector<std::string>& partition_keys, const std::string& partition_default_name,
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::vector<std::map<std::string, std::string>>& partition_filters);

    class RawPlan {
     public:
        RawPlan(const ScanMode& scan_mode, const std::optional<Snapshot>& snapshot,
                std::vector<ManifestEntry>&& entries)
            : snapshot_(snapshot), manifest_entries_(std::move(entries)), scan_mode_(scan_mode) {}

        const std::optional<Snapshot>& GetSnapshot() const {
            return snapshot_;
        }

        std::optional<int64_t> SnapshotId() const {
            return snapshot_ == std::nullopt ? std::optional<int64_t>() : snapshot_.value().Id();
        }
        ScanMode GetScanMode() const {
            return scan_mode_;
        }

        // will move the manifest_entries_
        std::vector<ManifestEntry>&& Files() {
            return std::move(manifest_entries_);
        }

        /// Result `ManifestEntry` files with specific file kind.
        // will move the manifest_entries_
        std::vector<ManifestEntry> Files(const FileKind& kind);

        using GroupFiles =
            LinkedHashMap<BinaryRow, LinkedHashMap<int32_t, std::vector<ManifestEntry>>>;
        /// Return a map group by partition and bucket.
        static GroupFiles GroupByPartFiles(std::vector<ManifestEntry>&& files);

     private:
        std::optional<Snapshot> snapshot_;
        std::vector<ManifestEntry> manifest_entries_;
        ScanMode scan_mode_;
    };

    /// Produce a `Plan` of FileStoreScan.
    Result<std::shared_ptr<RawPlan>> CreatePlan() const;

    Result<std::vector<PartitionEntry>> ReadPartitionEntries() const;

 protected:
    /// @note Keep this thread-safe.
    virtual Result<bool> FilterByStats(const ManifestEntry& entry) const = 0;

    virtual std::vector<ManifestFileMeta> PostFilterManifests(
        std::vector<ManifestFileMeta>&& manifests) const {
        return std::move(manifests);
    }

    virtual Result<std::vector<ManifestEntry>> PostFilterManifestEntries(
        std::vector<ManifestEntry>&& entries) const {
        return std::move(entries);
    }

    virtual Result<std::vector<ManifestEntry>> FilterWholeBucketByStats(
        std::vector<ManifestEntry>&& entries) const {
        return std::move(entries);
    }

    virtual bool WholeBucketFilterEnabled() const {
        return false;
    }
    Status SplitAndSetFilter(const std::vector<std::string>& partition_keys,
                             const std::shared_ptr<arrow::Schema>& arrow_schema,
                             const std::shared_ptr<ScanFilter>& scan_filters);

 private:
    Status ReadManifests(std::optional<Snapshot>* snapshot_ptr,
                         std::vector<ManifestFileMeta>* manifests_ptr) const;

    Status ReadManifestsWithSnapshot(const Snapshot& snapshot,
                                     std::vector<ManifestFileMeta>* manifests) const;

    Status ReadManifestEntries(const std::vector<ManifestFileMeta>& manifest_metas,
                               std::vector<ManifestEntry>* manifest_entries) const;

    Status ReadAndMergeFileEntries(const std::vector<ManifestFileMeta>& manifest_metas,
                                   std::vector<ManifestEntry>* merged_entries) const;

    Status ReadAndNoMergeFileEntries(const std::vector<ManifestFileMeta>& manifest_metas,
                                     std::vector<ManifestEntry>* manifest_entries) const;

    Status ReadFileEntries(const std::vector<ManifestFileMeta>& manifest_metas,
                           std::vector<ManifestEntry>* manifest_entries) const;

    Result<bool> FilterManifestFileMeta(const ManifestFileMeta& manifest) const;

    Status ReadManifestFileMeta(const ManifestFileMeta& manifest,
                                std::vector<ManifestEntry>* entries) const;

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<SchemaManager> schema_manager_;
    std::shared_ptr<PredicateFilter> predicates_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<TableSchema> table_schema_;
    std::optional<std::vector<Range>> row_ranges_;

    ScanMode scan_mode_ = ScanMode::ALL;
    CoreOptions core_options_;

 private:
    mutable std::mutex lock_;
    bool only_read_real_buckets_ = false;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<ManifestList> manifest_list_;
    std::shared_ptr<ManifestFile> manifest_file_;
    std::shared_ptr<arrow::Schema> partition_schema_;
    std::shared_ptr<PredicateFilter> partition_filter_;
    std::shared_ptr<Executor> executor_;
    std::optional<int32_t> bucket_filter_;
    std::function<bool(int32_t)> level_filter_;
    std::optional<Snapshot> specified_snapshot_;
};
}  // namespace paimon
