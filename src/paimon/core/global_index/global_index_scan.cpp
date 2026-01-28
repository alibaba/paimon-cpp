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

#include "paimon/global_index/global_index_scan.h"

#include "paimon/core/core_options.h"
#include "paimon/core/global_index/global_index_scan_impl.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/utils/snapshot_manager.h"
namespace paimon {
namespace {
Result<std::shared_ptr<TableSchema>> LoadSchema(const std::string& root_path,
                                                const std::map<std::string, std::string>& options,
                                                const std::shared_ptr<FileSystem>& file_system) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options, CoreOptions::FromMap(options, file_system));
    SchemaManager schema_manager(tmp_options.GetFileSystem(), root_path);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_table_schema,
                           schema_manager.Latest());
    if (latest_table_schema == std::nullopt) {
        return Status::Invalid("not found latest schema");
    }
    return latest_table_schema.value();
}

Result<CoreOptions> MergeOptions(const std::shared_ptr<TableSchema>& table_schema,
                                 const std::map<std::string, std::string>& options,
                                 const std::shared_ptr<FileSystem>& file_system) {
    auto final_options = table_schema->Options();
    for (const auto& [key, value] : options) {
        final_options[key] = value;
    }
    return CoreOptions::FromMap(final_options, file_system);
}

Result<Snapshot> LoadSnapshot(const std::string& root_path,
                              const std::optional<int64_t>& snapshot_id,
                              const CoreOptions& core_options) {
    std::optional<Snapshot> snapshot;
    SnapshotManager snapshot_manager(core_options.GetFileSystem(), root_path);
    if (snapshot_id) {
        PAIMON_ASSIGN_OR_RAISE(snapshot, snapshot_manager.LoadSnapshot(snapshot_id.value()));
    } else {
        PAIMON_ASSIGN_OR_RAISE(snapshot, snapshot_manager.LatestSnapshot());
    }
    if (!snapshot) {
        return Status::Invalid("not found latest snapshot");
    }
    return snapshot.value();
}
}  // namespace

Result<std::unique_ptr<GlobalIndexScan>> GlobalIndexScan::Create(
    const std::string& root_path, const std::optional<int64_t>& snapshot_id,
    const std::optional<std::vector<std::map<std::string, std::string>>>& partitions,
    const std::map<std::string, std::string>& options,
    const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (partitions && partitions.value().empty()) {
        return Status::Invalid(
            "invalid input partition, supposed to be null or at least one partition");
    }
    std::shared_ptr<MemoryPool> pool = memory_pool ? memory_pool : GetDefaultPool();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> table_schema,
                           LoadSchema(root_path, options, file_system));
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           MergeOptions(table_schema, options, file_system));
    std::shared_ptr<PredicateFilter> partition_filters;
    if (partitions) {
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
        PAIMON_ASSIGN_OR_RAISE(partition_filters, FileStoreScan::CreatePartitionPredicate(
                                                      table_schema->PartitionKeys(),
                                                      core_options.GetPartitionDefaultName(),
                                                      arrow_schema, partitions.value()));
    }
    PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, LoadSnapshot(root_path, snapshot_id, core_options));
    return std::make_unique<GlobalIndexScanImpl>(root_path, table_schema, snapshot,
                                                 partition_filters, core_options, pool);
}

Result<std::unique_ptr<GlobalIndexScan>> GlobalIndexScan::Create(
    const std::string& root_path, const std::optional<int64_t>& snapshot_id,
    const std::shared_ptr<Predicate>& partitions, const std::map<std::string, std::string>& options,
    const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    std::shared_ptr<PredicateFilter> partition_filters;
    if (partitions) {
        partition_filters = std::dynamic_pointer_cast<PredicateFilter>(partitions);
        if (!partition_filters) {
            return Status::Invalid("partition filters cannot cast to PredicateFilter");
        }
    }
    std::shared_ptr<MemoryPool> pool = memory_pool ? memory_pool : GetDefaultPool();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> table_schema,
                           LoadSchema(root_path, options, file_system));
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           MergeOptions(table_schema, options, file_system));
    PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, LoadSnapshot(root_path, snapshot_id, core_options));
    return std::make_unique<GlobalIndexScanImpl>(root_path, table_schema, snapshot,
                                                 partition_filters, core_options, pool);
}

}  // namespace paimon
