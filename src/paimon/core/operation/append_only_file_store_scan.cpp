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

#include "paimon/core/operation/append_only_file_store_scan.h"

#include <cassert>
#include <cstdint>
#include <exception>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>

#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/predicate/predicate_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/file_index_evaluator.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats_evolution.h"
#include "paimon/core/stats/simple_stats_evolutions.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/status.h"

namespace paimon {
class Executor;
class ManifestFile;
class ManifestList;
class MemoryPool;
class ScanFilter;
class SnapshotManager;

Result<std::unique_ptr<AppendOnlyFileStoreScan>> AppendOnlyFileStoreScan::Create(
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<ManifestList>& manifest_list,
    const std::shared_ptr<ManifestFile>& manifest_file,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::shared_ptr<ScanFilter>& scan_filters, const CoreOptions& core_options,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    auto scan = std::unique_ptr<AppendOnlyFileStoreScan>(
        new AppendOnlyFileStoreScan(snapshot_manager, schema_manager, manifest_list, manifest_file,
                                    table_schema, arrow_schema, core_options, executor, pool));
    PAIMON_RETURN_NOT_OK(
        scan->SplitAndSetFilter(table_schema->PartitionKeys(), arrow_schema, scan_filters));
    return scan;
}

AppendOnlyFileStoreScan::AppendOnlyFileStoreScan(
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<ManifestList>& manifest_list,
    const std::shared_ptr<ManifestFile>& manifest_file,
    const std::shared_ptr<TableSchema>& table_schema, const std::shared_ptr<arrow::Schema>& schema,
    const CoreOptions& core_options, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool)
    : FileStoreScan(snapshot_manager, schema_manager, manifest_list, manifest_file, table_schema,
                    schema, core_options, executor, pool) {
    evolutions_ = std::make_shared<SimpleStatsEvolutions>(table_schema, pool);
}

Result<bool> AppendOnlyFileStoreScan::FilterByStats(const ManifestEntry& entry) const {
    if (!predicates_) {
        return true;
    }
    const auto& meta = entry.File();
    std::shared_ptr<TableSchema> data_schema = table_schema_;
    std::shared_ptr<Predicate> trimmed_predicates = predicates_;
    int64_t data_schema_id = meta->schema_id;
    if (data_schema_id != table_schema_->Id()) {
        PAIMON_ASSIGN_OR_RAISE(data_schema, schema_manager_->ReadSchema(data_schema_id));
    }
    auto evolution = evolutions_->GetOrCreate(data_schema);
    if (data_schema_id != table_schema_->Id()) {
        // remove fields with casting in predicate
        PAIMON_ASSIGN_OR_RAISE(trimmed_predicates,
                               ReconstructPredicateWithNonCastedFields(predicates_, evolution));
    }
    if (!trimmed_predicates) {
        return true;
    }
    // evolution stats, from data schema to table schema, also deal with dense fields
    PAIMON_ASSIGN_OR_RAISE(
        SimpleStatsEvolution::EvolutionStats new_stats,
        evolution->Evolution(meta->value_stats, meta->row_count, meta->value_stats_cols));

    // predicate tests evolution stats
    auto predicate_filter = std::dynamic_pointer_cast<PredicateFilter>(trimmed_predicates);
    if (!predicate_filter) {
        return Status::Invalid("cannot cast to predicate filter");
    }
    try {
        PAIMON_ASSIGN_OR_RAISE(
            bool predicate_result,
            predicate_filter->Test(schema_, meta->row_count, *(new_stats.min_values),
                                   *(new_stats.max_values), *(new_stats.null_counts)));
        if (!predicate_result) {
            return false;
        }
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("FilterByStats failed for file {}, with {} error",
                                           meta->file_name, e.what()));
    } catch (...) {
        return Status::Invalid(
            fmt::format("FilterByStats failed for file {}, with unknown error", meta->file_name));
    }

    if (!core_options_.FileIndexReadEnabled()) {
        return true;
    }

    return TestFileIndex(meta, evolution, data_schema);
}

Result<bool> AppendOnlyFileStoreScan::TestFileIndex(
    const std::shared_ptr<DataFileMeta>& meta,
    const std::shared_ptr<SimpleStatsEvolution>& evolution,
    const std::shared_ptr<TableSchema>& data_schema) const {
    std::shared_ptr<Predicate> data_predicate = predicates_;
    if (data_schema->Id() != table_schema_->Id()) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<Predicate>> reconstruct_predicate,
                               FieldMappingBuilder::ReconstructPredicateWithDataFields(
                                   predicates_, evolution->GetFieldNameToTableField(),
                                   evolution->GetFieldIdToDataField()));

        if (reconstruct_predicate == std::nullopt) {
            return true;
        }
        data_predicate = reconstruct_predicate.value();
    }
    assert(data_predicate);
    auto data_arrow_schema = DataField::ConvertDataFieldsToArrowSchema(data_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileIndexResult> index_result,
        FileIndexEvaluator::Evaluate(data_arrow_schema, data_predicate, meta, pool_));
    return index_result->IsRemain();
}

}  // namespace paimon
