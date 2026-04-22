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

#include "paimon/core/operation/key_value_file_store_scan.h"

#include <cstdint>
#include <exception>
#include <map>
#include <optional>
#include <set>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/core/bucket/bucket_select_converter.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/options/merge_engine.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_evolution.h"
#include "paimon/core/stats/simple_stats_evolutions.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_utils.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class Executor;
class ManifestFile;
class ManifestList;
class MemoryPool;
class ScanFilter;
class SnapshotManager;

Result<std::unique_ptr<KeyValueFileStoreScan>> KeyValueFileStoreScan::Create(
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<ManifestList>& manifest_list,
    const std::shared_ptr<ManifestFile>& manifest_file,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::shared_ptr<ScanFilter>& scan_filters, const CoreOptions& core_options,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    auto scan = std::unique_ptr<KeyValueFileStoreScan>(
        new KeyValueFileStoreScan(snapshot_manager, schema_manager, manifest_list, manifest_file,
                                  table_schema, arrow_schema, core_options, executor, pool));
    PAIMON_RETURN_NOT_OK(
        scan->SplitAndSetFilter(table_schema->PartitionKeys(), arrow_schema, scan_filters));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_pk, table_schema->TrimmedPrimaryKeys());
    PAIMON_RETURN_NOT_OK(scan->SplitAndSetKeyValueFilter(trimmed_pk));
    return scan;
}

Result<bool> KeyValueFileStoreScan::FilterByStats(const ManifestEntry& entry) const {
    PAIMON_ASSIGN_OR_RAISE(bool value_filter_enabled, IsValueFilterEnabled());
    if (value_filter_enabled) {
        PAIMON_ASSIGN_OR_RAISE(bool filtered, FilterByValueFilter(entry));
        if (!filtered) {
            return false;
        }
    }

    if (key_filter_ != nullptr) {
        const auto& stats = entry.File()->key_stats;
        return key_filter_->Test(schema_, entry.File()->row_count, stats.MinValues(),
                                 stats.MaxValues(), stats.NullCounts());
    }
    return true;
}

Result<std::vector<ManifestEntry>> KeyValueFileStoreScan::FilterWholeBucketByStats(
    std::vector<ManifestEntry>&& entries) const {
    return NoOverlapping(entries) ? FilterWholeBucketPerFile(std::move(entries))
                                  : FilterWholeBucketAllFiles(std::move(entries));
}

Status KeyValueFileStoreScan::SplitAndSetKeyValueFilter(
    const std::vector<std::string>& trimmed_pk) {
    if (predicates_ == nullptr) {
        return Status::OK();
    }
    // currently we can only perform filter push down on keys
    // consider this case:
    //   data file 1: insert key = a, value = 1
    //   data file 2: update key = a, value = 2
    //   filter: value = 1
    // if we perform filter push down on values, data file 1 will be chosen, but data
    // file 2 will be ignored, and the final result will be key = a, value = 1 while the
    // correct result is an empty set
    std::map<std::string, int32_t> trimmed_pk_to_idx =
        ObjectUtils::CreateIdentifierToIndexMap(trimmed_pk);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> key_predicate,
                           PredicateUtils::CreatePickedFieldFilter(predicates_, trimmed_pk_to_idx));
    if (key_predicate) {
        auto key_filter = std::dynamic_pointer_cast<PredicateFilter>(key_predicate);
        if (!key_filter) {
            return Status::Invalid("invalid key predicate, cannot cast to PredicateFilter");
        }
        WithKeyFilter(key_filter);

        // Bucket select conversion: derive target bucket from EQUAL predicates on bucket keys
        const auto& bucket_keys = table_schema_->BucketKeys();
        int32_t num_buckets = core_options_.GetBucket();
        if (num_buckets > 0 && !bucket_keys.empty()) {
            std::vector<std::shared_ptr<arrow::DataType>> bucket_key_arrow_types;
            bucket_key_arrow_types.reserve(bucket_keys.size());
            for (const auto& key : bucket_keys) {
                PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema_->GetField(key));
                bucket_key_arrow_types.push_back(field.Type());
            }
            PAIMON_ASSIGN_OR_RAISE(
                std::optional<int32_t> selected_bucket,
                BucketSelectConverter::Convert(key_predicate, bucket_keys, bucket_key_arrow_types,
                                               core_options_.GetBucketFunctionType(), num_buckets,
                                               pool_.get()));
            if (selected_bucket.has_value()) {
                SetBucketFilterIfAbsent(selected_bucket.value());
            }
        }
    }

    // Only set value filtering when there are predicates on non-primary-key fields.
    //
    // For primary key tables, we cannot safely push down arbitrary value predicates to each
    // individual file (see the comment above). When value filtering is enabled, we will filter
    // by stats either per file (when no overlapping) or by whole bucket (when overlapping).
    //
    // For key-only predicates, key_stats based filtering is enough and enabling value filtering
    // may trigger unsupported paths (e.g. DataFileMeta::value_stats_cols).
    std::set<std::string> field_names;
    PAIMON_RETURN_NOT_OK(PredicateUtils::GetAllNames(predicates_, &field_names));
    std::set<std::string> pk_names(trimmed_pk.begin(), trimmed_pk.end());
    bool has_non_pk_predicate = false;
    for (const auto& name : field_names) {
        if (pk_names.find(name) == pk_names.end()) {
            has_non_pk_predicate = true;
            break;
        }
    }
    if (has_non_pk_predicate) {
        // support value filter in bucket level
        WithValueFilter(predicates_);
    }
    return Status::OK();
}

Result<bool> KeyValueFileStoreScan::IsValueFilterEnabled() const {
    if (value_filter_ == nullptr) {
        return false;
    }
    switch (scan_mode_) {
        case ScanMode::ALL:
            return value_filter_force_enabled_;
        case ScanMode::DELTA:
            return false;
        default:
            return Status::NotImplemented("only support ALL and DELTA scan mode");
    }
}

Result<bool> KeyValueFileStoreScan::FilterByValueFilter(const ManifestEntry& entry) const {
    if (!value_filter_) {
        return true;
    }
    if (entry.File()->embedded_index != nullptr) {
        return Status::NotImplemented("do not support embedded index in DataFileMeta");
    }

    const auto& meta = entry.File();

    // Primary key table currently does not support schema evolution for value filtering.
    // Here we only handle `value_stats_cols` (dense stats) projection.
    if (meta->schema_id != table_schema_->Id()) {
        return Status::NotImplemented(
            "Primary key table does not support schema evolution in FilterByValueFilter");
    }

    auto evolution = evolutions_->GetOrCreate(table_schema_);

    PAIMON_ASSIGN_OR_RAISE(
        SimpleStatsEvolution::EvolutionStats new_stats,
        evolution->Evolution(meta->value_stats, meta->row_count, meta->value_stats_cols));

    try {
        PAIMON_ASSIGN_OR_RAISE(
            bool predicate_result,
            value_filter_->Test(schema_, meta->row_count, *(new_stats.min_values),
                                *(new_stats.max_values), *(new_stats.null_counts)));
        return predicate_result;
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("FilterByValueFilter failed for file {}, with {} error",
                                           meta->file_name, e.what()));
    } catch (...) {
        return Status::Invalid(fmt::format(
            "FilterByValueFilter failed for file {}, with unknown error", meta->file_name));
    }
}

bool KeyValueFileStoreScan::NoOverlapping(const std::vector<ManifestEntry>& entries) {
    if (entries.size() <= 1) {
        return true;
    }
    std::set<int32_t> level_set;
    for (const auto& entry : entries) {
        level_set.insert(entry.File()->level);
    }
    if (level_set.find(0) != level_set.end()) {
        // level 0 files have overlapping
        return false;
    }
    // have different level, have overlapping
    return level_set.size() == 1;
}

Result<std::vector<ManifestEntry>> KeyValueFileStoreScan::FilterWholeBucketPerFile(
    std::vector<ManifestEntry>&& entries) const {
    std::vector<ManifestEntry> filtered_entries;
    filtered_entries.reserve(entries.size());
    for (auto& entry : entries) {
        PAIMON_ASSIGN_OR_RAISE(bool filtered, FilterByValueFilter(entry));
        if (filtered) {
            filtered_entries.emplace_back(std::move(entry));
        }
    }
    return filtered_entries;
}

Result<std::vector<ManifestEntry>> KeyValueFileStoreScan::FilterWholeBucketAllFiles(
    std::vector<ManifestEntry>&& entries) const {
    if (!core_options_.DeletionVectorsEnabled() &&
        (core_options_.GetMergeEngine() == MergeEngine::AGGREGATE ||
         core_options_.GetMergeEngine() == MergeEngine::PARTIAL_UPDATE)) {
        return entries;
    }
    // entries come from the same bucket, if any of it doesn't meet the request, we could
    // filter the bucket.
    for (auto& entry : entries) {
        PAIMON_ASSIGN_OR_RAISE(bool filtered, FilterByValueFilter(entry));
        if (filtered) {
            return std::move(entries);
        }
    }
    return std::vector<ManifestEntry>();
}

KeyValueFileStoreScan::KeyValueFileStoreScan(
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

}  // namespace paimon
