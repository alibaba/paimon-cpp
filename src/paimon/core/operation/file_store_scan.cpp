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

#include "paimon/core/operation/file_store_scan.h"

#include <algorithm>
#include <cstddef>
#include <future>
#include <list>
#include <numeric>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/cache/cache.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_entry.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/manifest/snapshot_live_manifest_entries.h"
#include "paimon/core/operation/metrics/scan_metrics.h"
#include "paimon/core/partition/partition_info.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_evolution.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/duration.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_segment.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/scan_context.h"

namespace paimon {
enum class FieldType;

Result<std::shared_ptr<Predicate>> FileStoreScan::ReconstructPredicateWithNonCastedFields(
    const std::shared_ptr<Predicate>& predicate,
    const std::shared_ptr<SimpleStatsEvolution>& evolution) {
    const auto& id_to_data_fields = evolution->GetFieldIdToDataField();
    const auto& name_to_table_fields = evolution->GetFieldNameToTableField();

    std::set<std::string> field_names_in_predicate;
    PAIMON_RETURN_NOT_OK(PredicateUtils::GetAllNames(predicate, &field_names_in_predicate));
    std::set<std::string> excluded_field_names;
    for (const auto& field_name : field_names_in_predicate) {
        auto table_iter = name_to_table_fields.find(field_name);
        if (table_iter == name_to_table_fields.end()) {
            return Status::Invalid(
                fmt::format("field {} in predicate is not included in table schema", field_name));
        }
        auto data_iter = id_to_data_fields.find(table_iter->second.Id());
        if (data_iter == id_to_data_fields.end()) {
            continue;
        }
        // Exclude fields requiring casting to avoid false negatives in stats filtering.
        if (!data_iter->second.second.Type()->Equals(table_iter->second.Type())) {
            excluded_field_names.insert(field_name);
        }
    }
    return PredicateUtils::ExcludePredicateWithFields(predicate, excluded_field_names);
}

std::vector<ManifestEntry> FileStoreScan::RawPlan::Files(const FileKind& kind) {
    std::vector<ManifestEntry> entries = Files();
    std::vector<ManifestEntry> filtered_entries;
    filtered_entries.reserve(entries.size());
    for (auto& entry : entries) {
        if (entry.Kind() == kind) {
            filtered_entries.emplace_back(std::move(entry));
        }
    }
    return filtered_entries;
}

FileStoreScan::RawPlan::GroupFiles FileStoreScan::RawPlan::GroupByPartFiles(
    std::vector<ManifestEntry>&& files) {
    LinkedHashMap<BinaryRow, LinkedHashMap<int32_t, std::vector<ManifestEntry>>> group_by;
    for (auto& entry : files) {
        auto& bucket_map = group_by[entry.Partition()];
        auto& file_list = bucket_map[entry.Bucket()];
        file_list.emplace_back(std::move(entry));
    }
    return group_by;
}

Result<std::vector<PartitionEntry>> FileStoreScan::ReadPartitionEntries() const {
    std::optional<Snapshot> snapshot;
    std::vector<ManifestFileMeta> all_manifest_file_metas;
    std::vector<ManifestFileMeta> filtered_manifest_file_metas;
    PAIMON_RETURN_NOT_OK(
        ReadManifests(&snapshot, &all_manifest_file_metas, &filtered_manifest_file_metas));
    std::vector<ManifestEntry> manifest_entries;
    PAIMON_RETURN_NOT_OK(ReadFileEntries(filtered_manifest_file_metas, &manifest_entries,
                                         /*apply_scan_filter=*/true));
    std::unordered_map<BinaryRow, PartitionEntry> partitions;
    PAIMON_RETURN_NOT_OK(PartitionEntry::Merge(manifest_entries, &partitions));

    std::vector<PartitionEntry> partition_entries;
    partition_entries.reserve(partitions.size());
    for (const auto& [_, partition_entry] : partitions) {
        if (partition_entry.FileCount() > 0) {
            partition_entries.push_back(partition_entry);
        }
    }
    return partition_entries;
}

Result<std::shared_ptr<FileStoreScan::RawPlan>> FileStoreScan::CreatePlan() const {
    Duration duration;
    std::optional<Snapshot> snapshot;
    std::vector<ManifestFileMeta> all_manifest_file_metas;
    std::vector<ManifestFileMeta> filtered_manifest_file_metas;
    PAIMON_RETURN_NOT_OK(
        ReadManifests(&snapshot, &all_manifest_file_metas, &filtered_manifest_file_metas));

    std::vector<ManifestEntry> manifest_entries;
    const bool use_snapshot_live_manifest_cache =
        snapshot.has_value() && scan_mode_ == ScanMode::ALL &&
        core_options_.GetScanManifestEntryCacheMaxSnapshots() > 0 &&
        core_options_.GetCache() != nullptr && !table_path_.empty() &&
        !row_range_index_.has_value() && bucket_filter_.has_value();
    if (use_snapshot_live_manifest_cache) {
        PAIMON_RETURN_NOT_OK(ReadManifestEntriesWithCache(
            snapshot.value(), all_manifest_file_metas, bucket_filter_.value(), &manifest_entries));
        std::vector<ManifestEntry> filtered_entries;
        filtered_entries.reserve(manifest_entries.size());
        for (auto& entry : manifest_entries) {
            PAIMON_ASSIGN_OR_RAISE(bool keep, FilterManifestEntry(entry));
            if (keep) {
                filtered_entries.emplace_back(std::move(entry));
            }
        }
        manifest_entries = std::move(filtered_entries);
    } else {
        PAIMON_RETURN_NOT_OK(ReadManifestEntries(filtered_manifest_file_metas, &manifest_entries));
    }
    PAIMON_ASSIGN_OR_RAISE(manifest_entries,
                           PostFilterManifestEntries(std::move(manifest_entries)));

    if (WholeBucketFilterEnabled()) {
        // We group files by bucket here, and filter them by the whole bucket filter.
        // Why do this: because in primary key table, we can't just filter the value
        // by the stat in files (see `PrimaryKeyFileStoreTable.nonPartitionFilterConsumer`),
        // but we can do this by filter the whole bucket files
        // we use LinkedHashMap to avoid disorder
        LinkedHashMap<std::pair<BinaryRow, int32_t>, std::vector<ManifestEntry>> grouped_entries;
        for (auto& entry : manifest_entries) {
            auto key = std::make_pair(entry.Partition(), entry.Bucket());
            grouped_entries[key].push_back(std::move(entry));
        }
        manifest_entries.clear();
        for (const auto& [_, entries] : grouped_entries) {
            auto tmp_entries = entries;
            PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> filtered_bucket_entries,
                                   FilterWholeBucketByStats(std::move(tmp_entries)));
            for (auto& entry : filtered_bucket_entries) {
                manifest_entries.emplace_back(std::move(entry));
            }
        }
    }
    const int64_t all_data_files = std::accumulate(
        all_manifest_file_metas.begin(), all_manifest_file_metas.end(), int64_t{0},
        [](const int64_t sum, const ManifestFileMeta& manifest_file_meta) {
            return sum + manifest_file_meta.NumAddedFiles() - manifest_file_meta.NumDeletedFiles();
        });
    const uint64_t scan_duration_ms = duration.Get();
    metrics_->SetCounter(ScanMetrics::LAST_SCAN_DURATION, scan_duration_ms);
    metrics_->ObserveHistogram(ScanMetrics::SCAN_DURATION, static_cast<double>(scan_duration_ms));
    metrics_->SetCounter(ScanMetrics::LAST_SCANNED_SNAPSHOT_ID,
                         snapshot.has_value() ? snapshot.value().Id() : int64_t{0});
    metrics_->SetCounter(ScanMetrics::LAST_SCANNED_MANIFESTS,
                         static_cast<int64_t>(filtered_manifest_file_metas.size()));
    metrics_->SetCounter(
        ScanMetrics::LAST_SCAN_SKIPPED_TABLE_FILES,
        std::max(int64_t{0}, all_data_files - static_cast<int64_t>(manifest_entries.size())));
    metrics_->SetCounter(ScanMetrics::LAST_SCAN_RESULTED_TABLE_FILES, manifest_entries.size());
    return std::make_shared<FileStoreScan::RawPlan>(scan_mode_, snapshot,
                                                    std::move(manifest_entries));
}

Status FileStoreScan::ReadManifests(std::optional<Snapshot>* snapshot_ptr,
                                    std::vector<ManifestFileMeta>* all_manifests_ptr,
                                    std::vector<ManifestFileMeta>* filter_manifests_ptr) const {
    auto& snapshot = *snapshot_ptr;
    auto& all_manifests = *all_manifests_ptr;
    auto& filtered_manifests = *filter_manifests_ptr;
    if (specified_snapshot_ != std::nullopt) {
        snapshot = specified_snapshot_;
    } else {
        PAIMON_ASSIGN_OR_RAISE(snapshot, snapshot_manager_->LatestSnapshot());
    }
    if (snapshot == std::nullopt) {
        all_manifests = std::vector<ManifestFileMeta>();
        filtered_manifests = std::vector<ManifestFileMeta>();
        return Status::OK();
    }
    PAIMON_RETURN_NOT_OK(ReadManifestsWithSnapshot(snapshot.value(), &all_manifests));
    for (const auto& meta : all_manifests) {
        PAIMON_ASSIGN_OR_RAISE(bool filter_meta_result, FilterManifestFileMeta(meta));
        if (filter_meta_result) {
            filtered_manifests.push_back(meta);
        }
    }
    return Status::OK();
}

Status FileStoreScan::ReadManifestsWithSnapshot(const Snapshot& snapshot,
                                                std::vector<ManifestFileMeta>* manifests) const {
    switch (scan_mode_) {
        case ScanMode::ALL:
            return manifest_list_->ReadDataManifests(snapshot, manifests);
        case ScanMode::DELTA:
            return manifest_list_->ReadDeltaManifests(snapshot, manifests);
        default:
            return Status::NotImplemented("Unknown scan mode ",
                                          std::to_string(static_cast<int32_t>(scan_mode_)));
    }
}

Status FileStoreScan::ReadFileEntries(const std::vector<ManifestFileMeta>& manifest_metas,
                                      std::vector<ManifestEntry>* manifest_entries,
                                      bool apply_scan_filter) const {
    std::vector<std::future<Result<std::vector<ManifestEntry>>>> futures;
    for (const auto& meta : manifest_metas) {
        auto read_meta_task = [this, meta,
                               apply_scan_filter]() -> Result<std::vector<ManifestEntry>> {
            std::vector<ManifestEntry> tmp_entries;
            if (apply_scan_filter) {
                PAIMON_RETURN_NOT_OK(ReadManifestFileMeta(meta, &tmp_entries));
            } else {
                PAIMON_RETURN_NOT_OK(
                    manifest_file_->Read(meta.FileName(), /*filter=*/nullptr, &tmp_entries));
            }
            return tmp_entries;
        };
        futures.push_back(Via(executor_.get(), read_meta_task));
    }

    // sequential execute
    auto unfiltered_entries = CollectAll(futures);
    for (auto& entry_list : unfiltered_entries) {
        if (!entry_list.ok()) {
            return entry_list.status();
        }
        manifest_entries->reserve(manifest_entries->size() + entry_list.value().size());
        for (auto& entry : entry_list.value()) {
            manifest_entries->emplace_back(std::move(entry));
        }
    }
    return Status::OK();
}

Status FileStoreScan::ReadManifestEntries(const std::vector<ManifestFileMeta>& manifest_metas,
                                          std::vector<ManifestEntry>* manifest_entries) const {
    if (scan_mode_ == ScanMode::ALL) {
        return ReadAndMergeFileEntries(manifest_metas, manifest_entries);
    }
    return ReadAndNoMergeFileEntries(manifest_metas, manifest_entries);
}

// Cache merged live manifest entries for one bucket before applying scan filters. Each cache value
// keeps a bounded number of snapshot results for the same table/branch/bucket. Exact snapshot hits
// can be returned directly; cache misses rebuild the target snapshot bucket from the target
// snapshot's data manifests.
Status FileStoreScan::ReadManifestEntriesWithCache(
    const Snapshot& snapshot, const std::vector<ManifestFileMeta>& all_manifest_metas,
    int32_t bucket, std::vector<ManifestEntry>* manifest_entries) const {
    PAIMON_ASSIGN_OR_RAISE(SnapshotLiveManifestEntries cached_entries,
                           LoadSnapshotLiveManifestEntries(bucket));
    std::optional<SnapshotLiveManifestEntries::Entry> cached =
        cached_entries.LatestBeforeOrEqual(snapshot.Id());
    if (cached && cached->snapshot_id == snapshot.Id()) {
        *manifest_entries = *cached->entries;
        return Status::OK();
    }

    // Rebuild the target snapshot bucket from all manifests and write the live entries back to the
    // cache.
    std::vector<ManifestFileMeta> bucket_manifest_metas;
    for (const auto& meta : all_manifest_metas) {
        if (MayContainBucket(meta, bucket)) {
            bucket_manifest_metas.push_back(meta);
        }
    }
    PAIMON_RETURN_NOT_OK(
        ReadAndMergeBucketFileEntries(bucket_manifest_metas, bucket, manifest_entries));
    std::vector<ManifestEntry> cache_entries = *manifest_entries;
    cached_entries.Put(snapshot.Id(), std::move(cache_entries));
    PAIMON_RETURN_NOT_OK(StoreSnapshotLiveManifestEntries(bucket, cached_entries));
    return Status::OK();
}

std::shared_ptr<CacheKey> FileStoreScan::SnapshotLiveManifestEntriesCacheKey(int32_t bucket) const {
    return CacheKey::ForSnapshotLiveManifestEntries(
        table_path_, BranchManager::NormalizeBranch(core_options_.GetBranch()), bucket);
}

Result<SnapshotLiveManifestEntries> FileStoreScan::LoadSnapshotLiveManifestEntries(
    int32_t bucket) const {
    auto supplier = [](const std::shared_ptr<CacheKey>&) -> Result<std::shared_ptr<CacheValue>> {
        return std::shared_ptr<CacheValue>();
    };
    std::shared_ptr<CacheKey> cache_key = SnapshotLiveManifestEntriesCacheKey(bucket);
    const auto max_snapshots = core_options_.GetScanManifestEntryCacheMaxSnapshots();
    Result<std::shared_ptr<CacheValue>> cache_result =
        core_options_.GetCache()->Get(cache_key, supplier);
    if (!cache_result.ok() || !cache_result.value()) {
        return SnapshotLiveManifestEntries(max_snapshots);
    }
    Result<SnapshotLiveManifestEntries> deserialized = SnapshotLiveManifestEntries::Deserialize(
        cache_result.value()->GetSegment(), max_snapshots, pool_);
    if (!deserialized.ok()) {
        return SnapshotLiveManifestEntries(max_snapshots);
    }
    return std::move(deserialized.value());
}

Status FileStoreScan::StoreSnapshotLiveManifestEntries(
    int32_t bucket, const SnapshotLiveManifestEntries& entries) const {
    Result<std::shared_ptr<Bytes>> bytes_result = entries.Serialize(pool_);
    if (!bytes_result.ok()) {
        return Status::OK();
    }
    auto cache_value =
        std::make_shared<CacheValue>(MemorySegment::Wrap(bytes_result.value()), CacheCallback());
    Status status =
        core_options_.GetCache()->Put(SnapshotLiveManifestEntriesCacheKey(bucket), cache_value);
    return status.ok() ? status : Status::OK();
}

Status FileStoreScan::ReadAndMergeBucketFileEntries(
    const std::vector<ManifestFileMeta>& manifest_metas, int32_t bucket,
    std::vector<ManifestEntry>* merged_entries) const {
    std::vector<ManifestEntry> unmerged_entries;
    std::vector<ManifestEntry> entries;
    PAIMON_RETURN_NOT_OK(ReadFileEntries(manifest_metas, &entries, /*apply_scan_filter=*/false));
    unmerged_entries.reserve(entries.size());
    for (auto& entry : entries) {
        if (entry.Bucket() == bucket) {
            unmerged_entries.emplace_back(std::move(entry));
        }
    }
    return MergeLiveEntries(unmerged_entries, merged_entries);
}

Status FileStoreScan::MergeLiveEntries(const std::vector<ManifestEntry>& unmerged_entries,
                                       std::vector<ManifestEntry>* live_entries) {
    std::unordered_set<FileEntry::Identifier> deleted_entries;
    for (const auto& entry : unmerged_entries) {
        if (entry.Kind() == FileKind::Delete()) {
            deleted_entries.insert(entry.CreateIdentifier());
        }
    }
    for (const auto& entry : unmerged_entries) {
        if (entry.Kind() == FileKind::Add() &&
            deleted_entries.find(entry.CreateIdentifier()) == deleted_entries.end()) {
            live_entries->push_back(entry);
        }
    }
    return Status::OK();
}

Status FileStoreScan::ReadAndMergeFileEntries(const std::vector<ManifestFileMeta>& manifest_metas,
                                              std::vector<ManifestEntry>* merged_entries) const {
    std::vector<ManifestEntry> unmerged_entries;
    PAIMON_RETURN_NOT_OK(
        ReadFileEntries(manifest_metas, &unmerged_entries, /*apply_scan_filter=*/true));
    return MergeLiveEntries(unmerged_entries, merged_entries);
}

Status FileStoreScan::ReadAndNoMergeFileEntries(
    const std::vector<ManifestFileMeta>& manifest_metas,
    std::vector<ManifestEntry>* manifest_entries) const {
    return ReadFileEntries(manifest_metas, manifest_entries, /*apply_scan_filter=*/true);
}

bool FileStoreScan::MayContainBucket(const ManifestFileMeta& manifest, int32_t bucket) const {
    const std::optional<int32_t>& min_bucket = manifest.MinBucket();
    const std::optional<int32_t>& max_bucket = manifest.MaxBucket();
    if (min_bucket && max_bucket) {
        return bucket >= min_bucket.value() && bucket <= max_bucket.value();
    }
    return true;
}

Result<bool> FileStoreScan::FilterManifestFileMeta(const ManifestFileMeta& manifest) const {
    // filter by min max bucket
    std::optional<int32_t> min_bucket = manifest.MinBucket();
    std::optional<int32_t> max_bucket = manifest.MaxBucket();
    if (min_bucket && max_bucket) {
        if (only_read_real_buckets_ && max_bucket.value() < 0) {
            return false;
        }
        if (bucket_filter_ && (bucket_filter_.value() < min_bucket.value() ||
                               bucket_filter_.value() > max_bucket.value())) {
            return false;
        }
    }
    // filter by partition filter

    if (partition_filter_) {
        SimpleStats stats = manifest.PartitionStats();
        PAIMON_ASSIGN_OR_RAISE(
            bool saved, partition_filter_->Test(
                            partition_schema_,
                            /*row_count=*/manifest.NumAddedFiles() + manifest.NumDeletedFiles(),
                            stats.MinValues(), stats.MaxValues(), stats.NullCounts()));
        if (!saved) {
            return false;
        }
    }
    return FilterManifestByRowRanges(manifest);
}

bool FileStoreScan::FilterManifestByRowRanges(const ManifestFileMeta& manifest) const {
    if (!row_range_index_) {
        return true;
    }
    std::optional<int64_t> min = manifest.MinRowId();
    std::optional<int64_t> max = manifest.MaxRowId();
    if (!min || !max) {
        return true;
    }
    return row_range_index_->Intersects(min.value(), max.value());
}

Status FileStoreScan::ReadManifestFileMeta(const ManifestFileMeta& manifest,
                                           std::vector<ManifestEntry>* entries) const {
    std::vector<ManifestEntry> unfiltered_entries;
    PAIMON_RETURN_NOT_OK(manifest_file_->Read(
        manifest.FileName(),
        [this](const ManifestEntry& entry) -> Result<bool> { return FilterManifestEntry(entry); },
        &unfiltered_entries));
    entries->reserve(entries->size() + unfiltered_entries.size());
    for (auto& entry : unfiltered_entries) {
        entries->emplace_back(std::move(entry));
    }
    return Status::OK();
}

Result<bool> FileStoreScan::FilterManifestEntry(const ManifestEntry& entry) const {
    if (partition_filter_) {
        PAIMON_ASSIGN_OR_RAISE(bool res,
                               partition_filter_->Test(partition_schema_, entry.Partition()));
        if (!res) {
            return false;
        }
    }
    if (only_read_real_buckets_ && entry.Bucket() < 0) {
        return false;
    }
    if (bucket_filter_ != std::nullopt && entry.Bucket() != bucket_filter_.value()) {
        return false;
    }
    if (level_filter_ != nullptr && !level_filter_(entry.Level())) {
        return false;
    }
    return FilterByStats(entry);
}

Status FileStoreScan::SplitAndSetFilter(const std::vector<std::string>& partition_keys,
                                        const std::shared_ptr<arrow::Schema>& arrow_schema,
                                        const std::shared_ptr<ScanFilter>& scan_filters) {
    if (scan_filters->GetPredicate()) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldMappingBuilder> mapping_builder,
                               FieldMappingBuilder::Create(arrow_schema, partition_keys,
                                                           scan_filters->GetPredicate()));
        PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> data_fields,
                               DataField::ConvertArrowSchemaToDataFields(arrow_schema));
        auto converted_fields = BlobUtils::ConvertBlobInlineDataFields(
            data_fields, core_options_.GetBlobInlineFields());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldMapping> mapping,
                               mapping_builder->CreateFieldMapping(converted_fields));
        if (mapping->partition_info != std::nullopt) {
            const auto& partition_info = mapping->partition_info.value();
            partition_schema_ =
                DataField::ConvertDataFieldsToArrowSchema(partition_info.partition_read_schema);
            if (partition_info.partition_filter) {
                auto predicate_filter =
                    std::dynamic_pointer_cast<PredicateFilter>(partition_info.partition_filter);
                if (!predicate_filter) {
                    return Status::Invalid(
                        "invalid partition predicate, cannot cast to PredicateFilter");
                }
                partition_filter_ = predicate_filter;
            }
        }
        const auto& non_partition_info = mapping->non_partition_info;
        if (non_partition_info.non_partition_filter) {
            auto predicate =
                std::dynamic_pointer_cast<PredicateFilter>(non_partition_info.non_partition_filter);
            if (!predicate) {
                return Status::Invalid(
                    "invalid non partition predicate, cannot cast to PredicateFilter");
            }
            predicates_ = predicate;
        }
    }
    bucket_filter_ = scan_filters->GetBucketFilter();
    if (!scan_filters->GetPartitionFilters().empty()) {
        PAIMON_ASSIGN_OR_RAISE(
            partition_filter_,
            CreatePartitionPredicate(partition_keys, core_options_.GetPartitionDefaultName(),
                                     arrow_schema, scan_filters->GetPartitionFilters()));
    }
    return Status::OK();
}

Result<std::shared_ptr<PredicateFilter>> FileStoreScan::CreatePartitionPredicate(
    const std::vector<std::string>& partition_keys, const std::string& partition_default_name,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::vector<std::map<std::string, std::string>>& partition_filters) {
    if (partition_filters.empty()) {
        return std::shared_ptr<PredicateFilter>();
    }
    std::map<std::string, std::pair<int32_t, arrow::Type::type>> partition_keys_to_id_and_type;
    for (size_t i = 0; i < partition_keys.size(); i++) {
        const auto& partition_key = partition_keys[i];
        auto field = arrow_schema->GetFieldByName(partition_key);
        if (field == nullptr) {
            return Status::Invalid(fmt::format("field {} does not exist in schema", partition_key));
        }
        partition_keys_to_id_and_type[partition_key] = {i, field->type()->id()};
    }
    std::vector<std::shared_ptr<Predicate>> or_predicates;
    or_predicates.reserve(partition_filters.size());
    for (const auto& partition_filter : partition_filters) {
        std::vector<std::shared_ptr<Predicate>> and_predicates;
        and_predicates.reserve(partition_filter.size());
        if (partition_filter.empty()) {
            // partition_filter.empty() indicates all partition are included
            // or_predicates have one all_true predicate, just return all true predicate (nullptr)
            return std::shared_ptr<PredicateFilter>();
        }
        for (const auto& [key, value] : partition_filter) {
            auto iter = partition_keys_to_id_and_type.find(key);
            if (iter == partition_keys_to_id_and_type.end()) {
                return Status::Invalid(
                    fmt::format("field {} does not exist in partition keys", key));
            }
            PAIMON_ASSIGN_OR_RAISE(FieldType field_type,
                                   FieldTypeUtils::ConvertToFieldType(iter->second.second));
            // for null partition
            if (value == partition_default_name) {
                auto predicate = PredicateBuilder::IsNull(iter->second.first, key, field_type);
                and_predicates.push_back(predicate);
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(Literal literal,
                                   LiteralConverter::ConvertLiteralsFromString(field_type, value));
            auto predicate = PredicateBuilder::Equal(iter->second.first, key, field_type, literal);
            and_predicates.push_back(predicate);
        }
        assert(!and_predicates.empty());
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> and_predicate,
                               PredicateBuilder::And(and_predicates));
        or_predicates.push_back(and_predicate);
    }
    assert(!or_predicates.empty());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> or_predicate,
                           PredicateBuilder::Or(or_predicates));
    auto predicate_filter = std::dynamic_pointer_cast<PredicateFilter>(or_predicate);
    if (!predicate_filter) {
        return Status::Invalid("invalid partition predicate, cannot cast to predicate filter");
    }
    return predicate_filter;
}

}  // namespace paimon
