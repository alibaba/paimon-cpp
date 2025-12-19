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

#include <cstddef>
#include <future>
#include <list>
#include <unordered_map>
#include <unordered_set>

#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_array.h"
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
#include "paimon/core/partition/partition_info.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/scan_context.h"

namespace paimon {
enum class FieldType;

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
    std::vector<ManifestFileMeta> manifest_file_metas;
    PAIMON_RETURN_NOT_OK(ReadManifests(&snapshot, &manifest_file_metas));
    std::vector<ManifestEntry> manifest_entries;
    PAIMON_RETURN_NOT_OK(ReadFileEntries(manifest_file_metas, &manifest_entries));
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
    std::optional<Snapshot> snapshot;
    std::vector<ManifestFileMeta> manifest_file_metas;
    PAIMON_RETURN_NOT_OK(ReadManifests(&snapshot, &manifest_file_metas));
    manifest_file_metas = PostFilterManifests(std::move(manifest_file_metas));

    std::vector<ManifestEntry> manifest_entries;
    PAIMON_RETURN_NOT_OK(ReadManifestEntries(manifest_file_metas, &manifest_entries));
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
    return std::make_shared<FileStoreScan::RawPlan>(scan_mode_, snapshot,
                                                    std::move(manifest_entries));
}

Status FileStoreScan::ReadManifests(std::optional<Snapshot>* snapshot_ptr,
                                    std::vector<ManifestFileMeta>* manifests_ptr) const {
    auto& snapshot = *snapshot_ptr;
    auto& manifests = *manifests_ptr;
    if (specified_snapshot_ != std::nullopt) {
        snapshot = specified_snapshot_;
    } else {
        PAIMON_ASSIGN_OR_RAISE(snapshot, snapshot_manager_->LatestSnapshot());
    }
    if (snapshot == std::nullopt) {
        manifests = std::vector<ManifestFileMeta>();
        return Status::OK();
    }
    std::vector<ManifestFileMeta> unfiltered_manifest_metas;
    PAIMON_RETURN_NOT_OK(ReadManifestsWithSnapshot(snapshot.value(), &unfiltered_manifest_metas));
    for (const auto& meta : unfiltered_manifest_metas) {
        PAIMON_ASSIGN_OR_RAISE(bool filter_meta_result, FilterManifestFileMeta(meta));
        if (filter_meta_result) {
            manifests.push_back(meta);
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
                                      std::vector<ManifestEntry>* manifest_entries) const {
    std::vector<std::future<Result<std::vector<ManifestEntry>>>> futures;
    for (const auto& meta : manifest_metas) {
        auto read_meta_task = [this, &meta]() -> Result<std::vector<ManifestEntry>> {
            std::vector<ManifestEntry> tmp_entries;
            PAIMON_RETURN_NOT_OK(ReadManifestFileMeta(meta, &tmp_entries));
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

Status FileStoreScan::ReadAndMergeFileEntries(const std::vector<ManifestFileMeta>& manifest_metas,
                                              std::vector<ManifestEntry>* merged_entries) const {
    std::vector<ManifestEntry> unmerged_entries;
    PAIMON_RETURN_NOT_OK(ReadFileEntries(manifest_metas, &unmerged_entries));
    std::unordered_set<FileEntry::Identifier> deleted_entries;
    for (const auto& entry : unmerged_entries) {
        if (entry.Kind() == FileKind::Delete()) {
            deleted_entries.insert(entry.CreateIdentifier());
        }
    }
    for (auto& entry : unmerged_entries) {
        if (entry.Kind() == FileKind::Add() &&
            deleted_entries.find(entry.CreateIdentifier()) == deleted_entries.end()) {
            merged_entries->push_back(std::move(entry));
        }
    }
    return Status::OK();
}

Status FileStoreScan::ReadAndNoMergeFileEntries(
    const std::vector<ManifestFileMeta>& manifest_metas,
    std::vector<ManifestEntry>* manifest_entries) const {
    return ReadFileEntries(manifest_metas, manifest_entries);
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
    if (!partition_filter_) {
        return true;
    }
    SimpleStats stats = manifest.PartitionStats();
    return partition_filter_->Test(
        partition_schema_,
        /*row_count=*/manifest.NumAddedFiles() + manifest.NumDeletedFiles(), stats.MinValues(),
        stats.MaxValues(), stats.NullCounts());
}

Status FileStoreScan::ReadManifestFileMeta(const ManifestFileMeta& manifest,
                                           std::vector<ManifestEntry>* entries) const {
    auto filter = [&](const ManifestEntry& entry) -> Result<bool> {
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
        return true;
    };
    std::vector<ManifestEntry> unfiltered_entries;
    PAIMON_RETURN_NOT_OK(manifest_file_->Read(manifest.FileName(), filter, &unfiltered_entries));
    entries->reserve(entries->size() + unfiltered_entries.size());
    for (auto& entry : unfiltered_entries) {
        PAIMON_ASSIGN_OR_RAISE(bool res, FilterByStats(entry));
        if (res) {
            entries->emplace_back(std::move(entry));
        }
    }
    return Status::OK();
}

Status FileStoreScan::SplitAndSetFilter(const std::vector<std::string>& partition_keys,
                                        const std::shared_ptr<arrow::Schema>& arrow_schema,
                                        const std::shared_ptr<ScanFilter>& scan_filters) {
    if (scan_filters->GetPredicate()) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldMappingBuilder> mapping_builder,
                               FieldMappingBuilder::Create(arrow_schema, partition_keys,
                                                           scan_filters->GetPredicate()));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldMapping> mapping,
                               mapping_builder->CreateFieldMapping(arrow_schema));
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
