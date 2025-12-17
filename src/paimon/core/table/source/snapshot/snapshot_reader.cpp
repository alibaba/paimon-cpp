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

#include "paimon/core/table/source/snapshot/snapshot_reader.h"

#include <cassert>
#include <list>
#include <string>
#include <unordered_map>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/plan_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"

namespace paimon {
Result<std::shared_ptr<Plan>> SnapshotReader::Read() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> raw_plan, scan_->CreatePlan());
    const std::optional<Snapshot>& snapshot = raw_plan->GetSnapshot();
    FileStoreScan::RawPlan::GroupFiles files =
        FileStoreScan::RawPlan::GroupByPartFiles(raw_plan->Files(FileKind::Add()));
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::shared_ptr<Split>> data_splits,
        GenerateSplits(snapshot, scan_mode_ != ScanMode::ALL, split_generator_, std::move(files)));
    return std::make_shared<PlanImpl>(raw_plan->SnapshotId(), data_splits);
}

Result<std::vector<std::shared_ptr<Split>>> SnapshotReader::GenerateSplits(
    const std::optional<Snapshot>& snapshot, bool is_streaming,
    const std::unique_ptr<SplitGenerator>& split_generator,
    FileStoreScan::RawPlan::GroupFiles&& grouped_manifest_entries) const {
    std::vector<std::shared_ptr<Split>> splits;
    // Read deletion indexes at once to reduce file IO
    std::unordered_map<std::pair<BinaryRow, int32_t>, std::vector<std::shared_ptr<IndexFileMeta>>>
        deletion_index_files_map;
    bool deletion_file_enabled = scan_->GetCoreOptions().DeletionVectorsEnabled();
    if (!is_streaming) {
        if (deletion_file_enabled && snapshot != std::nullopt) {
            PAIMON_ASSIGN_OR_RAISE(
                deletion_index_files_map,
                index_file_handler_->Scan(
                    snapshot.value(), std::string(DeletionVectorsIndexFile::DELETION_VECTORS_INDEX),
                    grouped_manifest_entries.key_set()));
        }
    }
    for (auto& [partition, bucket_map] : grouped_manifest_entries) {
        for (auto& [bucket, manifest_entries] : bucket_map) {
            // collect data file metas
            assert(!manifest_entries.empty());
            auto total_buckets = manifest_entries[0].TotalBuckets();
            std::vector<std::shared_ptr<DataFileMeta>> files;
            files.reserve(manifest_entries.size());
            for (auto& entry : manifest_entries) {
                files.emplace_back(std::move(entry.File()));
            }
            std::vector<SplitGenerator::SplitGroup> split_groups;
            if (is_streaming) {
                PAIMON_ASSIGN_OR_RAISE(split_groups,
                                       split_generator->SplitForStreaming(std::move(files)));
            } else {
                PAIMON_ASSIGN_OR_RAISE(split_groups,
                                       split_generator->SplitForBatch(std::move(files)));
            }
            for (auto& split_group : split_groups) {
                std::vector<std::shared_ptr<DataFileMeta>>& data_files = split_group.files;
                PAIMON_ASSIGN_OR_RAISE(std::string bucket_path,
                                       path_factory_->BucketPath(partition, bucket));
                DataSplitImpl::Builder builder(partition, bucket, bucket_path,
                                               std::move(data_files));
                builder.WithTotalBuckets(total_buckets)
                    .WithSnapshot(snapshot == std::nullopt ? Snapshot::FIRST_SNAPSHOT_ID - 1
                                                           : snapshot.value().Id())
                    .IsStreaming(is_streaming)
                    .RawConvertible(split_group.raw_convertible);
                if (deletion_file_enabled && !deletion_index_files_map.empty()) {
                    PAIMON_ASSIGN_OR_RAISE(
                        std::vector<std::optional<DeletionFile>> deletion_files,
                        GetDeletionFiles(
                            partition, bucket, builder.DataFiles(),
                            deletion_index_files_map[std::make_pair(partition, bucket)]));
                    builder.WithDataDeletionFiles(deletion_files);
                }
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplit> data_split, builder.Build());
                splits.emplace_back(data_split);
            }
        }
    }
    return splits;
}

Result<std::vector<std::optional<DeletionFile>>> SnapshotReader::GetDeletionFiles(
    const BinaryRow& partition, int32_t bucket,
    const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
    const std::vector<std::shared_ptr<IndexFileMeta>>& index_file_metas) const {
    std::unordered_map<std::string, std::shared_ptr<IndexFileMeta>> data_file_to_index_file_meta;
    for (const auto& index_file_meta : index_file_metas) {
        const auto& dv_metas = index_file_meta->DvRanges();
        if (dv_metas != std::nullopt) {
            for (const auto& dv_meta_iter : dv_metas.value()) {
                const auto& dv_meta = dv_meta_iter.second;
                data_file_to_index_file_meta.insert(
                    std::make_pair(dv_meta.data_file_name, index_file_meta));
            }
        }
    }

    std::vector<std::optional<DeletionFile>> deletion_files;
    deletion_files.reserve(data_files.size());
    for (const auto& file : data_files) {
        auto index_file_meta_iter = data_file_to_index_file_meta.find(file->file_name);
        if (index_file_meta_iter != data_file_to_index_file_meta.end()) {
            const auto& optional_dv_metas = index_file_meta_iter->second->DvRanges();
            assert(optional_dv_metas != std::nullopt);
            const auto& dv_metas = optional_dv_metas.value();
            auto dv_meta_iter = dv_metas.find(file->file_name);
            if (dv_meta_iter != dv_metas.end()) {
                PAIMON_ASSIGN_OR_RAISE(
                    std::string index_file_path,
                    index_file_handler_->FilePath(partition, bucket, index_file_meta_iter->second));
                deletion_files.emplace_back(
                    DeletionFile(index_file_path, dv_meta_iter->second.offset,
                                 dv_meta_iter->second.length, dv_meta_iter->second.cardinality));
                continue;
            }
        }
        deletion_files.emplace_back(std::nullopt);
    }
    return deletion_files;
}

}  // namespace paimon
