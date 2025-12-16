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

#include "paimon/core/global_index/global_index_scan_impl.h"

#include "paimon/core/global_index/row_range_global_index_scanner_impl.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/utils/snapshot_manager.h"

namespace paimon {
GlobalIndexScanImpl::GlobalIndexScanImpl(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const std::optional<int64_t>& snapshot_id,
    const std::optional<std::vector<std::map<std::string, std::string>>>& partitions,
    const CoreOptions& options, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      root_path_(root_path),
      table_schema_(table_schema),
      snapshot_id_(snapshot_id),
      partitions_(partitions),
      options_(options) {
    assert(!partitions || !partitions.value().empty());
}

Result<std::shared_ptr<RowRangeGlobalIndexScanner>> GlobalIndexScanImpl::CreateRangeScan(
    const Range& range) {
    PAIMON_RETURN_NOT_OK(Scan());
    std::optional<BinaryRow> partition;
    // field id -> {index type -> entry}
    std::map<int32_t, std::map<std::string, std::vector<IndexManifestEntry>>> filtered_entries;
    for (const auto& entry : entries_) {
        const auto& global_index_meta = entry.index_file->GetGlobalIndexMeta();
        assert(global_index_meta);
        const auto& meta = global_index_meta.value();
        if (Range::HasIntersection(range, Range(meta.row_range_start, meta.row_range_end))) {
            if (!partition) {
                partition = entry.partition;
            } else if (!(partition.value() == entry.partition)) {
                // TODO(xinyu.lxy): add inte case
                return Status::Invalid(
                    "input range contain multiple partitions, fail to create range scan");
            }
            filtered_entries[meta.index_field_id][entry.index_file->IndexType()].push_back(entry);
        }
    }
    std::shared_ptr<IndexPathFactory> index_file_path_factory =
        path_factory_->CreateGlobalIndexFileFactory();
    return std::make_shared<RowRangeGlobalIndexScannerImpl>(table_schema_, index_file_path_factory,
                                                            filtered_entries, options_, pool_);
}
Result<std::vector<Range>> GlobalIndexScanImpl::GetRowRangeList() {
    PAIMON_RETURN_NOT_OK(Scan());
    std::map<std::string, std::vector<Range>> index_ranges;
    for (const auto& entry : entries_) {
        const auto& global_index_meta = entry.index_file->GetGlobalIndexMeta();
        assert(global_index_meta);
        const auto& index_meta = global_index_meta.value();
        index_ranges[entry.index_file->IndexType()].push_back(
            Range(index_meta.row_range_start, index_meta.row_range_end));
    }
    std::string check_index_type;
    std::vector<Range> check_ranges;
    // check all type index have same shard ranges
    // If index a has [1,10],[20,30] and index b has [1,10],[20,25], it's inconsistent, because
    // it is hard to handle the [26,30] range.
    for (const auto& [type, ranges] : index_ranges) {
        if (check_index_type.empty()) {
            check_index_type = type;
            check_ranges = Range::SortAndMergeOverlap(ranges, /*adjacent=*/true);
        } else {
            auto merged = Range::SortAndMergeOverlap(ranges, /*adjacent=*/true);
            if (merged != check_ranges) {
                return Status::Invalid(
                    fmt::format("Inconsistent row ranges among index types: {} and {}",
                                check_index_type, type));
            }
        }
    }

    return check_ranges;
}

Status GlobalIndexScanImpl::Scan() {
    if (initialized_) {
        return Status::OK();
    }
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema_->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options_.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(
        path_factory_,
        FileStorePathFactory::Create(
            root_path_, arrow_schema, table_schema_->PartitionKeys(),
            options_.GetPartitionDefaultName(), options_.GetWriteFileFormat()->Identifier(),
            options_.DataFilePrefix(), options_.LegacyPartitionNameEnabled(), external_paths,
            options_.IndexFileInDataFileDir(), pool_));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<IndexManifestFile> index_manifest_file,
                           IndexManifestFile::Create(
                               options_.GetFileSystem(), options_.GetManifestFormat(),
                               options_.GetManifestCompression(), path_factory_, pool_, options_));
    auto index_file_handler = std::make_unique<IndexFileHandler>(
        std::move(index_manifest_file), std::make_shared<IndexFilePathFactories>(path_factory_));

    // prepare snapshot
    std::optional<Snapshot> snapshot;
    SnapshotManager snapshot_manager(options_.GetFileSystem(), root_path_);
    if (snapshot_id_) {
        PAIMON_ASSIGN_OR_RAISE(snapshot, snapshot_manager.LoadSnapshot(snapshot_id_.value()));
    } else {
        PAIMON_ASSIGN_OR_RAISE(snapshot, snapshot_manager.LatestSnapshot());
    }
    if (!snapshot) {
        return Status::Invalid("not found latest snapshot");
    }

    // prepare filter
    std::unordered_set<BinaryRow> partitions_set;
    if (partitions_) {
        for (const auto& partition : partitions_.value()) {
            PAIMON_ASSIGN_OR_RAISE(BinaryRow partition_row, path_factory_->ToBinaryRow(partition));
            partitions_set.insert(partition_row);
        }
    }
    std::function<Result<bool>(const IndexManifestEntry&)> filter =
        [&](const IndexManifestEntry& entry) -> bool {
        if (!partitions_set.empty() &&
            partitions_set.find(entry.partition) == partitions_set.end()) {
            return false;
        }
        if (!entry.index_file->GetGlobalIndexMeta()) {
            return false;
        }
        return true;
    };
    PAIMON_ASSIGN_OR_RAISE(entries_, index_file_handler->Scan(snapshot.value(), filter));
    initialized_ = true;
    return Status::OK();
}

}  // namespace paimon
