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

#include <string>
#include <unordered_set>
#include <utility>

#include "paimon/common/executor/future.h"
#include "paimon/core/global_index/row_range_global_index_scanner_impl.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/global_index/bitmap_global_index_result.h"
namespace paimon {
GlobalIndexScanImpl::GlobalIndexScanImpl(const std::string& root_path,
                                         const std::shared_ptr<TableSchema>& table_schema,
                                         const Snapshot& snapshot,
                                         const std::shared_ptr<PredicateFilter>& partitions,
                                         const CoreOptions& options,
                                         const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      root_path_(root_path),
      table_schema_(table_schema),
      snapshot_(snapshot),
      partitions_(partitions),
      options_(options) {}

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
    std::map<std::string, std::vector<Range>> index_type_to_ranges;
    std::vector<Range> index_ranges;
    index_ranges.reserve(entries_.size());
    for (const auto& entry : entries_) {
        const auto& global_index_meta = entry.index_file->GetGlobalIndexMeta();
        assert(global_index_meta);
        const auto& index_meta = global_index_meta.value();
        Range range(index_meta.row_range_start, index_meta.row_range_end);
        index_ranges.push_back(range);
        index_type_to_ranges[entry.index_file->IndexType()].push_back(range);
    }
    std::string check_index_type;
    std::vector<Range> check_ranges;
    // check all type index have same shard ranges
    // If index a has [1,10],[20,30] and index b has [1,10],[20,25], it's inconsistent, because
    // it is hard to handle the [26,30] range.
    for (const auto& [type, ranges] : index_type_to_ranges) {
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
    return Range::SortAndMergeOverlap(index_ranges, /*adjacent=*/false);
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

    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> partition_fields,
                           table_schema_->GetFields(table_schema_->PartitionKeys()));
    auto partition_schema = DataField::ConvertDataFieldsToArrowSchema(partition_fields);
    std::function<Result<bool>(const IndexManifestEntry&)> filter =
        [&](const IndexManifestEntry& entry) -> Result<bool> {
        if (partitions_) {
            PAIMON_ASSIGN_OR_RAISE(bool saved,
                                   partitions_->Test(partition_schema, entry.partition));
            if (!saved) {
                return false;
            }
        }
        if (!entry.index_file->GetGlobalIndexMeta()) {
            return false;
        }
        return true;
    };
    PAIMON_ASSIGN_OR_RAISE(entries_, index_file_handler->Scan(snapshot_, filter));
    initialized_ = true;
    return Status::OK();
}

Result<std::optional<std::shared_ptr<GlobalIndexResult>>> GlobalIndexScanImpl::ParallelScan(
    const std::vector<Range>& ranges, const std::shared_ptr<Predicate>& predicate,
    const std::shared_ptr<VectorSearch>& vector_search, const std::shared_ptr<Executor>& executor) {
    std::vector<std::shared_ptr<RowRangeGlobalIndexScannerImpl>> range_scanners;
    range_scanners.reserve(ranges.size());
    for (const auto& range : ranges) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RowRangeGlobalIndexScanner> scanner,
                               CreateRangeScan(range));
        auto scanner_impl = std::dynamic_pointer_cast<RowRangeGlobalIndexScannerImpl>(scanner);
        if (!scanner_impl) {
            return Status::Invalid(
                "invalid RowRangeGlobalIndexScanner, fail to cast to "
                "RowRangeGlobalIndexScannerImpl");
        }
        range_scanners.push_back(scanner_impl);
    }

    std::vector<std::future<Result<std::optional<std::shared_ptr<GlobalIndexResult>>>>> futures;
    for (size_t i = 0; i < range_scanners.size(); i++) {
        const auto& scanner = range_scanners[i];
        const auto& range = ranges[i];
        auto search_index =
            [&scanner, &predicate, &vector_search,
             &range]() -> Result<std::optional<std::shared_ptr<GlobalIndexResult>>> {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexEvaluator> evaluator,
                                   scanner->CreateIndexEvaluator());
            PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<GlobalIndexResult>> index_result,
                                   evaluator->Evaluate(predicate, vector_search));
            if (!index_result) {
                return index_result;
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result_with_offset,
                                   index_result.value()->AddOffset(range.from));
            return std::optional<std::shared_ptr<GlobalIndexResult>>(result_with_offset);
        };
        futures.push_back(Via(executor.get(), search_index));
    }
    auto collected_results = CollectAll(futures);

    // collect inner result and check all null
    bool all_null = true;
    std::vector<std::optional<std::shared_ptr<GlobalIndexResult>>> results;
    for (auto& result : collected_results) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<GlobalIndexResult>> inner_result,
                               result);
        if (inner_result) {
            all_null = false;
        }
        results.push_back(std::move(inner_result));
    }
    if (all_null) {
        return std::optional<std::shared_ptr<GlobalIndexResult>>();
    }

    // union result from multiple ranges
    std::optional<std::shared_ptr<GlobalIndexResult>> final_global_index_result;

    for (size_t i = 0; i < results.size(); ++i) {
        std::shared_ptr<GlobalIndexResult> result =
            results[i] ? results[i].value() : BitmapGlobalIndexResult::FromRanges({ranges[i]});
        if (!final_global_index_result) {
            final_global_index_result = result;
        } else {
            PAIMON_ASSIGN_OR_RAISE(final_global_index_result,
                                   final_global_index_result.value()->Or(result));
        }
    }
    return std::optional<std::shared_ptr<GlobalIndexResult>>(final_global_index_result);
}
}  // namespace paimon
