/*
 * Copyright 2025-present Alibaba Inc.
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

#include "paimon/core/table/source/data_evolution_batch_scan.h"

#include "paimon/core/global_index/global_index_scan_impl.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/global_index_scan.h"

namespace paimon {
DataEvolutionBatchScan::DataEvolutionBatchScan(
    const std::string& table_path, const std::shared_ptr<SnapshotReader>& snapshot_reader,
    std::unique_ptr<DataTableBatchScan>&& batch_scan,
    const std::shared_ptr<GlobalIndexResult>& global_index_result,
    const std::shared_ptr<VectorSearch>& vector_search, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Executor>& executor)
    : AbstractTableScan(core_options, snapshot_reader),
      pool_(pool),
      table_path_(table_path),
      batch_scan_(std::move(batch_scan)),
      global_index_result_(global_index_result),
      vector_search_(vector_search),
      executor_(executor) {}

Result<std::shared_ptr<Plan>> DataEvolutionBatchScan::CreatePlan() {
    std::optional<std::vector<Range>> row_ranges;
    std::shared_ptr<GlobalIndexResult> final_global_index_result = global_index_result_;
    if (!final_global_index_result) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<GlobalIndexResult>> index_result,
                               EvalGlobalIndex());
        if (index_result) {
            final_global_index_result = index_result.value();
            PAIMON_ASSIGN_OR_RAISE(row_ranges, index_result.value()->ToRanges());
        }
    } else {
        PAIMON_ASSIGN_OR_RAISE(row_ranges, final_global_index_result->ToRanges());
    }
    if (!row_ranges) {
        return batch_scan_->CreatePlan();
    }
    batch_scan_->WithRowRanges(row_ranges.value());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> data_plan, batch_scan_->CreatePlan());
    std::map<int64_t, float> id_to_score;
    if (auto vector_search_result =
            std::dynamic_pointer_cast<VectorSearchGlobalIndexResult>(final_global_index_result)) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<VectorSearchGlobalIndexResult::VectorSearchIterator> vector_search_iter,
            vector_search_result->CreateVectorSearchIterator());
        while (vector_search_iter->HasNext()) {
            auto [id, score] = vector_search_iter->NextWithScore();
            id_to_score[id] = score;
        }
    }
    return WrapToIndexedSplits(data_plan, row_ranges.value(), id_to_score);
}

Result<std::shared_ptr<Plan>> DataEvolutionBatchScan::WrapToIndexedSplits(
    const std::shared_ptr<Plan>& data_plan, const std::vector<Range>& row_ranges,
    const std::map<int64_t, float>& id_to_score) const {
    std::vector<Range> sorted_row_ranges =
        Range::SortAndMergeOverlap(row_ranges, /*adjacent=*/true);
    auto data_splits = data_plan->Splits();
    std::vector<std::shared_ptr<Split>> indexed_splits;
    indexed_splits.reserve(data_splits.size());
    for (const auto& split : data_splits) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        if (!data_split) {
            return Status::Invalid("Cannot cast split to DataSplit when create IndexedSplit");
        }
        std::vector<Range> file_ranges;
        file_ranges.reserve(data_split->DataFiles().size());
        for (const auto& meta : data_split->DataFiles()) {
            PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, meta->NonNullFirstRowId());
            file_ranges.emplace_back(first_row_id, first_row_id + meta->row_count - 1);
        }
        auto sorted_file_ranges = Range::SortAndMergeOverlap(file_ranges, /*adjacent=*/true);
        std::vector<Range> expected = Range::And(sorted_file_ranges, sorted_row_ranges);
        std::vector<float> scores;
        if (!id_to_score.empty()) {
            for (const auto& range : expected) {
                for (int64_t i = range.from; i <= range.to; i++) {
                    auto iter = id_to_score.find(i);
                    if (iter != id_to_score.end()) {
                        scores.push_back(iter->second);
                    } else {
                        return Status::Invalid(fmt::format("cannot find score for row {}", i));
                    }
                }
            }
        }
        indexed_splits.push_back(std::make_shared<IndexedSplitImpl>(data_split, expected, scores));
    }
    return std::make_shared<PlanImpl>(data_plan->SnapshotId(), indexed_splits);
}

Result<std::optional<std::shared_ptr<GlobalIndexResult>>> DataEvolutionBatchScan::EvalGlobalIndex()
    const {
    auto predicate = batch_scan_->GetNonPartitionPredicate();
    if (!predicate && !vector_search_) {
        return std::optional<std::shared_ptr<GlobalIndexResult>>();
    }
    if (!core_options_.GlobalIndexEnabled()) {
        return std::optional<std::shared_ptr<GlobalIndexResult>>();
    }
    auto partition_filter = batch_scan_->GetPartitionPredicate();
    // TODO(lisizhuo.lsz): support time travel
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<GlobalIndexScan> index_scan,
        GlobalIndexScan::Create(table_path_, core_options_.GetScanSnapshotId(), partition_filter,
                                core_options_.ToMap(), core_options_.GetFileSystem(), pool_));
    auto index_scan_impl = dynamic_cast<GlobalIndexScanImpl*>(index_scan.get());
    if (!index_scan_impl) {
        return Status::Invalid("invalid GlobalIndexScan, cannot cast to GlobalIndexScanImpl");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<Range> indexed_row_ranges, index_scan->GetRowRangeList());
    if (indexed_row_ranges.empty()) {
        return std::optional<std::shared_ptr<GlobalIndexResult>>();
    }
    const auto& snapshot = index_scan_impl->GetSnapshot();
    const std::optional<int64_t>& next_row_id = snapshot.NextRowId();
    if (!next_row_id) {
        return Status::Invalid("invalid snapshot, next row id is null");
    }

    std::vector<Range> non_indexed_row_ranges =
        Range(0, next_row_id.value() - 1).Exclude(indexed_row_ranges);
    PAIMON_ASSIGN_OR_RAISE(
        std::optional<std::shared_ptr<GlobalIndexResult>> index_result,
        index_scan_impl->ParallelScan(indexed_row_ranges, predicate, vector_search_, executor_));
    if (!index_result) {
        return std::optional<std::shared_ptr<GlobalIndexResult>>();
    }
    auto index_result_value = std::move(index_result).value();
    if (!non_indexed_row_ranges.empty()) {
        for (const auto& range : non_indexed_row_ranges) {
            PAIMON_ASSIGN_OR_RAISE(
                index_result_value,
                index_result_value->Or(BitmapGlobalIndexResult::FromRanges({range})));
        }
    }
    return std::optional<std::shared_ptr<paimon::GlobalIndexResult>>(index_result_value);
}

}  // namespace paimon
