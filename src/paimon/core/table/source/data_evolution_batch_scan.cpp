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
    const std::shared_ptr<GlobalIndexResult>& global_index_result, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Executor>& executor)
    : AbstractTableScan(core_options, snapshot_reader),
      pool_(pool),
      table_path_(table_path),
      batch_scan_(std::move(batch_scan)),
      global_index_result_(global_index_result),
      executor_(executor) {}

Result<std::shared_ptr<Plan>> DataEvolutionBatchScan::CreatePlan() {
    std::optional<std::vector<Range>> row_ranges;
    std::shared_ptr<GlobalIndexResult> final_global_index_result = global_index_result_;
    if (!final_global_index_result) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> index_result, EvalGlobalIndex());
        if (index_result) {
            final_global_index_result = index_result;
            PAIMON_ASSIGN_OR_RAISE(row_ranges, index_result->ToRanges());
        }
    } else {
        PAIMON_ASSIGN_OR_RAISE(row_ranges, final_global_index_result->ToRanges());
    }
    if (!row_ranges) {
        return batch_scan_->CreatePlan();
    }
    if (row_ranges.value().empty()) {
        return PlanImpl::EmptyPlan();
    }
    PAIMON_ASSIGN_OR_RAISE(RowRangeIndex row_range_index,
                           RowRangeIndex::Create(row_ranges.value()));
    batch_scan_->WithRowRangeIndex(row_range_index);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> data_plan, batch_scan_->CreatePlan());
    std::map<int64_t, float> id_to_score;
    if (auto scored_result =
            std::dynamic_pointer_cast<ScoredGlobalIndexResult>(final_global_index_result)) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScoredGlobalIndexResult::ScoredIterator> scored_iter,
                               scored_result->CreateScoredIterator());
        while (scored_iter->HasNext()) {
            auto [id, score] = scored_iter->NextWithScore();
            id_to_score[id] = score;
        }
    }
    return WrapToIndexedSplits(data_plan, row_range_index, id_to_score);
}

Result<std::shared_ptr<Plan>> DataEvolutionBatchScan::WrapToIndexedSplits(
    const std::shared_ptr<Plan>& data_plan, const RowRangeIndex& row_range_index,
    const std::map<int64_t, float>& id_to_score) const {
    // TODO(lisizhuo.lsz): add executor here
    auto data_splits = data_plan->Splits();
    std::vector<std::shared_ptr<Split>> indexed_splits;
    indexed_splits.reserve(data_splits.size());
    for (const auto& split : data_splits) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        if (!data_split) {
            return Status::Invalid("Cannot cast split to DataSplit when create IndexedSplit");
        }
        const auto& files = data_split->DataFiles();
        if (files.empty()) {
            return Status::Invalid("Empty data files in WrapToIndexedSplits");
        }
        PAIMON_ASSIGN_OR_RAISE(int64_t min, files[0]->NonNullFirstRowId());
        PAIMON_ASSIGN_OR_RAISE(int64_t max, files[files.size() - 1]->NonNullFirstRowId());
        max += files[files.size() - 1]->row_count - 1;

        std::vector<Range> expected = row_range_index.IntersectedRanges(min, max);
        if (expected.empty()) {
            return Status::Invalid(
                fmt::format("There should be intersected ranges for split with min row id {} and "
                            "max row id {}.",
                            min, max));
        }

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

Result<std::shared_ptr<GlobalIndexResult>> DataEvolutionBatchScan::EvalGlobalIndex() const {
    auto predicate = batch_scan_->GetNonPartitionPredicate();
    if (!predicate) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    if (!core_options_.GlobalIndexEnabled()) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    auto partition_filter = batch_scan_->GetPartitionPredicate();
    // TODO(lisizhuo.lsz): support time travel
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<GlobalIndexScan> index_scan,
        GlobalIndexScan::Create(table_path_, core_options_.GetScanSnapshotId(), partition_filter,
                                core_options_.ToMap(), core_options_.GetFileSystem(), executor_,
                                pool_));
    auto index_scan_impl = dynamic_cast<GlobalIndexScanImpl*>(index_scan.get());
    if (!index_scan_impl) {
        return Status::Invalid("invalid GlobalIndexScan, cannot cast to GlobalIndexScanImpl");
    }

    return index_scan_impl->Scan(predicate);
}

}  // namespace paimon
