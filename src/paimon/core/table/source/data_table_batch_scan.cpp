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

#include "paimon/core/table/source/data_table_batch_scan.h"

#include <functional>
#include <utility>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/options/merge_engine.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/plan_impl.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/status.h"

namespace paimon {
class DataSplit;

DataTableBatchScan::DataTableBatchScan(bool pk_table, const CoreOptions& core_options,
                                       const std::shared_ptr<SnapshotReader>& snapshot_reader,
                                       std::optional<int32_t> push_down_limit)
    : AbstractTableScan(core_options, snapshot_reader), push_down_limit_(push_down_limit) {
    if (pk_table && (core_options.DeletionVectorsEnabled() ||
                     core_options.GetMergeEngine() == MergeEngine::FIRST_ROW)) {
        auto level_filter = [](int32_t level) -> bool { return level > 0; };
        snapshot_reader_->WithLevelFilter(level_filter);
        snapshot_reader_->EnableValueFilter();
    }
    if (core_options.GetBucket() == BucketModeDefine::POSTPONE_BUCKET) {
        snapshot_reader_->OnlyReadRealBuckets();
    }
}

Result<std::shared_ptr<Plan>> DataTableBatchScan::CreatePlan() {
    if (starting_scanner_ == nullptr) {
        PAIMON_ASSIGN_OR_RAISE(starting_scanner_, CreateStartingScanner(/*is_streaming=*/false));
    }
    if (has_next_) {
        has_next_ = false;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<StartingScanner::ScanResult> scan_result,
                               starting_scanner_->Scan(snapshot_reader_));
        return ApplyPushDownLimit(scan_result);
    }
    return Status::Invalid("end of scan");
}

Result<std::shared_ptr<Plan>> DataTableBatchScan::ApplyPushDownLimit(
    const std::shared_ptr<StartingScanner::ScanResult>& scan_result) const {
    auto current_scan_result =
        std::dynamic_pointer_cast<StartingScanner::CurrentSnapshot>(scan_result);
    if (!current_scan_result) {
        // NoSnapshot
        return PlanImpl::EmptyPlan();
    }
    if (push_down_limit_ == std::nullopt) {
        return current_scan_result->GetPlan();
    }
    std::vector<std::shared_ptr<Split>> splits = current_scan_result->Splits();
    std::vector<std::shared_ptr<Split>> limited_data_splits;
    limited_data_splits.reserve(splits.size());
    int64_t scanned_row_count = 0;
    for (const auto& split : splits) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        if (!data_split) {
            return Status::Invalid("DataSplit cannot cast to DataSplitImpl");
        }
        if (data_split->RawConvertible()) {
            int64_t partial_merged_row_count = data_split->PartialMergedRowCount();
            limited_data_splits.emplace_back(data_split);
            scanned_row_count += partial_merged_row_count;
            if (scanned_row_count >= push_down_limit_.value()) {
                PAIMON_ASSIGN_OR_RAISE(int64_t snapshot_id, current_scan_result->SnapshotId());
                return std::make_shared<PlanImpl>(snapshot_id, limited_data_splits);
            }
        }
    }
    return current_scan_result->GetPlan();
}

}  // namespace paimon
