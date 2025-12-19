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

#pragma once
#include <cstdint>
#include <memory>
#include <optional>

#include "paimon/core/table/source/abstract_table_scan.h"
#include "paimon/core/table/source/snapshot/starting_scanner.h"
#include "paimon/result.h"
#include "paimon/table/source/plan.h"

namespace paimon {
class CoreOptions;
class SnapshotReader;

/// `TableScan` implementation for batch planning.
class DataTableBatchScan : public AbstractTableScan {
 public:
    DataTableBatchScan(bool pk_table, const CoreOptions& core_options,
                       const std::shared_ptr<SnapshotReader>& snapshot_reader,
                       std::optional<int32_t> push_down_limit);

    Result<std::shared_ptr<Plan>> CreatePlan() override;

    std::shared_ptr<PredicateFilter> GetNonPartitionPredicate() const {
        return snapshot_reader_->GetNonPartitionPredicate();
    }
    std::shared_ptr<PredicateFilter> GetPartitionPredicate() const {
        return snapshot_reader_->GetPartitionPredicate();
    }

    DataTableBatchScan* WithRowRanges(const std::vector<Range>& row_ranges) {
        snapshot_reader_->WithRowRanges(row_ranges);
        return this;
    }

 private:
    Result<std::shared_ptr<Plan>> ApplyPushDownLimit(
        const std::shared_ptr<StartingScanner::ScanResult>& scan_result) const;

 private:
    std::shared_ptr<StartingScanner> starting_scanner_;
    bool has_next_ = true;
    std::optional<int32_t> push_down_limit_;
};
}  // namespace paimon
