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

#pragma once
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "paimon/core/table/source/abstract_table_scan.h"
#include "paimon/core/table/source/data_table_batch_scan.h"
#include "paimon/result.h"
#include "paimon/utils/range.h"

namespace paimon {
class DataEvolutionBatchScan : public AbstractTableScan {
 public:
    DataEvolutionBatchScan(const std::string& table_path,
                           const std::shared_ptr<SnapshotReader>& snapshot_reader,
                           std::unique_ptr<DataTableBatchScan>&& batch_scan,
                           const std::shared_ptr<GlobalIndexResult>& global_index_result,
                           const std::shared_ptr<VectorSearch>& vector_search,
                           const CoreOptions& core_options, const std::shared_ptr<MemoryPool>& pool,
                           const std::shared_ptr<Executor>& executor);

    Result<std::shared_ptr<Plan>> CreatePlan() override;

 private:
    Result<std::shared_ptr<Plan>> WrapToIndexedSplits(
        const std::shared_ptr<Plan>& data_plan, const std::vector<Range>& row_ranges,
        const std::map<int64_t, float>& id_to_score) const;
    Result<std::optional<std::shared_ptr<GlobalIndexResult>>> EvalGlobalIndex() const;

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::string table_path_;
    std::unique_ptr<DataTableBatchScan> batch_scan_;
    std::shared_ptr<GlobalIndexResult> global_index_result_;
    std::shared_ptr<VectorSearch> vector_search_;
    std::shared_ptr<Executor> executor_;
};

}  // namespace paimon
