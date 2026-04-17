/*
 * Copyright 2026-present Alibaba Inc.
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

#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/compact/cancellation_controller.h"
#include "paimon/core/compact/compact_future_manager.h"
#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"
#include "paimon/core/mergetree/compact/compact_rewriter.h"
#include "paimon/core/mergetree/compact/compact_strategy.h"
#include "paimon/core/mergetree/levels.h"
#include "paimon/core/operation/metrics/compaction_metrics.h"
#include "paimon/executor.h"
#include "paimon/logging.h"

namespace paimon {

/// Compact manager for key value tables.
class MergeTreeCompactManager : public CompactFutureManager {
 public:
    MergeTreeCompactManager(const std::shared_ptr<Levels>& levels,
                            const std::shared_ptr<CompactStrategy>& strategy,
                            const std::shared_ptr<FieldsComparator>& key_comparator,
                            int64_t compaction_file_size, int32_t num_sorted_run_stop_trigger,
                            const std::shared_ptr<CompactRewriter>& rewriter,
                            const std::shared_ptr<CompactionMetrics::Reporter>& metrics_reporter,
                            const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer,
                            bool lazy_gen_deletion_file, bool need_lookup,
                            bool force_rewrite_all_files, bool force_keep_delete,
                            const std::shared_ptr<CancellationController>& cancellation_controller,
                            const std::shared_ptr<Executor>& executor);

    ~MergeTreeCompactManager() override = default;

    bool ShouldWaitForLatestCompaction() const override;

    bool ShouldWaitForPreparingCheckpoint() const override;

    Status AddNewFile(const std::shared_ptr<DataFileMeta>& file) override;

    std::vector<std::shared_ptr<DataFileMeta>> AllFiles() const override;

    Status TriggerCompaction(bool full_compaction) override;

    void RequestCancelCompaction() override;

    Result<std::optional<std::shared_ptr<CompactResult>>> GetCompactionResult(
        bool blocking) override;

    bool CompactNotCompleted() const override;

    Status Close() override;

    std::shared_ptr<Levels> GetLevels() const {
        return levels_;
    }

    std::shared_ptr<CompactStrategy> GetStrategy() const {
        return strategy_;
    }

 private:
    Status SubmitCompaction(const CompactUnit& unit, bool drop_delete);

    void ReportMetrics() const;

    std::shared_ptr<Executor> executor_;
    std::shared_ptr<Levels> levels_;
    std::shared_ptr<CompactStrategy> strategy_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    int64_t compaction_file_size_;
    int32_t num_sorted_run_stop_trigger_;
    std::shared_ptr<CompactRewriter> rewriter_;
    std::shared_ptr<CompactionMetrics::Reporter> metrics_reporter_;
    std::shared_ptr<BucketedDvMaintainer> dv_maintainer_;
    bool lazy_gen_deletion_file_;
    bool need_lookup_;
    bool force_rewrite_all_files_;
    bool force_keep_delete_;
    std::shared_ptr<CancellationController> cancellation_controller_;
    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon
