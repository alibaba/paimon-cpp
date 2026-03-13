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

#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <vector>

#include "paimon/common/executor/future.h"
#include "paimon/core/compact/compact_deletion_file.h"
#include "paimon/core/compact/compact_future_manager.h"
#include "paimon/core/compact/compact_task.h"
#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/metrics/compaction_metrics.h"
#include "paimon/executor.h"
#include "paimon/logging.h"
#include "paimon/result.h"

namespace paimon {

/// Compact manager for `AppendOnlyFileStore`.
class BucketedAppendCompactManager : public CompactFutureManager {
 public:
    using DataFileMetaPriorityQueue =
        std::priority_queue<std::shared_ptr<DataFileMeta>,
                            std::vector<std::shared_ptr<DataFileMeta>>,
                            std::function<bool(const std::shared_ptr<DataFileMeta>&,
                                               const std::shared_ptr<DataFileMeta>&)>>;

    using CompactRewriter = std::function<Result<std::vector<std::shared_ptr<DataFileMeta>>>(
        const std::vector<std::shared_ptr<DataFileMeta>>&)>;

    /// New files may be created during the compaction process, then the results of the compaction
    /// may be put after the new files, and this order will be disrupted. We need to ensure this
    /// order, so we force the order by sequence.
    static std::function<bool(const std::shared_ptr<DataFileMeta>&,
                              const std::shared_ptr<DataFileMeta>&)>
    FileComparator(bool ignore_overlap) {
        return [ignore_overlap](const std::shared_ptr<DataFileMeta>& lhs,
                                const std::shared_ptr<DataFileMeta>& rhs) -> bool {
            if (*lhs == *rhs) {
                return false;
            }
            if (!ignore_overlap && IsOverlap(lhs, rhs)) {
                // TODO(yonghao.fyh): add log
            }
            return lhs->min_sequence_number < rhs->min_sequence_number;
        };
    }

    BucketedAppendCompactManager(const std::shared_ptr<Executor>& executor,
                                 const std::vector<std::shared_ptr<DataFileMeta>>& restored,
                                 const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer,
                                 int32_t min_file_num, int64_t target_file_size,
                                 int64_t compaction_file_size, bool force_rewrite_all_files,
                                 CompactRewriter rewriter,
                                 const std::shared_ptr<CompactionMetrics::Reporter>& reporter);
    ~BucketedAppendCompactManager() override = default;

    Status TriggerCompaction(bool full_compaction) override;

    bool ShouldWaitForLatestCompaction() const override {
        return false;
    }
    bool ShouldWaitForPreparingCheckpoint() const override {
        return false;
    }

    void AddNewFile(const std::shared_ptr<DataFileMeta>& file) override {
        to_compact_.push(file);
    }

    std::vector<std::shared_ptr<DataFileMeta>> AllFiles() const override;

    /// Finish current task, and update result files to to_compact_
    Result<std::optional<std::shared_ptr<CompactResult>>> GetCompactionResult(
        bool blocking) override;

    Status Close() override {
        if (reporter_) {
            reporter_->Unregister();
            reporter_.reset();
        }
        return Status::OK();
    }

 private:
    static constexpr int32_t FULL_COMPACT_MIN_FILE = 3;

    static bool IsOverlap(const std::shared_ptr<DataFileMeta>& o1,
                          const std::shared_ptr<DataFileMeta>& o2) {
        return o2->min_sequence_number <= o1->max_sequence_number &&
               o2->max_sequence_number >= o1->min_sequence_number;
    }

    static Result<std::shared_ptr<CompactResult>> Compact(
        const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer,
        const std::vector<std::shared_ptr<DataFileMeta>>& to_compact, CompactRewriter rewriter);

    /// A `CompactTask` impl for full compaction of append-only table.
    class FullCompactTask : public CompactTask {
     public:
        FullCompactTask(const std::shared_ptr<CompactionMetrics::Reporter>& reporter,
                        const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer,
                        const std::vector<std::shared_ptr<DataFileMeta>>& inputs,
                        int64_t compaction_file_size, bool force_rewrite_all_files,
                        CompactRewriter rewriter)
            : CompactTask(reporter),
              dv_maintainer_(dv_maintainer),
              to_compact_(inputs.begin(), inputs.end()),
              compaction_file_size_(compaction_file_size),
              force_rewrite_all_files_(force_rewrite_all_files),
              rewriter_(rewriter) {}

     protected:
        Result<std::shared_ptr<CompactResult>> DoCompact() override;

     private:
        bool HasDeletionFile(const std::shared_ptr<DataFileMeta>& file) const {
            if (dv_maintainer_) {
                return dv_maintainer_->DeletionVectorOf(file->file_name) != std::nullopt;
            }
            return false;
        }

        static constexpr int32_t FULL_COMPACT_MIN_FILE = 3;

        std::shared_ptr<BucketedDvMaintainer> dv_maintainer_;
        std::deque<std::shared_ptr<DataFileMeta>> to_compact_;
        int64_t compaction_file_size_;
        bool force_rewrite_all_files_;
        CompactRewriter rewriter_;
    };

    /// A `CompactTask` impl for append-only table auto-compaction.
    ///
    /// This task accepts an already-picked candidate to perform one-time rewrite. And for the
    /// rest of input files, it is the duty of `AppendOnlyWriter` to invoke the next time
    /// compaction.
    class AutoCompactTask : public CompactTask {
     public:
        AutoCompactTask(const std::shared_ptr<CompactionMetrics::Reporter>& reporter,
                        const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer,
                        const std::vector<std::shared_ptr<DataFileMeta>>& to_compact,
                        CompactRewriter rewriter)
            : CompactTask(reporter),
              dv_maintainer_(dv_maintainer),
              to_compact_(to_compact),
              rewriter_(rewriter) {}

     protected:
        Result<std::shared_ptr<CompactResult>> DoCompact() override {
            return Compact(dv_maintainer_, to_compact_, rewriter_);
        }

     private:
        std::shared_ptr<BucketedDvMaintainer> dv_maintainer_;
        std::vector<std::shared_ptr<DataFileMeta>> to_compact_;
        CompactRewriter rewriter_;
    };

    DataFileMetaPriorityQueue GetToCompact() const {
        return to_compact_;
    }

    std::optional<std::vector<std::shared_ptr<DataFileMeta>>> PickCompactBefore();
    Status TriggerFullCompaction();
    void TriggerCompactionWithBestEffort();

    std::shared_ptr<Executor> executor_;
    std::shared_ptr<BucketedDvMaintainer> dv_maintainer_;
    int32_t min_file_num_;
    int64_t target_file_size_;
    int64_t compaction_file_size_;
    bool force_rewrite_all_files_;
    CompactRewriter rewriter_;
    std::shared_ptr<CompactionMetrics::Reporter> reporter_;
    std::optional<std::vector<std::shared_ptr<DataFileMeta>>> compacting_;
    DataFileMetaPriorityQueue to_compact_;
    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon
