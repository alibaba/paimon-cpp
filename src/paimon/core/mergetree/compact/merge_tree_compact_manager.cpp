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

#include "paimon/core/mergetree/compact/merge_tree_compact_manager.h"

#include <cassert>
#include <iterator>

#include "fmt/format.h"
#include "paimon/common/executor/future.h"
#include "paimon/core/compact/compact_deletion_file.h"
#include "paimon/core/compact/compact_task.h"
#include "paimon/core/mergetree/compact/file_rewrite_compact_task.h"
#include "paimon/core/mergetree/compact/interval_partition.h"
#include "paimon/core/mergetree/compact/merge_tree_compact_task.h"

namespace paimon {

std::string FilesToString(const std::vector<LevelSortedRun>& runs) {
    fmt::memory_buffer buffer;
    bool first = true;
    for (const auto& level_sorted_run : runs) {
        for (const auto& file : level_sorted_run.run.Files()) {
            if (!first) {
                fmt::format_to(std::back_inserter(buffer), ", ");
            }
            fmt::format_to(std::back_inserter(buffer), "({}, {}, {})", file->file_name, file->level,
                           file->file_size);
            first = false;
        }
    }
    return fmt::to_string(buffer);
}

MergeTreeCompactManager::MergeTreeCompactManager(
    const std::shared_ptr<Levels>& levels, const std::shared_ptr<CompactStrategy>& strategy,
    const std::shared_ptr<FieldsComparator>& key_comparator, int64_t compaction_file_size,
    int32_t num_sorted_run_stop_trigger, const std::shared_ptr<CompactRewriter>& rewriter,
    const std::shared_ptr<CompactionMetrics::Reporter>& metrics_reporter,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, bool lazy_gen_deletion_file,
    bool need_lookup, bool force_rewrite_all_files, bool force_keep_delete,
    const std::shared_ptr<CancellationController>& cancellation_controller,
    const std::shared_ptr<Executor>& executor)
    : executor_(executor),
      levels_(levels),
      strategy_(strategy),
      key_comparator_(key_comparator),
      compaction_file_size_(compaction_file_size),
      num_sorted_run_stop_trigger_(num_sorted_run_stop_trigger),
      rewriter_(rewriter),
      metrics_reporter_(metrics_reporter),
      dv_maintainer_(dv_maintainer),
      lazy_gen_deletion_file_(lazy_gen_deletion_file),
      need_lookup_(need_lookup),
      force_rewrite_all_files_(force_rewrite_all_files),
      force_keep_delete_(force_keep_delete),
      cancellation_controller_(cancellation_controller),
      logger_(Logger::GetLogger("MergeTreeCompactManager")) {
    assert(cancellation_controller_ != nullptr);
    ReportMetrics();
}

bool MergeTreeCompactManager::ShouldWaitForLatestCompaction() const {
    return levels_->NumberOfSortedRuns() > num_sorted_run_stop_trigger_;
}

bool MergeTreeCompactManager::ShouldWaitForPreparingCheckpoint() const {
    // cast to long to avoid Numeric overflow
    return levels_->NumberOfSortedRuns() > (static_cast<int64_t>(num_sorted_run_stop_trigger_) + 1);
}

Status MergeTreeCompactManager::AddNewFile(const std::shared_ptr<DataFileMeta>& file) {
    // if overwrite an empty partition, the snapshot will be changed to APPEND, then its files
    // might be upgraded to high level, thus we should use #update
    PAIMON_RETURN_NOT_OK(levels_->Update(/*before=*/{}, /*after=*/{file}));
    ReportMetrics();
    return Status::OK();
}

std::vector<std::shared_ptr<DataFileMeta>> MergeTreeCompactManager::AllFiles() const {
    return levels_->AllFiles();
}

Status MergeTreeCompactManager::TriggerCompaction(bool full_compaction) {
    std::optional<CompactUnit> optional_unit;
    std::vector<LevelSortedRun> runs = levels_->LevelSortedRuns();

    if (full_compaction) {
        if (task_future_.valid()) {
            return Status::Invalid(
                "A compaction task is still running while the user forces a new compaction. This "
                "is unexpected.");
        }

        PAIMON_LOG_DEBUG(logger_, "Trigger forced full compaction. Picking from runs:\n%s",
                         StringUtils::VectorToString(runs).c_str());
        optional_unit = CompactStrategy::PickFullCompaction(
            levels_->NumberOfLevels(), runs, dv_maintainer_, force_rewrite_all_files_);
    } else {
        if (task_future_.valid()) {
            return Status::OK();
        }

        PAIMON_LOG_DEBUG(logger_, "Trigger normal compaction. Picking from the following runs:\n%s",
                         StringUtils::VectorToString(runs).c_str());

        PAIMON_ASSIGN_OR_RAISE(std::optional<CompactUnit> picked,
                               strategy_->Pick(levels_->NumberOfLevels(), runs));
        if (picked && !picked->files.empty() &&
            (picked->files.size() > 1 || picked->files[0]->level != picked->output_level)) {
            optional_unit = picked;
        }
    }

    if (!optional_unit) {
        return Status::OK();
    }

    // As long as there is no older data, We can drop the deletion.
    // If the output level is 0, there may be older data not involved in compaction.
    // If the output level is bigger than 0, as long as there is no older data in
    // the current levels, the output is the oldest, so we can drop the deletion.
    // See CompactStrategy::Pick.
    const CompactUnit& unit = optional_unit.value();
    bool drop_delete =
        !force_keep_delete_ && unit.output_level != 0 &&
        (unit.output_level >= levels_->NonEmptyHighestLevel() || dv_maintainer_ != nullptr);

    PAIMON_LOG_DEBUG(logger_, "Submit compaction with files (name, level, size): %s",
                     StringUtils::VectorToString(levels_->LevelSortedRuns()).c_str());
    return SubmitCompaction(unit, drop_delete);
}

void MergeTreeCompactManager::CancelCompaction() {
    cancellation_controller_->Cancel();
    CompactFutureManager::CancelCompaction();
}

Status MergeTreeCompactManager::SubmitCompaction(const CompactUnit& unit, bool drop_delete) {
    cancellation_controller_->Reset();
    if (unit.file_rewrite) {
        auto task = std::make_shared<FileRewriteCompactTask>(rewriter_, unit, drop_delete,
                                                             metrics_reporter_);
        task_future_ = Via(executor_.get(), [task]() -> Result<std::shared_ptr<CompactResult>> {
            return task->Execute();
        });
    } else {
        MergeTreeCompactTask::DeletionFileSupplier compact_df_supplier =
            []() -> Result<std::shared_ptr<CompactDeletionFile>> {
            return std::shared_ptr<CompactDeletionFile>();
        };
        if (dv_maintainer_ != nullptr) {
            if (lazy_gen_deletion_file_) {
                compact_df_supplier = [dv = dv_maintainer_]() {
                    return CompactDeletionFile::LazyGeneration(dv);
                };
            } else {
                compact_df_supplier = [dv = dv_maintainer_]() {
                    return CompactDeletionFile::GenerateFiles(dv);
                };
            }
        }

        auto task = std::make_shared<MergeTreeCompactTask>(
            key_comparator_, compaction_file_size_, rewriter_, unit, drop_delete,
            levels_->MaxLevel(), metrics_reporter_, compact_df_supplier, force_rewrite_all_files_);
        task_future_ = Via(executor_.get(), [task]() -> Result<std::shared_ptr<CompactResult>> {
            return task->Execute();
        });
    }

    if (metrics_reporter_) {
        metrics_reporter_->IncreaseCompactionsQueuedCount();
        metrics_reporter_->IncreaseCompactionsTotalCount();
    }
    return Status::OK();
}

Result<std::optional<std::shared_ptr<CompactResult>>> MergeTreeCompactManager::GetCompactionResult(
    bool blocking) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<CompactResult>> result,
                           InnerGetCompactionResult(blocking));
    if (result) {
        const auto& compact_result = result.value();
        PAIMON_RETURN_NOT_OK(levels_->Update(compact_result->Before(), compact_result->After()));
        ReportMetrics();
        PAIMON_LOG_DEBUG(logger_, "Levels in compact manager updated. Current runs are\n%s",
                         StringUtils::VectorToString(levels_->LevelSortedRuns()).c_str());
    }
    return result;
}

bool MergeTreeCompactManager::CompactNotCompleted() const {
    // If it is a lookup compaction, we should ensure that all level 0 files are consumed, so
    // here we need to make the outside think that we still need to do unfinished compact
    // working
    return CompactFutureManager::CompactNotCompleted() ||
           (need_lookup_ && !levels_->GetLevel0().empty());
}

Status MergeTreeCompactManager::Close() {
    PAIMON_RETURN_NOT_OK(rewriter_->Close());
    if (metrics_reporter_) {
        metrics_reporter_->Unregister();
        metrics_reporter_.reset();
    }
    return Status::OK();
}

void MergeTreeCompactManager::ReportMetrics() const {
    if (metrics_reporter_) {
        metrics_reporter_->ReportLevel0FileCount(levels_->GetLevel0().size());
        metrics_reporter_->ReportTotalFileSize(levels_->TotalFileSize());
    }
}

}  // namespace paimon
