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

#include "paimon/core/append/bucketed_append_compact_manager.h"

#include "paimon/common/executor/future.h"

namespace paimon {

BucketedAppendCompactManager::BucketedAppendCompactManager(
    const std::shared_ptr<Executor>& executor,
    const std::vector<std::shared_ptr<DataFileMeta>>& restored,
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer, int32_t min_file_num,
    int64_t target_file_size, int64_t compaction_file_size, bool force_rewrite_all_files,
    CompactRewriter rewriter, const std::shared_ptr<CompactionMetrics::Reporter>& reporter)
    : executor_(executor),
      dv_maintainer_(dv_maintainer),
      min_file_num_(min_file_num),
      target_file_size_(target_file_size),
      compaction_file_size_(compaction_file_size),
      force_rewrite_all_files_(force_rewrite_all_files),
      rewriter_(rewriter),
      reporter_(reporter),
      to_compact_(
          [](const std::shared_ptr<DataFileMeta>& lhs, const std::shared_ptr<DataFileMeta>& rhs) {
              return lhs->min_sequence_number > rhs->min_sequence_number;
          }),
      logger_(Logger::GetLogger("BucketedAppendCompactManager")) {
    for (const auto& file : restored) {
        to_compact_.push(file);
    }
}

Status BucketedAppendCompactManager::TriggerCompaction(bool full_compaction) {
    if (full_compaction) {
        PAIMON_RETURN_NOT_OK(TriggerFullCompaction());
    } else {
        TriggerCompactionWithBestEffort();
    }
    return Status::OK();
}

Status BucketedAppendCompactManager::TriggerFullCompaction() {
    if (task_future_.valid()) {
        return Status::Invalid(
            "A compaction task is still running while the user forces a new compaction. This "
            "is unexpected.");
    }
    // if all files are force picked or deletion vector enables, always trigger compaction.
    if (!force_rewrite_all_files_ &&
        (to_compact_.empty() ||
         (dv_maintainer_ == nullptr && to_compact_.size() < FULL_COMPACT_MIN_FILE))) {
        return Status::OK();
    }

    std::vector<std::shared_ptr<DataFileMeta>> compacting;
    while (!to_compact_.empty()) {
        compacting.push_back(to_compact_.top());
        to_compact_.pop();
    }
    auto compact_task = std::make_shared<FullCompactTask>(reporter_, dv_maintainer_, compacting,
                                                          compaction_file_size_,
                                                          force_rewrite_all_files_, rewriter_);
    task_future_ = Via(executor_.get(), [compact_task]() -> Result<std::shared_ptr<CompactResult>> {
        return compact_task->Execute();
    });
    compacting_ = compacting;
    return Status::OK();
}

void BucketedAppendCompactManager::TriggerCompactionWithBestEffort() {
    if (task_future_.valid()) {
        return;
    }
    std::optional<std::vector<std::shared_ptr<DataFileMeta>>> picked = PickCompactBefore();
    if (picked) {
        compacting_ = picked.value();
        auto compact_task = std::make_shared<AutoCompactTask>(reporter_, dv_maintainer_,
                                                              compacting_.value(), rewriter_);
        task_future_ =
            Via(executor_.get(), [compact_task]() -> Result<std::shared_ptr<CompactResult>> {
                return compact_task->Execute();
            });
    }
}

std::optional<std::vector<std::shared_ptr<DataFileMeta>>>
BucketedAppendCompactManager::PickCompactBefore() {
    if (to_compact_.empty()) {
        return std::nullopt;
    }
    int64_t total_file_size = 0;
    int32_t file_num = 0;
    std::deque<std::shared_ptr<DataFileMeta>> candidates;

    while (!to_compact_.empty()) {
        std::shared_ptr<DataFileMeta> file = to_compact_.top();
        to_compact_.pop();
        candidates.push_back(file);
        total_file_size += file->file_size;
        file_num++;
        if (file_num >= min_file_num_) {
            return std::vector<std::shared_ptr<DataFileMeta>>(candidates.begin(), candidates.end());
        } else if (total_file_size >= target_file_size_ * 2) {
            // Shift the compaction window right and drop the oldest file so picked files stay
            // contiguous, preserving append order during compaction.
            std::shared_ptr<DataFileMeta> removed = candidates.front();
            candidates.pop_front();
            total_file_size -= removed->file_size;
            file_num--;
        }
    }
    for (const auto& candidate : candidates) {
        to_compact_.push(candidate);
    }
    return std::nullopt;
}

std::vector<std::shared_ptr<DataFileMeta>> BucketedAppendCompactManager::AllFiles() const {
    std::vector<std::shared_ptr<DataFileMeta>> all_files;
    if (compacting_ != std::nullopt) {
        all_files.insert(all_files.end(), compacting_.value().begin(), compacting_.value().end());
    }
    auto to_compact = to_compact_;
    while (!to_compact.empty()) {
        all_files.push_back(to_compact.top());
        to_compact.pop();
    }
    return all_files;
}

Result<std::optional<std::shared_ptr<CompactResult>>>
BucketedAppendCompactManager::GetCompactionResult(bool blocking) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<CompactResult>> result,
                           InnerGetCompactionResult(blocking));
    if (result) {
        std::shared_ptr<CompactResult> compact_result = result.value();
        if (!compact_result->After().empty()) {
            // if the last compacted file is still small,
            // add it back to the head
            std::shared_ptr<DataFileMeta> last_file = compact_result->After().back();
            if (last_file->file_size < compaction_file_size_) {
                to_compact_.push(last_file);
            }
        }
        compacting_ = std::nullopt;
    }
    return result;
}

Result<std::shared_ptr<CompactResult>> BucketedAppendCompactManager::Compact(
    const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer,
    const std::vector<std::shared_ptr<DataFileMeta>>& to_compact, CompactRewriter rewriter) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> rewrite,
                           rewriter(to_compact));

    auto result = std::make_shared<CompactResult>(to_compact, rewrite);
    if (dv_maintainer != nullptr) {
        for (const auto& file : to_compact) {
            dv_maintainer->RemoveDeletionVectorOf(file->file_name);
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CompactDeletionFile> deletion_file,
                               CompactDeletionFile::GenerateFiles(dv_maintainer));
        result->SetDeletionFile(deletion_file);
    }
    return result;
}

Result<std::shared_ptr<CompactResult>> BucketedAppendCompactManager::FullCompactTask::DoCompact() {
    // remove large files
    while (!force_rewrite_all_files_ && !to_compact_.empty()) {
        const auto& file = to_compact_.front();
        // the data file with deletion file always need to be compacted.
        if (file->file_size >= compaction_file_size_ && !HasDeletionFile(file)) {
            to_compact_.pop_front();
            continue;
        }
        break;
    }

    // do compaction
    if (dv_maintainer_ != nullptr) {
        // if deletion vector enables, always trigger compaction.
        return Compact(
            dv_maintainer_,
            std::vector<std::shared_ptr<DataFileMeta>>(to_compact_.begin(), to_compact_.end()),
            rewriter_);
    } else {
        // compute small files
        int32_t big = 0;
        int32_t small = 0;
        for (const auto& file : to_compact_) {
            if (file->file_size >= compaction_file_size_) {
                big++;
            } else {
                small++;
            }
        }
        if (force_rewrite_all_files_ ||
            (small > big && to_compact_.size() >= FULL_COMPACT_MIN_FILE)) {
            return Compact(
                /*dv_maintainer=*/nullptr,
                std::vector<std::shared_ptr<DataFileMeta>>(to_compact_.begin(), to_compact_.end()),
                rewriter_);
        } else {
            return std::make_shared<CompactResult>();
        }
    }
}

}  // namespace paimon
