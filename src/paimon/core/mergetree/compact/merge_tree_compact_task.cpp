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

#include "paimon/core/mergetree/compact/merge_tree_compact_task.h"

namespace paimon {

MergeTreeCompactTask::MergeTreeCompactTask(
    const std::shared_ptr<FieldsComparator>& key_comparator, int64_t min_file_size,
    const std::shared_ptr<CompactRewriter>& rewriter, const CompactUnit& unit, bool drop_delete,
    int32_t max_level, const std::shared_ptr<CompactionMetrics::Reporter>& metrics_reporter,
    DeletionFileSupplier compact_df_supplier, bool force_rewrite_all_files)
    : CompactTask(metrics_reporter),
      min_file_size_(min_file_size),
      rewriter_(rewriter),
      output_level_(unit.output_level),
      compact_df_supplier_(std::move(compact_df_supplier)),
      partitioned_(IntervalPartition(unit.files, key_comparator).Partition()),
      drop_delete_(drop_delete),
      max_level_(max_level),
      force_rewrite_all_files_(force_rewrite_all_files) {}

Result<std::shared_ptr<CompactResult>> MergeTreeCompactTask::DoCompact() {
    std::vector<std::vector<SortedRun>> candidate;
    auto result = std::make_shared<CompactResult>();

    // Checking the order and compacting adjacent and contiguous files
    // Note: can't skip an intermediate file to compact, this will destroy the overall orderliness
    for (const auto& section : partitioned_) {
        if (section.size() > 1) {
            candidate.push_back(section);
        } else if (!section.empty()) {
            const auto& run = section[0];
            // No overlapping:
            // We can just upgrade the large file and just change the level instead of rewriting
            // it. But for small files, we will try to compact it.
            for (const auto& file : run.Files()) {
                if (file->file_size < min_file_size_) {
                    // Smaller files are rewritten along with the previous files.
                    candidate.push_back({SortedRun::FromSingle(file)});
                } else {
                    // Large file appears, rewrite previous and upgrade it.
                    PAIMON_RETURN_NOT_OK(Rewrite(&candidate, result.get()));
                    PAIMON_RETURN_NOT_OK(Upgrade(file, result.get()));
                }
            }
        }
    }

    PAIMON_RETURN_NOT_OK(Rewrite(&candidate, result.get()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CompactDeletionFile> deletion_file,
                           compact_df_supplier_());
    result->SetDeletionFile(deletion_file);
    return result;
}

Status MergeTreeCompactTask::Upgrade(const std::shared_ptr<DataFileMeta>& file,
                                     CompactResult* to_update) {
    // TODO(yonghao.fyh): check expire
    if ((output_level_ == max_level_ && ContainsDeleteRecords(file)) || force_rewrite_all_files_) {
        std::vector<std::vector<SortedRun>> candidate = {{SortedRun::FromSingle(file)}};
        return RewriteImpl(&candidate, to_update);
    }

    if (file->level != output_level_) {
        PAIMON_ASSIGN_OR_RAISE(CompactResult upgraded, rewriter_->Upgrade(output_level_, file));
        PAIMON_RETURN_NOT_OK(to_update->Merge(upgraded));
        ++upgrade_files_num_;
    }
    return Status::OK();
}

Status MergeTreeCompactTask::Rewrite(std::vector<std::vector<SortedRun>>* candidate,
                                     CompactResult* to_update) {
    if (candidate->empty()) {
        return Status::OK();
    }

    if (candidate->size() == 1) {
        const auto& section = (*candidate)[0];
        if (section.empty()) {
            return Status::Invalid("invalid section, section cannot be empty in candidate");
        }
        if (section.size() == 1) {
            for (const auto& file : section[0].Files()) {
                PAIMON_RETURN_NOT_OK(Upgrade(file, to_update));
            }
            candidate->clear();
            return Status::OK();
        }
    }

    return RewriteImpl(candidate, to_update);
}

Status MergeTreeCompactTask::RewriteImpl(std::vector<std::vector<SortedRun>>* candidate,
                                         CompactResult* to_update) {
    PAIMON_ASSIGN_OR_RAISE(CompactResult rewritten,
                           rewriter_->Rewrite(output_level_, drop_delete_, *candidate));
    PAIMON_RETURN_NOT_OK(to_update->Merge(rewritten));
    candidate->clear();
    return Status::OK();
}

bool MergeTreeCompactTask::ContainsDeleteRecords(const std::shared_ptr<DataFileMeta>& file) {
    return !file->delete_row_count.has_value() || file->delete_row_count.value() > 0;
}

}  // namespace paimon
