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

#include "paimon/core/append/append_compact_task.h"

#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/core/compact/cancellation_controller.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/operation/append_only_file_store_write.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/sink/commit_message_impl.h"

namespace paimon {

AppendCompactTask::AppendCompactTask(const BinaryRow& partition,
                                     const std::vector<std::shared_ptr<DataFileMeta>>& files)
    : partition_(partition), compact_before_(files) {}

Result<std::shared_ptr<CommitMessage>> AppendCompactTask::DoCompact(
    const CoreOptions& options, AppendOnlyFileStoreWrite* write) {
    if (!options.DeletionVectorsEnabled() && compact_before_.size() <= 1) {
        return Status::Invalid("AppendCompactTask needs more than one file input.");
    }
    if (options.DeletionVectorsEnabled()) {
        // TODO(xinyu.lxy): support dv
        return Status::NotImplemented("not support for dv in UNAWARE_BUCKET mode");
    }
    // Non-DV mode: rewrite all compact_before files into new files.
    auto cancellation_controller = std::make_shared<CancellationController>();
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::shared_ptr<DataFileMeta>> rewritten,
        write->CompactRewrite(partition_, BucketModeDefine::UNAWARE_BUCKET,
                              /*dv_factory=*/nullptr, compact_before_, cancellation_controller));
    compact_after_ = std::move(rewritten);

    // Build CompactIncrement with before/after file lists and empty changelog/index files.
    auto compact_before_copy = compact_before_;
    auto compact_after_copy = compact_after_;
    CompactIncrement compact_increment(std::move(compact_before_copy),
                                       std::move(compact_after_copy),
                                       /*changelog_files=*/{},
                                       /*new_index_files=*/{},
                                       /*deleted_index_files=*/{});

    // Build an empty DataIncrement (no new data files from compaction).
    DataIncrement data_increment(/*new_files=*/{}, /*deleted_files=*/{}, /*changelog_files=*/{});

    // Bucket 0 is the bucket for unaware-bucket table, for compatibility with the old design.
    auto commit_message = std::make_shared<CommitMessageImpl>(partition_,
                                                              /*bucket=*/0,
                                                              /*total_buckets=*/options.GetBucket(),
                                                              data_increment, compact_increment);

    return std::static_pointer_cast<CommitMessage>(commit_message);
}

std::string AppendCompactTask::ToString() const {
    std::vector<std::string> before_names;
    before_names.reserve(compact_before_.size());
    for (const auto& file : compact_before_) {
        before_names.emplace_back(file->file_name);
    }

    std::vector<std::string> after_names;
    after_names.reserve(compact_after_.size());
    for (const auto& file : compact_after_) {
        after_names.emplace_back(file->file_name);
    }

    return fmt::format(
        "CompactionTask {{partition = {}, compactBefore = [{}], compactAfter = [{}]}}",
        partition_.ToString(), fmt::join(before_names, ", "), fmt::join(after_names, ", "));
}

}  // namespace paimon
