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
#include <functional>
#include <memory>
#include <vector>

#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/compact/compact_deletion_file.h"
#include "paimon/core/compact/compact_task.h"
#include "paimon/core/compact/compact_unit.h"
#include "paimon/core/mergetree/compact/compact_rewriter.h"
#include "paimon/core/mergetree/compact/interval_partition.h"
#include "paimon/core/mergetree/sorted_run.h"

namespace paimon {

/// Compact task for merge tree compaction.
class MergeTreeCompactTask : public CompactTask {
 public:
    using DeletionFileSupplier = std::function<Result<std::shared_ptr<CompactDeletionFile>>()>;

    // TODO(yonghao.fyh): Support RecordLevelExpire
    MergeTreeCompactTask(const std::shared_ptr<FieldsComparator>& key_comparator,
                         int64_t min_file_size, const std::shared_ptr<CompactRewriter>& rewriter,
                         const CompactUnit& unit, bool drop_delete, int32_t max_level,
                         const std::shared_ptr<CompactionMetrics::Reporter>& metrics_reporter,
                         DeletionFileSupplier compact_df_supplier, bool force_rewrite_all_files);

 protected:
    Result<std::shared_ptr<CompactResult>> DoCompact() override;

 private:
    Status Upgrade(const std::shared_ptr<DataFileMeta>& file, CompactResult* to_update);

    Status Rewrite(std::vector<std::vector<SortedRun>>* candidate, CompactResult* to_update);

    Status RewriteImpl(std::vector<std::vector<SortedRun>>* candidate, CompactResult* to_update);

    static bool ContainsDeleteRecords(const std::shared_ptr<DataFileMeta>& file);

    int64_t min_file_size_;
    std::shared_ptr<CompactRewriter> rewriter_;
    int32_t output_level_;
    DeletionFileSupplier compact_df_supplier_;
    std::vector<std::vector<SortedRun>> partitioned_;
    bool drop_delete_;
    int32_t max_level_;
    bool force_rewrite_all_files_;
    int32_t upgrade_files_num_ = 0;
};

}  // namespace paimon
