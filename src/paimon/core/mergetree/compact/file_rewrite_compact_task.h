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
#include <vector>

#include "paimon/core/compact/compact_task.h"
#include "paimon/core/compact/compact_unit.h"
#include "paimon/core/mergetree/compact/compact_rewriter.h"
#include "paimon/core/mergetree/sorted_run.h"

namespace paimon {

/// Compact task for file rewrite compaction.
class FileRewriteCompactTask : public CompactTask {
 public:
    FileRewriteCompactTask(const std::shared_ptr<CompactRewriter>& rewriter,
                           const CompactUnit& unit, bool drop_delete,
                           const std::shared_ptr<CompactionMetrics::Reporter>& metrics_reporter)
        : CompactTask(metrics_reporter),
          rewriter_(rewriter),
          output_level_(unit.output_level),
          files_(unit.files),
          drop_delete_(drop_delete) {}

 protected:
    Result<std::shared_ptr<CompactResult>> DoCompact() override {
        auto result = std::make_shared<CompactResult>();
        for (const auto& file : files_) {
            PAIMON_RETURN_NOT_OK(RewriteFile(file, result.get()));
        }
        return result;
    }

 private:
    Status RewriteFile(const std::shared_ptr<DataFileMeta>& file, CompactResult* to_update) {
        std::vector<std::vector<SortedRun>> candidate = {{SortedRun::FromSingle(file)}};
        PAIMON_ASSIGN_OR_RAISE(CompactResult rewritten,
                               rewriter_->Rewrite(output_level_, drop_delete_, candidate));
        return to_update->Merge(rewritten);
    }

    std::shared_ptr<CompactRewriter> rewriter_;
    int32_t output_level_;
    std::vector<std::shared_ptr<DataFileMeta>> files_;
    bool drop_delete_;
};

}  // namespace paimon
