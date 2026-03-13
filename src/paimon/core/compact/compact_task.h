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

#include <chrono>
#include <memory>
#include <vector>

#include "paimon/core/compact/compact_result.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/metrics/compaction_metrics.h"
#include "paimon/core/utils/duration.h"
#include "paimon/logging.h"
#include "paimon/result.h"

namespace paimon {

/// Base Compact task for metrics.
class CompactTask {
 public:
    explicit CompactTask(const std::shared_ptr<CompactionMetrics::Reporter>& reporter)
        : reporter_(reporter), logger_(Logger::GetLogger("CompactTask")) {}

    virtual ~CompactTask() = default;

    Result<std::shared_ptr<CompactResult>> Execute() {
        Duration duration;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CompactResult> result, DoCompact());
        if (reporter_) {
            reporter_->ReportCompactionTime(static_cast<int64_t>(duration.Get()));
            reporter_->IncreaseCompactionsCompletedCount();
            reporter_->ReportCompactionInputSize(CollectRewriteSize(result->Before()));
            reporter_->ReportCompactionOutputSize(CollectRewriteSize(result->After()));
        }
        PAIMON_LOG_DEBUG(
            logger_,
            "Done compacting %zu files to %zu files in %lldms. Rewrite input file size "
            "= %lld, output file size = %lld",
            result->Before().size(), result->After().size(), static_cast<int64_t>(duration.Get()),
            CollectRewriteSize(result->Before()), CollectRewriteSize(result->After()));
        return result;
    }

 protected:
    /// Perform compaction.
    ///
    /// @return `CompactResult` of compact before and compact after files.
    virtual Result<std::shared_ptr<CompactResult>> DoCompact() = 0;

 private:
    static int64_t CollectRewriteSize(const std::vector<std::shared_ptr<DataFileMeta>>& files) {
        int64_t size = 0;
        for (const auto& file : files) {
            size += file->file_size;
        }
        return size;
    }

    std::shared_ptr<CompactionMetrics::Reporter> reporter_;
    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon
