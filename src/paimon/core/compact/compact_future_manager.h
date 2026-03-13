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

#include <future>

#include "paimon/core/compact/compact_manager.h"

namespace paimon {

class CompactFutureManager : public CompactManager {
 public:
    ~CompactFutureManager() override {
        if (task_future_.valid()) {
            task_future_.wait();
        }
        for (auto& f : cancelled_futures_) {
            if (f.valid()) {
                f.wait();
            }
        }
    }

    /// Cancel the current compaction task if it is running.
    /// @note: This method may leave behind orphan files.
    void CancelCompaction() override {
        // std::future does not support cancellation natively.
        if (task_future_.valid()) {
            // Detach the future so we don't block on destruction
            cancelled_futures_.push_back(std::move(task_future_));
        }
    }

    bool CompactNotCompleted() const override {
        return task_future_.valid();
    }

 protected:
    Result<std::shared_ptr<CompactResult>> ObtainCompactResult(
        std::future<Result<std::shared_ptr<CompactResult>>> task_future) {
        return task_future.get();
    }

    Result<std::optional<std::shared_ptr<CompactResult>>> InnerGetCompactionResult(bool blocking) {
        if (!task_future_.valid()) {
            return std::optional<std::shared_ptr<CompactResult>>();
        }
        bool ready = blocking ||
                     (task_future_.wait_for(std::chrono::seconds(0)) == std::future_status::ready);
        if (ready) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CompactResult> result,
                                   ObtainCompactResult(std::move(task_future_)));
            return std::make_optional(std::move(result));
        }
        return std::optional<std::shared_ptr<CompactResult>>();
    }

    std::future<Result<std::shared_ptr<CompactResult>>> task_future_;
    std::vector<std::future<Result<std::shared_ptr<CompactResult>>>> cancelled_futures_;
};

}  // namespace paimon
