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
    }

    /// Request cancellation for future-based compaction.
    ///
    /// `std::future` itself cannot be cancelled. Subclasses must override this
    /// method to signal their concrete cancellation controller.
    void RequestCancelCompaction() override = 0;

    /// Wait for the current compaction task to exit if it is running.
    /// @note This is a blocking join operation and may leave behind orphan files.
    void WaitForCompactionToExit() override {
        // std::future does not support cancellation natively.
        // Move away the active future first, then wait for completion so callers
        // can safely start a new task without reusing cancellation state.
        if (task_future_.valid()) {
            auto cancelled = std::move(task_future_);
            cancelled.wait();
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
            Result<std::shared_ptr<CompactResult>> result =
                ObtainCompactResult(std::move(task_future_));
            if (result.status().ok()) {
                return std::make_optional(std::move(result).value());
            } else if (result.status().IsCancelled()) {
                return std::optional<std::shared_ptr<CompactResult>>();
            } else {
                return result.status();
            }
        }
        return std::optional<std::shared_ptr<CompactResult>>();
    }

    std::future<Result<std::shared_ptr<CompactResult>>> task_future_;
};

}  // namespace paimon
