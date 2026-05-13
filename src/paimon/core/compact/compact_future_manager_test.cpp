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

#include "paimon/core/compact/compact_future_manager.h"

#include <future>
#include <memory>
#include <optional>

#include "gtest/gtest.h"
#include "paimon/core/compact/compact_result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class FakeCompactFutureManager : public CompactFutureManager {
 public:
    Status AddNewFile(const std::shared_ptr<DataFileMeta>& /*file*/) override {
        return Status::OK();
    }

    std::vector<std::shared_ptr<DataFileMeta>> AllFiles() const override {
        return {};
    }

    Status TriggerCompaction(bool /*full_compaction*/) override {
        return Status::OK();
    }

    Result<std::optional<std::shared_ptr<CompactResult>>> GetCompactionResult(
        bool blocking) override {
        return InnerGetCompactionResult(blocking);
    }

    void RequestCancelCompaction() override {}

    bool ShouldWaitForLatestCompaction() const override {
        return false;
    }

    bool ShouldWaitForPreparingCheckpoint() const override {
        return false;
    }

    Status Close() override {
        return Status::OK();
    }

    void SetTaskFuture(std::future<Result<std::shared_ptr<CompactResult>>> future) {
        task_future_ = std::move(future);
    }
};

TEST(CompactFutureManagerTest, TestCancelledStatusReturnsEmptyOptional) {
    FakeCompactFutureManager manager;

    std::promise<Result<std::shared_ptr<CompactResult>>> promise;
    manager.SetTaskFuture(promise.get_future());

    // Simulate compaction returning a Cancelled status.
    promise.set_value(Status::Cancelled("Compaction is cancelled"));

    ASSERT_OK_AND_ASSIGN(auto result, manager.GetCompactionResult(/*blocking=*/true));
    // The IsCancelled branch should return an empty optional (not an error).
    ASSERT_FALSE(result.has_value());
    // After consuming the cancelled future, task should no longer be active.
    ASSERT_FALSE(manager.CompactNotCompleted());
}
}  // namespace paimon::test
