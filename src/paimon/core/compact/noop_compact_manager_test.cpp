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

#include "paimon/core/compact/noop_compact_manager.h"

#include <memory>
#include <optional>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(NoopCompactManagerTest, ShouldIgnoreAddedFilesAndStayIdle) {
    NoopCompactManager manager;

    ASSERT_OK(manager.AddNewFile(nullptr));
    ASSERT_TRUE(manager.AllFiles().empty());
    ASSERT_FALSE(manager.CompactNotCompleted());
    ASSERT_FALSE(manager.ShouldWaitForLatestCompaction());
    ASSERT_FALSE(manager.ShouldWaitForPreparingCheckpoint());
}

TEST(NoopCompactManagerTest, ShouldRejectUserTriggeredFullCompaction) {
    NoopCompactManager manager;

    ASSERT_NOK_WITH_MSG(manager.TriggerCompaction(/*full_compaction=*/true),
                        "NoopCompactManager does not support user triggered compaction");
    ASSERT_NOK_WITH_MSG(manager.TriggerCompaction(/*full_compaction=*/true), Options::WRITE_ONLY);
}

TEST(NoopCompactManagerTest, ShouldReturnEmptyCompactionResult) {
    NoopCompactManager manager;

    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<CompactResult>> non_blocking,
                         manager.GetCompactionResult(/*blocking=*/false));
    ASSERT_FALSE(non_blocking.has_value());

    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<CompactResult>> blocking,
                         manager.GetCompactionResult(/*blocking=*/true));
    ASSERT_FALSE(blocking.has_value());
}

TEST(NoopCompactManagerTest, ShouldAllowNoopLifecycleOperations) {
    NoopCompactManager manager;

    ASSERT_OK(manager.TriggerCompaction(/*full_compaction=*/false));
    manager.CancelAndWaitCompaction();
    ASSERT_OK(manager.Close());
}

}  // namespace paimon::test
