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

#include "paimon/core/operation/commit/retry_waiter.h"

#include <chrono>

#include "gtest/gtest.h"

namespace paimon::test {

TEST(RetryWaiterTest, TestRetryWaitWithZeroBoundsReturnsQuickly) {
    RetryWaiter waiter(/*min_retry_wait_ms=*/0, /*max_retry_wait_ms=*/0);

    auto begin = std::chrono::steady_clock::now();
    waiter.RetryWait(/*retry_count=*/3);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - begin);

    ASSERT_LT(elapsed.count(), 20);
}

}  // namespace paimon::test
