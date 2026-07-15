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

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <random>
#include <thread>

namespace paimon {

RetryWaiter::RetryWaiter(int64_t min_retry_wait_ms, int64_t max_retry_wait_ms)
    : min_retry_wait_ms_(std::max<int64_t>(0, min_retry_wait_ms)),
      max_retry_wait_ms_(std::max<int64_t>(0, max_retry_wait_ms)) {}

void RetryWaiter::RetryWait(int32_t retry_count) const {
    int32_t non_negative_retry_count = std::max<int32_t>(0, retry_count);
    double exponential = std::pow(2.0, static_cast<double>(non_negative_retry_count));
    int64_t retry_wait = 0;
    if (min_retry_wait_ms_ > 0 && max_retry_wait_ms_ > 0) {
        double max_safe_exponential =
            static_cast<double>(max_retry_wait_ms_) / static_cast<double>(min_retry_wait_ms_);
        if (!std::isfinite(exponential) || exponential >= max_safe_exponential) {
            retry_wait = max_retry_wait_ms_;
        } else {
            retry_wait = static_cast<int64_t>(min_retry_wait_ms_ * exponential);
        }
    }

    int64_t jitter_upper = std::max<int64_t>(1, static_cast<int64_t>(retry_wait * 0.2));
    std::mt19937 rng(std::random_device{}());  // NOLINT(whitespace/braces)
    std::uniform_int_distribution<int64_t> dist(0, jitter_upper - 1);
    retry_wait += dist(rng);

    if (retry_wait <= 0) {
        return;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(retry_wait));
}

}  // namespace paimon
