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

namespace paimon {

// Calculate operation duration.
class Duration {
 public:
    Duration() : start_(std::chrono::high_resolution_clock::now()) {}

    uint64_t Get() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start_)
            .count();
    }

    uint64_t Reset() {
        uint64_t duration = Get();
        start_ = std::chrono::high_resolution_clock::now();
        return duration;
    }

 private:
    std::chrono::high_resolution_clock::time_point start_;
};

}  // namespace paimon
