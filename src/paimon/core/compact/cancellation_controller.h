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

#include <atomic>

namespace paimon {

class CancellationController {
 public:
    void Cancel() {
        cancelled_.store(true, std::memory_order_relaxed);
    }

    void Reset() {
        cancelled_.store(false, std::memory_order_relaxed);
    }

    bool IsCancelled() const {
        return cancelled_.load(std::memory_order_relaxed);
    }

 private:
    std::atomic_bool cancelled_{false};
};

}  // namespace paimon
