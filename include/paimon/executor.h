/*
 * Copyright 2024-present Alibaba Inc.
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
#include <functional>
#include <memory>

#include "paimon/visibility.h"

namespace paimon {
class Executor;

static constexpr uint32_t DEFAULT_EXECUTOR_THREAD_COUNT = 4;

/// Get a system wide singleton executor.
PAIMON_EXPORT std::shared_ptr<Executor> GetGlobalDefaultExecutor();

/// Create a default implementation of executor with `DEFAULT_EXECUTOR_THREAD_COUNT`.
PAIMON_EXPORT std::unique_ptr<Executor> CreateDefaultExecutor();

/// Create a default implementation of executor with specified thread_count.
PAIMON_EXPORT std::unique_ptr<Executor> CreateDefaultExecutor(uint32_t thread_count);

/// Interface class for defining basic operations of a task executor.
///
/// The Executor class provides interfaces for adding tasks and waiting for all tasks to complete.
class PAIMON_EXPORT Executor {
 public:
    virtual ~Executor() = default;

    /// Add a task to the executor.
    ///
    /// @param func The task to be executed, represented as a function object with no parameters and
    /// no return value.
    ///
    /// @note This method should be thread-safe and can be called from multiple threads
    /// simultaneously.
    virtual void Add(std::function<void()> func) = 0;

    /// Shutdown the executor immediately, discarding all pending tasks.
    virtual void ShutdownNow() = 0;
};

}  // namespace paimon
