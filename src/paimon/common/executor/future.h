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

#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <unordered_set>
#include <utility>
#include <vector>

#include "paimon/executor.h"
#include "paimon/result.h"

namespace paimon {

/// Submits a function to be executed asynchronously on a given executor and returns a
/// future.
///
/// This function wraps the provided callable (`func`) and submits it to the provided `executor`.
/// The function captures the result of `func` using a `std::promise`, which is used to fulfill
/// the returned `std::future`. If the callable throws an exception, the exception is captured
/// and set in the `promise`.
///
/// @tparam Func The type of the callable function.
/// @param executor The executor to run the function on. Must provide an `Add` method for task
/// submission.
/// @param func The function to execute asynchronously. Can be any callable object.
/// @return std::future<decltype(func())> A future that holds the result of the function
/// execution.
///
/// @note If `func` returns `void`, the returned future is of type `std::future<void>`.
template <typename Func>
auto Via(Executor* executor, Func&& func) -> std::future<decltype(func())> {
    using ResultType = decltype(func());

    // Check if func is callable (invocable)
    static_assert(std::is_invocable_v<Func>, "func must be callable");

    // Check if func is an empty std::function
    if constexpr (std::is_constructible_v<std::function<void()>, Func>) {
        std::function<void()> test_func = func;
        if (!test_func) {
            assert(false && "func cannot be an empty std::function");
        }
    }

    // Create a promise to store the result or exception.
    auto promise = std::make_shared<std::promise<ResultType>>();
    auto future = promise->get_future();  // Retrieve the future associated with the promise.

    // Wrap the task and submit it to the executor.
    executor->Add([promise, func = std::forward<Func>(func)]() mutable {
        try {
            if constexpr (std::is_void_v<ResultType>) {
                func();
                promise->set_value();
            } else {
                promise->set_value(func());
            }
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    });

    return future;
}

/// Collects the results of multiple futures.
///
/// This function waits for all provided futures to complete and collects their results.
/// The results are returned in a `std::vector`, preserving the order of the input futures.
///
/// @tparam T The type of the result stored in the futures.
/// @param futures A container of futures (e.g., std::vector<std::future<T>>).
/// @return std::vector<T> A vector of results corresponding to the input futures.
///
/// @note If `T` is `void`, use Wait function instead.
template <typename T>
std::vector<T> CollectAll(std::vector<std::future<T>>& futures) {
    std::vector<T> results;
    results.reserve(futures.size());  // Reserve space to avoid reallocation.
    for (auto& future : futures) {
        results.push_back(future.get());  // Wait for each future and collect the result.
    }

    return results;
}

/// Waits for all futures with `void` return type to complete.
///
/// This function waits for each provided `std::future<void>` to complete.
/// It ensures that all asynchronous operations have finished execution.
///
/// @param futures A container of `std::future<void>` (e.g., `std::vector<std::future<void>>`).
///
/// @note Use this function when dealing with futures that do not return a value.
inline void Wait(std::vector<std::future<void>>& futures) {
    for (auto& future : futures) {
        if (future.valid()) {
            future.get();
        }
    }
}

}  // namespace paimon
