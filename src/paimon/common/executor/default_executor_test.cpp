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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/executor/future.h"
#include "paimon/executor.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon::test {

TEST(DefaultExecutorTest, TestViaVoidFunc) {
    auto executor = GetGlobalDefaultExecutor();
    std::atomic<int64_t> sum = {0};
    std::vector<std::future<void>> futures;
    for (int32_t i = 0; i < 10; ++i) {
        futures.push_back(Via(executor.get(), [&sum]() { sum++; }));
    }
    Wait(futures);
    ASSERT_EQ(10, sum.load());
}

TEST(DefaultExecutorTest, TestVia) {
    auto executor = GetGlobalDefaultExecutor();
    std::atomic<int64_t> sum = {0};
    std::vector<std::future<int>> futures;
    for (int32_t i = 0; i < 10; ++i) {
        futures.push_back(Via(executor.get(), [i, &sum]() -> int32_t {
            sum++;
            return i * 2;
        }));
    }
    auto results = CollectAll(futures);
    ASSERT_EQ(10, results.size());
    std::vector<int> expect = {0, 2, 4, 6, 8, 10, 12, 14, 16, 18};
    ASSERT_EQ(expect, results);
    ASSERT_EQ(10, sum.load());
}

TEST(DefaultExecutorTest, TestViaWithResult) {
    auto executor = GetGlobalDefaultExecutor();
    std::vector<std::future<Result<std::vector<int32_t>>>> futures;
    std::vector<int32_t> inputs = {-2, -1, 1, 2};
    for (const auto& input : inputs) {
        futures.push_back(Via(executor.get(), [input]() -> Result<std::vector<int32_t>> {
            if (input > 0) {
                std::vector<int32_t> output = {-2, -1, 1, 2};
                return output;
            }
            return Status::Invalid("negative");
        }));
    }
    auto results = CollectAll(futures);
    ASSERT_EQ(4, results.size());
}

TEST(DefaultExecutorTest, TestViaWithException) {
    auto executor = GetGlobalDefaultExecutor();
    auto future = Via(executor.get(), []() { throw std::runtime_error("test"); });
    ASSERT_THROW(future.get(), std::runtime_error);
}

TEST(DefaultExecutorTest, TestShutdownNowDropsPendingTasks) {
    auto executor = CreateDefaultExecutor(/*thread_count=*/1);
    std::atomic<bool> first_started = false;
    std::atomic<int32_t> executed_count = 0;
    std::promise<void> release_first_task;
    auto release_future = release_first_task.get_future();
    executor->Add([&]() {
        first_started.store(true);
        release_future.wait();
        ++executed_count;
    });

    for (int32_t index = 0; index < 20; ++index) {
        executor->Add([&]() { ++executed_count; });
    }

    for (int32_t retry = 0; retry < 100 && !first_started.load(); ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_TRUE(first_started.load());
    std::thread shutdown_thread([&]() { executor->ShutdownNow(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    release_first_task.set_value();
    shutdown_thread.join();

    // Only the running task should complete. Pending tasks should be discarded.
    ASSERT_EQ(executed_count.load(), 1);
}

TEST(DefaultExecutorTest, TestAddTaskAfterShutdownNowIgnored) {
    auto executor = CreateDefaultExecutor(/*thread_count=*/1);
    std::atomic<int32_t> executed_count = 0;

    executor->ShutdownNow();
    executor->Add([&]() { ++executed_count; });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    ASSERT_EQ(executed_count.load(), 0);
}

}  // namespace paimon::test
