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
#include <cstdint>
#include <future>
#include <memory>
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
    for (int i = 0; i < 10; ++i) {
        futures.push_back(Via(executor.get(), [&sum]() { sum++; }));
    }
    Wait(futures);
    ASSERT_EQ(10, sum.load());
}

TEST(DefaultExecutorTest, TestVia) {
    auto executor = GetGlobalDefaultExecutor();
    std::atomic<int64_t> sum = {0};
    std::vector<std::future<int>> futures;
    for (int i = 0; i < 10; ++i) {
        futures.push_back(Via(executor.get(), [i, &sum]() -> int {
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

}  // namespace paimon::test
