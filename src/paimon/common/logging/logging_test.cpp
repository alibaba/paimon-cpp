/*
 * Copyright 2025-present Alibaba Inc.
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
#include "paimon/logging.h"

#include <future>
#include <random>
#include <thread>

#include "paimon/common/executor/future.h"
#include "paimon/executor.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
TEST(LoggerTest, TestMultiThreadGetLogger) {
    auto executor = CreateDefaultExecutor(/*thread_count=*/4);
    auto get_logger = []() {
        auto logger = Logger::GetLogger("my_log");
        ASSERT_TRUE(logger);
    };

    std::vector<std::future<void>> futures;
    for (int i = 0; i < 1000; ++i) {
        futures.push_back(Via(executor.get(), get_logger));
    }
    Wait(futures);
}
}  // namespace paimon::test
