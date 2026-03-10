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

#include "paimon/executor.h"

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

namespace paimon {

class DefaultExecutor : public Executor {
 public:
    explicit DefaultExecutor(uint32_t thread_count);
    ~DefaultExecutor() override;

    void Add(std::function<void()> func) override;

    void ShutdownNow() override;

 private:
    void WorkerThread();

    void ShutdownInternal(bool wait_for_pending_tasks);

    uint32_t thread_count_;
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    bool stop_ = false;
    int32_t active_tasks_ = 0;
};

DefaultExecutor::DefaultExecutor(uint32_t thread_count) : thread_count_(thread_count) {
    for (uint32_t i = 0; i < thread_count_; ++i) {
        workers_.emplace_back(&DefaultExecutor::WorkerThread, this);
    }
}

void DefaultExecutor::ShutdownInternal(bool wait_for_pending_tasks) {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (stop_) {
            return;
        }
        stop_ = true;
        if (!wait_for_pending_tasks) {
            // Discard all pending tasks immediately.
            std::queue<std::function<void()>> empty;
            tasks_.swap(empty);
        }
        condition_.notify_all();
    }
    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

DefaultExecutor::~DefaultExecutor() {
    // Graceful shutdown: wait for all pending tasks to complete.
    ShutdownInternal(/*wait_for_pending_tasks=*/true);
}

void DefaultExecutor::ShutdownNow() {
    // Immediate shutdown: discard all pending tasks.
    ShutdownInternal(/*wait_for_pending_tasks=*/false);
}

void DefaultExecutor::Add(std::function<void()> func) {
    if (!func) {
        return;
    }
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (stop_) {
            return;
        }
        tasks_.emplace(std::move(func));
    }
    condition_.notify_one();
}

void DefaultExecutor::WorkerThread() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            condition_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
            if (stop_ && tasks_.empty() && active_tasks_ == 0) {
                condition_.notify_all();
                return;
            }
            if (!tasks_.empty()) {
                task = std::move(tasks_.front());
                tasks_.pop();
                ++active_tasks_;
            }
        }
        if (task) {
            task();
            std::unique_lock<std::mutex> lock(queue_mutex_);
            --active_tasks_;
            if (tasks_.empty() && active_tasks_ == 0) {
                condition_.notify_all();
            }
        }
    }
}

PAIMON_EXPORT std::shared_ptr<Executor> GetGlobalDefaultExecutor() {
    static uint32_t all_cores = std::thread::hardware_concurrency();
    static std::shared_ptr<Executor> internal =
        std::make_shared<DefaultExecutor>(/*thread_count=*/all_cores);
    return internal;
}

PAIMON_EXPORT std::unique_ptr<Executor> CreateDefaultExecutor() {
    return CreateDefaultExecutor(DEFAULT_EXECUTOR_THREAD_COUNT);
}

PAIMON_EXPORT std::unique_ptr<Executor> CreateDefaultExecutor(uint32_t thread_count) {
    return std::make_unique<DefaultExecutor>(thread_count);
}

}  // namespace paimon
