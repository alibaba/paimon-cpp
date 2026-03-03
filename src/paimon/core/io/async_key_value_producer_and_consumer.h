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

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/sort_merge_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "tbb/concurrent_queue.h"

namespace paimon {
template <typename T, typename R>
class AsyncKeyValueConsumer;
class MemoryPool;
class Metrics;

// Asynchronous iterate SortMergeReader (producer) and row-to-array conversion (consumer), support
// multi-threaded conversion, R can be BatchReader::ReadBatch, KeyValueBatch
template <typename T, typename R>
class AsyncKeyValueProducerAndConsumer {
 public:
    using ConsumerCreator =
        std::function<Result<std::unique_ptr<RowToArrowArrayConverter<T, R>>>()>;

    AsyncKeyValueProducerAndConsumer(std::unique_ptr<SortMergeReader>&& sort_merge_reader,
                                     ConsumerCreator create_consumer, int32_t batch_size,
                                     int32_t consumer_thread_num,
                                     const std::shared_ptr<MemoryPool>& pool);

    ~AsyncKeyValueProducerAndConsumer() {
        CleanUp();
    }

    Result<R> NextBatch();

    std::shared_ptr<Metrics> GetReaderMetrics() const {
        return sort_merge_reader_->GetReaderMetrics();
    }

    void Close() {
        CleanUp();
        sort_merge_reader_->Close();
    }

 private:
    static constexpr int32_t RESULT_BATCH_COUNT = 3;

    // in case write batch size is too large and overflow arrow array
    static constexpr int32_t MAX_PROJECTION_BATCH_SIZE = 100000;

    void CleanUpQueue();
    Status ProduceLoop();
    void CleanUp();
    Status CheckStatus() const;
    Status CheckStatusAndCleanUp();

 private:
    int32_t batch_size_;
    int32_t consumer_thread_num_;
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<SortMergeReader> sort_merge_reader_;
    ConsumerCreator create_consumer_;

    // produce: merge sort KeyValue and push result KeyValue to kv_queue_, consume: project KeyValue
    // to arrow array and push result array to result_queue_
    std::atomic<bool> consume_finished_ = false;
    std::atomic<bool> next_batch_finished_ = false;
    std::shared_future<Status> producer_future_;
    std::vector<std::unique_ptr<AsyncKeyValueConsumer<T, R>>> consumers_;
    std::atomic<int32_t> consumer_finished_count_ = 0;
    tbb::concurrent_bounded_queue<std::optional<KeyValue>> kv_queue_;
    tbb::concurrent_bounded_queue<R> result_queue_;
};

template <typename T, typename R>
class AsyncKeyValueConsumer {
 public:
    AsyncKeyValueConsumer(int32_t batch_size,
                          std::unique_ptr<RowToArrowArrayConverter<T, R>>&& key_value_consumer,
                          std::atomic<bool>& consume_finished,
                          std::atomic<int32_t>& consumer_finished_count,
                          tbb::concurrent_bounded_queue<std::optional<KeyValue>>& kv_queue,
                          tbb::concurrent_bounded_queue<R>& result_queue);

    ~AsyncKeyValueConsumer() {
        CleanUp();
    }

    Status GetStatus() const;
    void CleanUp();

 private:
    Status ConsumeLoop();

 private:
    int32_t batch_size_;
    std::unique_ptr<RowToArrowArrayConverter<T, R>> key_value_consumer_;
    std::shared_future<Status> consumer_future_;
    std::atomic<bool>& consume_finished_;
    std::atomic<int32_t>& consumer_finished_count_;
    tbb::concurrent_bounded_queue<std::optional<KeyValue>>& kv_queue_;
    tbb::concurrent_bounded_queue<R>& result_queue_;
};

}  // namespace paimon
