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

#include "paimon/core/io/async_key_value_producer_and_consumer.h"

#include <unistd.h>

#include <chrono>
#include <type_traits>

#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/reader/batch_reader.h"

namespace paimon {
class MemoryPool;

template <typename T, typename R>
AsyncKeyValueProducerAndConsumer<T, R>::AsyncKeyValueProducerAndConsumer(
    std::unique_ptr<SortMergeReader>&& sort_merge_reader, ConsumerCreator create_consumer,
    int32_t batch_size, int32_t consumer_thread_num, const std::shared_ptr<MemoryPool>& pool)
    : batch_size_(std::min(batch_size, MAX_PROJECTION_BATCH_SIZE)),
      consumer_thread_num_(consumer_thread_num),
      pool_(pool),
      sort_merge_reader_(std::move(sort_merge_reader)),
      create_consumer_(std::move(create_consumer)) {
    kv_queue_.set_capacity(batch_size);
    result_queue_.set_capacity(RESULT_BATCH_COUNT);
}

template <typename T, typename R>
Status AsyncKeyValueProducerAndConsumer<T, R>::CheckStatus() const {
    // check producer status
    std::chrono::microseconds span(0);
    std::future_status status = producer_future_.wait_for(span);
    if (status == std::future_status::ready) {
        PAIMON_RETURN_NOT_OK(producer_future_.get());
    }
    // check consumer status
    for (const auto& consumer : consumers_) {
        PAIMON_RETURN_NOT_OK(consumer->GetStatus());
    }
    return Status::OK();
}

template <typename T, typename R>
Status AsyncKeyValueProducerAndConsumer<T, R>::CheckStatusAndCleanUp() {
    auto status = CheckStatus();
    if (!status.ok()) {
        CleanUp();
    }
    return status;
}

template <typename T, typename R>
Result<R> AsyncKeyValueProducerAndConsumer<T, R>::NextBatch() {
    if (!producer_future_.valid()) {
        producer_future_ = std::async(std::launch::async,
                                      &AsyncKeyValueProducerAndConsumer<T, R>::ProduceLoop, this)
                               .share();
    }
    if (consumers_.empty()) {
        consumers_.reserve(consumer_thread_num_);
        for (int32_t i = 0; i < consumer_thread_num_; i++) {
            Result<std::unique_ptr<RowToArrowArrayConverter<T, R>>> consumer = create_consumer_();
            PAIMON_RETURN_NOT_OK(consumer.status());
            auto async_consumer = std::make_unique<AsyncKeyValueConsumer<T, R>>(
                batch_size_ / consumer_thread_num_, std::move(consumer).value(), consume_finished_,
                consumer_finished_count_, kv_queue_, result_queue_);
            consumers_.push_back(std::move(async_consumer));
        }
    }
    PAIMON_RETURN_NOT_OK(CheckStatusAndCleanUp());

    if (next_batch_finished_) {
        // projection reader is eof
        return R();
    }

    R result;
    while (!result_queue_.try_pop(result)) {
        PAIMON_RETURN_NOT_OK(CheckStatusAndCleanUp());
        if (consumer_finished_count_ == consumer_thread_num_ && result_queue_.empty()) {
            // all consume thread finished
            next_batch_finished_ = true;
            return R();
        }
        usleep(1000);
    }

    return result;
}

template <typename T, typename R>
Status AsyncKeyValueProducerAndConsumer<T, R>::ProduceLoop() {
    while (!consume_finished_) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortMergeReader::Iterator> iterator,
                               sort_merge_reader_->NextBatch());
        if (iterator == nullptr) {
            // all iterator is all visited
            kv_queue_.push(std::nullopt);
            break;
        }
        while (!consume_finished_) {
            PAIMON_ASSIGN_OR_RAISE(bool has_next, iterator->HasNext());
            if (!has_next) {
                // current iterator is all visited
                break;
            }
            std::optional<KeyValue> kv = std::move(iterator->Next());
            kv_queue_.push(std::move(kv));
        }
    }
    return Status::OK();
}

template <typename T, typename R>
void AsyncKeyValueProducerAndConsumer<T, R>::CleanUp() {
    consume_finished_ = true;
    next_batch_finished_ = true;
    CleanUpQueue();
    if (producer_future_.valid()) {
        [[maybe_unused]] Status status = producer_future_.get();
    }
    for (auto& consumer : consumers_) {
        consumer->CleanUp();
    }
    CleanUpQueue();
}

template <typename T, typename R>
void AsyncKeyValueProducerAndConsumer<T, R>::CleanUpQueue() {
    R read_batch;
    while (result_queue_.try_pop(read_batch)) {
        if constexpr (std::is_same_v<R, BatchReader::ReadBatch>) {
            if (!BatchReader::IsEofBatch(read_batch)) {
                ReaderUtils::ReleaseReadBatch(std::move(read_batch));
            }
        } else if constexpr (std::is_same_v<R, KeyValueBatch>) {
            if (read_batch.batch) {
                ArrowArrayRelease(read_batch.batch.get());
            }
        }
    }

    std::optional<KeyValue> kv;
    while (kv_queue_.try_pop(kv)) {
    }
}

template class AsyncKeyValueProducerAndConsumer<KeyValue, BatchReader::ReadBatch>;
template class AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch>;

template <typename T, typename R>
AsyncKeyValueConsumer<T, R>::AsyncKeyValueConsumer(
    int32_t batch_size, std::unique_ptr<RowToArrowArrayConverter<T, R>>&& key_value_consumer,
    std::atomic<bool>& consume_finished, std::atomic<int32_t>& consumer_finished_count,
    tbb::concurrent_bounded_queue<std::optional<KeyValue>>& kv_queue,
    tbb::concurrent_bounded_queue<R>& result_queue)
    : batch_size_(batch_size),
      key_value_consumer_(std::move(key_value_consumer)),
      consume_finished_(consume_finished),
      consumer_finished_count_(consumer_finished_count),
      kv_queue_(kv_queue),
      result_queue_(result_queue) {
    consumer_future_ =
        std::async(std::launch::async, &AsyncKeyValueConsumer<T, R>::ConsumeLoop, this).share();
}

template <typename T, typename R>
Status AsyncKeyValueConsumer<T, R>::GetStatus() const {
    // check consumer status
    std::chrono::microseconds span(0);
    std::future_status status = consumer_future_.wait_for(span);
    if (status == std::future_status::ready) {
        PAIMON_RETURN_NOT_OK(consumer_future_.get());
    }
    return Status::OK();
}

template <typename T, typename R>
Status AsyncKeyValueConsumer<T, R>::ConsumeLoop() {
    while (!consume_finished_) {
        int32_t cur_batch_size = 0;
        std::vector<KeyValue> key_value_vec;
        key_value_vec.reserve(batch_size_);
        while (!consume_finished_) {
            std::optional<KeyValue> kv;
            if (!kv_queue_.try_pop(kv)) {
                usleep(1);
                continue;
            }
            if (!kv) {
                consume_finished_ = true;
                break;
            }
            key_value_vec.push_back(std::move(kv).value());
            cur_batch_size++;
            if (cur_batch_size >= batch_size_) {
                break;
            }
        }

        if (cur_batch_size > 0) {
            PAIMON_ASSIGN_OR_RAISE(R result, key_value_consumer_->NextBatch(key_value_vec));
            result_queue_.push(std::move(result));
        }
    }
    consumer_finished_count_++;
    return Status::OK();
}

template <typename T, typename R>
void AsyncKeyValueConsumer<T, R>::CleanUp() {
    if (consumer_future_.valid()) {
        [[maybe_unused]] Status status = consumer_future_.get();
    }
    key_value_consumer_->CleanUp();
}

template class AsyncKeyValueConsumer<KeyValue, BatchReader::ReadBatch>;
template class AsyncKeyValueConsumer<KeyValue, KeyValueBatch>;

}  // namespace paimon
