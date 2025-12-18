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

#include <memory>
#include <utility>
#include <vector>

#include "paimon/core/io/async_key_value_producer_and_consumer.h"
#include "paimon/core/io/key_value_projection_consumer.h"
#include "paimon/reader/batch_reader.h"

namespace paimon {
class AsyncKeyValueProjectionReader : public BatchReader {
 public:
    AsyncKeyValueProjectionReader(std::unique_ptr<SortMergeReader>&& sort_merge_reader,
                                  const std::shared_ptr<arrow::Schema>& target_schema,
                                  const std::vector<int32_t>& target_to_src_mapping,
                                  int32_t batch_size, int32_t projection_thread_num,
                                  const std::shared_ptr<MemoryPool>& pool) {
        auto create_consumer = [target_schema, target_to_src_mapping, pool]()
            -> Result<std::unique_ptr<RowToArrowArrayConverter<KeyValue, BatchReader::ReadBatch>>> {
            return KeyValueProjectionConsumer::Create(target_schema, target_to_src_mapping, pool);
        };
        producer_and_consumer_ =
            std::make_unique<AsyncKeyValueProducerAndConsumer<KeyValue, BatchReader::ReadBatch>>(
                std::move(sort_merge_reader), create_consumer, batch_size, projection_thread_num,
                pool);
    }

    Result<BatchReader::ReadBatch> NextBatch() override {
        return producer_and_consumer_->NextBatch();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return producer_and_consumer_->GetReaderMetrics();
    }

    void Close() override {
        producer_and_consumer_->Close();
    }

 private:
    std::unique_ptr<AsyncKeyValueProducerAndConsumer<KeyValue, BatchReader::ReadBatch>>
        producer_and_consumer_;
};

}  // namespace paimon
