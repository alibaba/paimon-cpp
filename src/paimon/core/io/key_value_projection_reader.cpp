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

#include "paimon/core/io/key_value_projection_reader.h"

#include <cassert>
#include <utility>

#include "arrow/c/abi.h"
#include "paimon/core/io/key_value_projection_consumer.h"
#include "paimon/core/key_value.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class MemoryPool;

Result<std::unique_ptr<KeyValueProjectionReader>> KeyValueProjectionReader::Create(
    std::unique_ptr<SortMergeReader>&& sort_merge_reader,
    const std::shared_ptr<arrow::Schema>& target_schema,
    const std::vector<int32_t>& target_to_src_mapping, int32_t batch_size,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    std::unique_ptr<RowToArrowArrayConverter<KeyValue, BatchReader::ReadBatch>> projection_consumer;
    PAIMON_ASSIGN_OR_RAISE(
        projection_consumer,
        KeyValueProjectionConsumer::Create(target_schema, target_to_src_mapping, arrow_pool));
    return std::unique_ptr<KeyValueProjectionReader>(new KeyValueProjectionReader(
        batch_size, std::move(sort_merge_reader), std::move(projection_consumer)));
}

KeyValueProjectionReader::KeyValueProjectionReader(
    int32_t batch_size, std::unique_ptr<SortMergeReader>&& sort_merge_reader,
    std::unique_ptr<RowToArrowArrayConverter<KeyValue, BatchReader::ReadBatch>>&&
        projection_consumer)
    : batch_size_(batch_size),
      sort_merge_reader_(std::move(sort_merge_reader)),
      key_value_consumer_(std::move(projection_consumer)) {}

Result<BatchReader::ReadBatch> KeyValueProjectionReader::NextBatch() {
    int32_t cur_batch_size = 0;
    std::vector<KeyValue> key_value_vec;
    key_value_vec.reserve(batch_size_);
    while (!read_finished_ && cur_batch_size < batch_size_) {
        if (iterator_ == nullptr) {
            PAIMON_ASSIGN_OR_RAISE(iterator_, sort_merge_reader_->NextBatch());
            if (iterator_ == nullptr) {
                // read eof
                read_finished_ = true;
                break;
            }
        }
        PAIMON_ASSIGN_OR_RAISE(bool has_next, iterator_->HasNext());
        if (!has_next) {
            // current iterator is all visited
            iterator_ = nullptr;
            continue;
        }
        key_value_vec.push_back(std::move(iterator_->Next()));
        cur_batch_size++;
    }
    if (cur_batch_size > 0) {
        return key_value_consumer_->NextBatch(key_value_vec);
    } else {
        assert(read_finished_);
        return BatchReader::MakeEofBatch();
    }
}

}  // namespace paimon
