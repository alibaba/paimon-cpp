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
#include <memory>
#include <vector>

#include "arrow/api.h"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/core/mergetree/compact/sort_merge_reader.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace arrow {
class MemoryPool;
class Schema;
}  // namespace arrow

namespace paimon {
class MemoryPool;
class Metrics;
struct KeyValue;

// Serial iterate KeyValue from SortMergeReader and convert KeyValue to arrow array
class KeyValueProjectionReader : public BatchReader {
 public:
    static Result<std::unique_ptr<KeyValueProjectionReader>> Create(
        std::unique_ptr<SortMergeReader>&& sort_merge_reader,
        const std::shared_ptr<arrow::Schema>& target_schema,
        const std::vector<int32_t>& target_to_src_mapping, int32_t batch_size,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    Result<ReadBatch> NextBatch() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return sort_merge_reader_->GetReaderMetrics();
    }

    void Close() override {
        iterator_.reset();
        key_value_consumer_->CleanUp();
        sort_merge_reader_->Close();
    }

 private:
    KeyValueProjectionReader(
        int32_t batch_size, std::unique_ptr<SortMergeReader>&& sort_merge_reader,
        std::unique_ptr<RowToArrowArrayConverter<KeyValue, BatchReader::ReadBatch>>&&
            projection_consumer);

 private:
    int32_t batch_size_;
    bool read_finished_ = false;
    std::unique_ptr<SortMergeReader> sort_merge_reader_;
    std::unique_ptr<SortMergeReader::Iterator> iterator_;
    std::unique_ptr<RowToArrowArrayConverter<KeyValue, BatchReader::ReadBatch>> key_value_consumer_;
};
}  // namespace paimon
