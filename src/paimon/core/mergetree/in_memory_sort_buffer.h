/*
 * Copyright 2026-present Alibaba Inc.
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
#include <string>
#include <vector>

#include "arrow/type_fwd.h"
#include "paimon/core/mergetree/sort_buffer.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class DataType;
class Schema;
class StructArray;
}  // namespace arrow

namespace paimon {
class FieldsComparator;
class KeyValueRecordReader;
class MemoryPool;

/// A buffered write batch that records the data and the first sequence number of the batch.
struct BufferedWriteBatch {
    int64_t first_sequence_number = 0;
    std::shared_ptr<arrow::StructArray> struct_array;
    std::vector<RecordBatch::RowKind> row_kinds;
};

/// Pure in-memory SortBuffer: buffers RecordBatches and exposes them as sorted
/// KeyValueRecordReaders. Does not support spill to disk.
class InMemorySortBuffer : public SortBuffer {
 public:
    InMemorySortBuffer(int64_t last_sequence_number,
                       const std::shared_ptr<arrow::DataType>& value_type,
                       const std::vector<std::string>& trimmed_primary_keys,
                       const std::vector<std::string>& user_defined_sequence_fields,
                       bool sequence_fields_ascending,
                       const std::shared_ptr<FieldsComparator>& key_comparator,
                       uint64_t write_buffer_size, const std::shared_ptr<MemoryPool>& pool);

    void Clear() override;
    uint64_t GetMemorySize() const override;
    Result<bool> FlushMemory() override;
    Result<bool> Write(std::unique_ptr<RecordBatch>&& batch) override;
    Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CreateReaders() override;
    bool HasData() const override;

    /// Estimate memory usage of an Arrow array.
    static Result<int64_t> EstimateMemoryUse(const std::shared_ptr<arrow::Array>& array);

 private:
    const std::shared_ptr<MemoryPool> pool_;
    const std::shared_ptr<arrow::DataType> value_type_;
    const std::vector<std::string> trimmed_primary_keys_;
    const std::vector<std::string> user_defined_sequence_fields_;
    const bool sequence_fields_ascending_;
    const std::shared_ptr<FieldsComparator> key_comparator_;
    const uint64_t write_buffer_size_;

    std::vector<BufferedWriteBatch> buffered_batches_;
    uint64_t current_memory_in_bytes_ = 0;
    int64_t next_sequence_number_ = 0;
};

}  // namespace paimon
