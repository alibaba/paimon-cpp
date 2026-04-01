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
#include <string>
#include <vector>

#include "arrow/type_fwd.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Array;
class DataType;
class StructArray;
}  // namespace arrow

namespace paimon {

class KeyValueRecordReader;
class FieldsComparator;
class MemoryPool;
struct KeyValue;
template <typename T>
class MergeFunctionWrapper;

/// WriteBuffer manages the in-memory batch buffer for MergeTreeWriter.
/// It is responsible for importing Arrow data, estimating memory usage,
/// and flushing buffered batches into KeyValueRecordReaders.
class WriteBuffer {
 public:
    WriteBuffer(const std::shared_ptr<arrow::DataType>& value_type,
                const std::vector<std::string>& trimmed_primary_keys,
                const std::vector<std::string>& user_defined_sequence_fields,
                const std::shared_ptr<FieldsComparator>& key_comparator,
                const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
                const std::shared_ptr<MemoryPool>& pool);

    /// Import a RecordBatch into the buffer.
    /// Performs: ImportArray → StructArray → EstimateMemoryUse → append to buffer.
    /// Does NOT check memory thresholds or trigger flush.
    Status Write(std::unique_ptr<RecordBatch>&& batch);

    /// Flush all buffered batches into KeyValueInMemoryRecordReaders and clear the buffer.
    /// @param[in,out] last_sequence_number current sequence number, updated after flush
    /// @return list of KeyValueRecordReaders built from buffered data
    Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> Flush(
        int64_t& last_sequence_number);

    /// Return current memory usage in bytes.
    int64_t GetMemoryUsage() const { return current_memory_in_bytes_; }

    /// Return whether the buffer is empty.
    bool IsEmpty() const { return batch_vec_.empty(); }

    /// Return the number of batches in the buffer.
    size_t GetBatchCount() const { return batch_vec_.size(); }

    /// Clear the buffer without building readers (for error paths or Close).
    void Clear();

    /// Estimate memory usage of an Arrow Array.
    static Result<int64_t> EstimateMemoryUse(const std::shared_ptr<arrow::Array>& array);

 private:
    // Immutable configuration
    std::shared_ptr<arrow::DataType> value_type_;
    std::vector<std::string> trimmed_primary_keys_;
    std::vector<std::string> user_defined_sequence_fields_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
    std::shared_ptr<MemoryPool> pool_;

    // Mutable buffer state
    std::vector<std::shared_ptr<arrow::StructArray>> batch_vec_;
    std::vector<std::vector<RecordBatch::RowKind>> row_kinds_vec_;
    int64_t current_memory_in_bytes_ = 0;
};

}  // namespace paimon
