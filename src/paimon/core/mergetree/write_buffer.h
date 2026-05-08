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
#include "paimon/core/mergetree/in_memory_sort_buffer.h"
#include "paimon/core/mergetree/sort_buffer.h"
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
    WriteBuffer(int64_t last_sequence_number, const std::shared_ptr<arrow::DataType>& value_type,
                const std::vector<std::string>& trimmed_primary_keys,
                const std::vector<std::string>& user_defined_sequence_fields,
                const std::shared_ptr<FieldsComparator>& key_comparator,
                const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
                const std::shared_ptr<MemoryPool>& pool);

    /// Import a RecordBatch into the buffer.
    /// Does NOT check memory thresholds or trigger flush.
    Status Write(std::unique_ptr<RecordBatch>&& batch);

    /// Create KeyValueRecordReaders from sort buffer without clearing the buffer.
    /// The caller should invoke Clear() after consuming the readers.
    /// @return list of KeyValueRecordReaders built from buffered data
    Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CreateReaders();

    /// Return current memory usage in bytes.
    uint64_t GetMemoryUsage() const {
        return sort_buffer_->GetMemorySize();
    }

    /// Return whether the buffer is empty.
    bool IsEmpty() const {
        return !sort_buffer_->HasData();
    }

    /// Clear the buffer without building readers (for error paths or Close).
    void Clear();

 private:
    std::unique_ptr<SortBuffer> sort_buffer_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
};

}  // namespace paimon
