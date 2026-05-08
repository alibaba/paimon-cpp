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
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/in_memory_sort_buffer.h"
#include "paimon/core/mergetree/sort_buffer.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Array;
class DataType;
class Schema;
class StructArray;
}  // namespace arrow

namespace paimon {
class IOManager;
class KeyValueRecordReader;
class FieldsComparator;
class MemoryPool;
struct KeyValue;
template <typename T>
class MergeFunctionWrapper;

/// WriteBuffer manages the batch buffer for MergeTreeWriter.
/// It delegates to a SortBuffer implementation (InMemorySortBuffer or ExternalSortBuffer) based on
/// the spillable configuration.
class WriteBuffer {
 public:
    static Result<std::unique_ptr<WriteBuffer>> Create(
        int64_t last_sequence_number, const std::shared_ptr<arrow::Schema>& value_schema,
        const std::vector<std::string>& trimmed_primary_keys,
        const std::vector<std::string>& user_defined_sequence_fields,
        const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
        const CoreOptions& options, const std::shared_ptr<IOManager>& io_manager,
        const std::shared_ptr<MemoryPool>& pool);

    /// Import a RecordBatch into the buffer.
    /// Return false when the batch was accepted but the caller should fall back to
    /// FlushWriteBuffer before buffering more data.
    Result<bool> Write(std::unique_ptr<RecordBatch>&& batch);

    /// Create KeyValueRecordReaders from sort buffer without clearing the buffer.
    /// The caller should invoke Clear() after consuming the readers.
    /// @return list of KeyValueRecordReaders built from buffered data
    Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CreateReaders();

    /// Try to spill current buffered data. Return false when the call completed normally but the
    /// caller should fall back to FlushWriteBuffer before buffering more data.
    Result<bool> FlushMemory();

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
    WriteBuffer(std::unique_ptr<SortBuffer>&& sort_buffer,
                const std::shared_ptr<FieldsComparator>& key_comparator,
                const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper);

    std::unique_ptr<SortBuffer> sort_buffer_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
};

}  // namespace paimon
