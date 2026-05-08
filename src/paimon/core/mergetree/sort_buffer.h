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
#include <vector>

#include "paimon/record_batch.h"
#include "paimon/result.h"

namespace paimon {
class KeyValueRecordReader;

/// SortBuffer is the interface for managing buffered records with sorting capability.
/// It abstracts the in-memory and external sort buffer implementations.
class SortBuffer {
 public:
    virtual ~SortBuffer() = default;

    /// Reset the buffer, releasing all in-memory batches and on-disk spill files.
    virtual void Clear() = 0;

    /// Return the current memory usage in bytes.
    virtual uint64_t GetMemorySize() const = 0;

    /// Spill in-memory data to disk if supported by this implementation.
    /// @return true for FlushMemory success with the buffer can accept more data afterwards.
    ///         false for FlushMemory success with there is no more quota for next flush. (on-disk)
    ///         false for the FlushMemory operation is not supported. (in-memory only)
    ///         Status::Invalid for flush failure.
    virtual Result<bool> FlushMemory() = 0;

    /// Append a RecordBatch to the buffer.
    /// @return true for write success with the buffer can accept more data afterwards.
    ///         false for write success with the buffer is no more quota (memory or disk) for next
    ///         write.
    ///         Status::Invalid for write failure.
    virtual Result<bool> Write(std::unique_ptr<RecordBatch>&& batch) = 0;

    /// Create sorted KeyValueRecordReaders from all buffered data (in-memory + on-disk).
    /// This does not clear the buffer; the caller should invoke Clear() afterwards.
    virtual Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CreateReaders() = 0;

    /// Return true if there is any data to output (in-memory or on-disk).
    virtual bool HasData() const = 0;
};

}  // namespace paimon
