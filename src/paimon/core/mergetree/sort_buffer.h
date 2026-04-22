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
#include "paimon/status.h"

namespace paimon {
class KeyValueRecordReader;

/// SortBuffer is the interface for managing buffered records with sorting capability.
/// It abstracts the in-memory and external (spill-to-disk) sort buffer implementations.
class SortBuffer {
 public:
    virtual ~SortBuffer() = default;

    /// Return the number of records in the buffer.
    virtual int64_t Size() const = 0;

    /// Clear all buffered data: both in-memory batches and on-disk spill files.
    /// Called after FlushWriteBuffer writes out the final data files (regardless of success or
    /// failure) to completely reset the buffer state.
    virtual Status Clear() = 0;

    /// Return the current memory usage in bytes.
    virtual uint64_t GetMemorySize() const = 0;

    /// Try to flush in-memory data to disk.
    /// @return true if memory was successfully released; false if not supported or unable to spill
    /// (caller should fall back to FlushWriteBuffer).
    virtual Result<bool> FlushMemory() = 0;

    /// Write a RecordBatch into the buffer.
    /// @return true if the write succeeded and the buffer is not full; false if the buffer is full
    /// (caller should fall back to FlushWriteBuffer).
    virtual Result<bool> Write(std::unique_ptr<RecordBatch>&& batch) = 0;

    /// Drain all buffered data (in-memory + on-disk) as sorted KeyValueRecordReaders.
    /// This only transfers data ownership to the returned readers without clearing the buffer.
    /// The caller should invoke Clear() after consuming the readers.
    virtual Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> SortedIterator() = 0;

    /// Return true if there is any data to output (in-memory or on-disk).
    virtual bool HasData() const = 0;
};

}  // namespace paimon
