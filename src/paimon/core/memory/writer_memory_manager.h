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
#include <unordered_map>

#include "paimon/status.h"

namespace paimon {

class BatchWriter;

/// Coordinates global write-buffer memory across managed writers. Used in `AbstractFileStoreWrite`.
/// @note This class is not thread-safe.
class WriterMemoryManager {
 public:
    explicit WriterMemoryManager(uint64_t memory_limit) : memory_limit_(memory_limit) {}

    /// Register a writer when create a new `BatchWriter` in `AbstractFileStoreWrite::GetWriter()`
    void RegisterWriter(BatchWriter* writer);
    /// Unregister a writer when the `BatchWriter` has been erased in `AbstractFileStoreWrite`
    void UnregisterWriter(BatchWriter* writer);
    /// Refresh the memory usage of a writer when after `BatchWriter::PrepareCommit()`
    void RefreshWriterMemory(BatchWriter* writer);
    /// Check if the total memory usage exceeds the limit after `BatchWriter::Write()`, and trigger
    /// flush if needed.
    Status OnWriteCompleted(BatchWriter* writer);

 private:
    struct Candidate {
        BatchWriter* writer = nullptr;
        uint64_t memory = 0;
    };

    void UpdateWriterMemory(BatchWriter* writer);
    Candidate PickLargest() const;
    Status ShrinkToLimit();

    const uint64_t memory_limit_;
    uint64_t total_memory_ = 0;
    std::unordered_map<BatchWriter*, uint64_t> writer_memory_;
};

}  // namespace paimon
