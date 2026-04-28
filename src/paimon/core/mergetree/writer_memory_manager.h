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
#include <unordered_map>
#include <unordered_set>

#include "paimon/status.h"

namespace paimon {

class BatchWriter;

/// Coordinates global write-buffer memory across managed writers.
/// NOTE: This class is not thread-safe.
class WriterMemoryManager {
 public:
    explicit WriterMemoryManager(uint64_t max_memory) : max_memory_(max_memory) {}

    void RegisterWriter(BatchWriter* writer);
    void UnregisterWriter(BatchWriter* writer);
    void RefreshWriterMemory(BatchWriter* writer);

    Status OnWriteCompleted(BatchWriter* writer);

 private:
    struct Candidate {
        BatchWriter* writer = nullptr;
        uint64_t memory = 0;
    };

    void UpdateWriterMemory(BatchWriter* writer);
    Candidate PickLargest(const std::unordered_set<BatchWriter*>& skipped) const;
    // XXX: for debug, remove it
    uint64_t ComputeTotalMemoryForDebug() const;
    Status ShrinkToLimit();

    uint64_t max_memory_;
    uint64_t total_memory_ = 0;
    std::unordered_map<BatchWriter*, uint64_t> writer_memory_;
};

}  // namespace paimon
