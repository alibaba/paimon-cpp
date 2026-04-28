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

#include "paimon/core/mergetree/writer_memory_manager.h"

#include <cassert>

#include "fmt/format.h"
#include "paimon/core/utils/batch_writer.h"

namespace paimon {

void WriterMemoryManager::RegisterWriter(BatchWriter* writer) {
    UpdateWriterMemory(writer);
}

void WriterMemoryManager::UnregisterWriter(BatchWriter* writer) {
    auto iter = writer_memory_.find(writer);
    if (iter == writer_memory_.end()) {
        return;
    }

    assert(total_memory_ >= iter->second);
    total_memory_ -= iter->second;
    writer_memory_.erase(iter);
}

void WriterMemoryManager::RefreshWriterMemory(BatchWriter* writer) {
    UpdateWriterMemory(writer);
}

Status WriterMemoryManager::OnWriteCompleted(BatchWriter* writer) {
    UpdateWriterMemory(writer);
    if (total_memory_ < max_memory_) {
        return Status::OK();
    }

    return ShrinkToLimit();
}

void WriterMemoryManager::UpdateWriterMemory(BatchWriter* writer) {
    uint64_t current_memory_usage = writer->GetMemoryUsage();
    auto [iter, inserted] = writer_memory_.emplace(writer, current_memory_usage);
    if (inserted) {
        total_memory_ += current_memory_usage;
    } else {
        uint64_t previous_memory = iter->second;
        if (current_memory_usage >= previous_memory) {
            total_memory_ += (current_memory_usage - previous_memory);
        } else {
            assert(total_memory_ >= previous_memory - current_memory_usage);
            total_memory_ -= (previous_memory - current_memory_usage);
        }
        iter->second = current_memory_usage;
    }
}

WriterMemoryManager::Candidate WriterMemoryManager::PickLargest(
    const std::unordered_set<BatchWriter*>& skipped) const {
    Candidate candidate;
    for (const auto& [writer, memory] : writer_memory_) {
        if (skipped.find(writer) != skipped.end()) {
            continue;
        }
        if (memory > candidate.memory) {
            candidate = {writer, memory};
        }
    }
    return candidate;
}

Status WriterMemoryManager::ShrinkToLimit() {
    std::unordered_set<BatchWriter*> no_progress_writers;
    while (true) {
        if (total_memory_ < max_memory_) {
            return Status::OK();
        }

        Candidate picked = PickLargest(no_progress_writers);
        if (picked.memory == 0) {
            return Status::Invalid(
                fmt::format("Unable to release memory to below the write-buffer-size limit ({} "
                            "bytes), this might be a bug.",
                            max_memory_));
        }

        BatchWriter* candidate = picked.writer;
        uint64_t before_memory = picked.memory;

        PAIMON_RETURN_NOT_OK(candidate->FlushMemory());

        UpdateWriterMemory(candidate);

        uint64_t after_memory = candidate->GetMemoryUsage();

        if (after_memory >= before_memory) {
            no_progress_writers.insert(candidate);
        } else {
            no_progress_writers.erase(candidate);
        }
    }
}

}  // namespace paimon
