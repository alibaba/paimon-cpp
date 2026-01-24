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

#include "paimon/common/utils/arrow/mem_utils.h"

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

class ArrowMemPoolAdaptor : public arrow::MemoryPool {
 public:
    explicit ArrowMemPoolAdaptor(paimon::MemoryPool& pool) : pool_(pool) {}

    explicit ArrowMemPoolAdaptor(const std::shared_ptr<paimon::MemoryPool>& pool)
        : pool_(*pool), life_holder_(pool) {}

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        *out = reinterpret_cast<uint8_t*>(pool_.Malloc(size, alignment));
        stats_.DidAllocateBytes(size);
        return arrow::Status::OK();
    }

    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                             uint8_t** ptr) override {
        *ptr = reinterpret_cast<uint8_t*>(pool_.Realloc(*ptr, old_size, new_size, alignment));
        stats_.DidReallocateBytes(old_size, new_size);
        return arrow::Status::OK();
    }

    void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
        pool_.Free(buffer, size, alignment);
        stats_.DidFreeBytes(size);
    }

    int64_t bytes_allocated() const override {
        return stats_.bytes_allocated();
    }

    int64_t max_memory() const override {
        return stats_.max_memory();
    }

    std::string backend_name() const override {
        return "Paimon Pool";
    }

    /// The number of bytes that were allocated.
    int64_t total_bytes_allocated() const override {
        return stats_.total_bytes_allocated();
    }

    /// The number of allocations or reallocations that were requested.
    int64_t num_allocations() const override {
        return stats_.num_allocations();
    }

 private:
    paimon::MemoryPool& pool_;
    std::shared_ptr<paimon::MemoryPool> life_holder_;
    arrow::internal::MemoryPoolStats stats_;
};

std::unique_ptr<arrow::MemoryPool> GetArrowPool(MemoryPool& pool) {
    return std::make_unique<ArrowMemPoolAdaptor>(pool);
}

std::unique_ptr<arrow::MemoryPool> GetArrowPool(const std::shared_ptr<MemoryPool>& pool) {
    return std::make_unique<ArrowMemPoolAdaptor>(pool);
}

}  // namespace paimon
