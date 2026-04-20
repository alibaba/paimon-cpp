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

#include "paimon/memory/memory_pool.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>

namespace paimon {

class MemoryPoolImpl : public MemoryPool {
 public:
    MemoryPoolImpl() = default;
    ~MemoryPoolImpl() override = default;

    void* Malloc(uint64_t size, uint64_t alignment) override;
    void* Realloc(void* p, size_t old_size, size_t new_size, uint64_t alignment) override;
    void Free(void* p, uint64_t size) override;
    uint64_t CurrentUsage() const override;
    uint64_t MaxMemoryUsage() const override {
        return max_allocated.load();
    }

 protected:
    std::atomic<int64_t> total_allocated_size = {0};
    std::atomic<int64_t> max_allocated = {0};
};

void* MemoryPoolImpl::Malloc(uint64_t size, uint64_t alignment) {
    void* memptr = nullptr;
#define DEFAULT_ALIGNMENT 64
    if (posix_memalign(reinterpret_cast<void**>(&memptr),
                       alignment == 0 ? DEFAULT_ALIGNMENT : alignment, size) != 0) {
        throw std::bad_alloc();
    }
    total_allocated_size.fetch_add(size);
    max_allocated.store(std::max(total_allocated_size.load(), max_allocated.load()));
    return memptr;
}

void* MemoryPoolImpl::Realloc(void* p, size_t old_size, size_t new_size, uint64_t alignment) {
    if (alignment == 0) {
        void* memptr = ::realloc(p, new_size);
        total_allocated_size.fetch_add(new_size - old_size);
        max_allocated.store(std::max(total_allocated_size.load(), max_allocated.load()));
        return memptr;
    } else {
        if (p == nullptr) {
            return Malloc(new_size, alignment);
        } else if (new_size == old_size) {
            return p;
        } else if (new_size == 0) {
            Free(p, old_size);
            return Malloc(0, alignment);
        } else if (new_size < old_size && old_size / 2 < new_size) {
            total_allocated_size.fetch_add(new_size - old_size);
            max_allocated.store(std::max(total_allocated_size.load(), max_allocated.load()));
            // do not shrink to fit, when new size is not very small, to avoid memory copy
            return p;
        } else {
            void* memptr = Malloc(new_size, alignment);
            memcpy(memptr, p, std::min(old_size, new_size));
            Free(p, old_size);
            return memptr;
        }
    }
}

void MemoryPoolImpl::Free(void* p, uint64_t size) {
    std::free(p);
    total_allocated_size.fetch_sub(size);
}

uint64_t MemoryPoolImpl::CurrentUsage() const {
    return total_allocated_size.load();
}

PAIMON_EXPORT std::shared_ptr<MemoryPool> GetDefaultPool() {
    static std::shared_ptr<MemoryPool> internal = std::make_shared<MemoryPoolImpl>();
    return internal;
}

PAIMON_EXPORT std::unique_ptr<MemoryPool> GetMemoryPool() {
    return std::make_unique<MemoryPoolImpl>();
}

}  // namespace paimon
