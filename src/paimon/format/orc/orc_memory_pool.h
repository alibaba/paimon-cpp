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

#include <memory>

#include "orc/MemoryPool.hh"
#include "paimon/common/utils/concurrent_hash_map.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::orc {

class OrcMemoryPool : public ::orc::MemoryPool {
 public:
    using SizeType = uint64_t;
    explicit OrcMemoryPool(const std::shared_ptr<paimon::MemoryPool>& pool) : pool_(pool) {}
    char* malloc(SizeType size) override {
        if (size == 0) {
            return ZERO_SIZE_AREA;
        }
        if (size > std::numeric_limits<SizeType>::max() - HEADER_SIZE) {
            return nullptr;
        }
        if (void* ret = pool_->Malloc(size + HEADER_SIZE)) {
            *reinterpret_cast<SizeType*>(ret) = size;
            return reinterpret_cast<char*>(ret) + HEADER_SIZE;
        }
        return nullptr;
    }
    void free(char* p) override {
        if (p == nullptr || p == ZERO_SIZE_AREA) {
            return;
        }
        char* raw = p - HEADER_SIZE;
        SizeType size = *reinterpret_cast<SizeType*>(raw);
        pool_->Free(raw, size + HEADER_SIZE);
    }

 private:
    static constexpr size_t ALIGNMENT = 64;
    static constexpr size_t HEADER_SIZE = (sizeof(SizeType) + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
    alignas(ALIGNMENT) inline static char ZERO_SIZE_AREA[1];

    std::shared_ptr<paimon::MemoryPool> pool_;
};

}  // namespace paimon::orc
