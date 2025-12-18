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
    explicit OrcMemoryPool(const std::shared_ptr<paimon::MemoryPool>& pool) : pool_(pool) {}
    char* malloc(uint64_t size) override {
        char* ret = reinterpret_cast<char*>(pool_->Malloc(size));
        alloc_map_.Insert(reinterpret_cast<size_t>(ret), size);
        return ret;
    }
    void free(char* p) override {
        std::optional<size_t> size = alloc_map_.Find(reinterpret_cast<size_t>(p));
        if (size) {
            pool_->Free(p, size.value());
            alloc_map_.Erase(reinterpret_cast<size_t>(p));
        } else {
            assert(false);
            pool_->Free(p, /*size=*/0);
        }
    }

 private:
    ConcurrentHashMap<size_t, uint64_t> alloc_map_;
    std::shared_ptr<paimon::MemoryPool> pool_;
};

}  // namespace paimon::orc
