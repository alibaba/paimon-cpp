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
#include <memory_resource>

#include "paimon/memory/memory_pool.h"
namespace paimon::lumina {
class LuminaMemoryPool : public std::pmr::memory_resource {
 public:
    explicit LuminaMemoryPool(const std::shared_ptr<MemoryPool>& pool) : pool_(pool) {}

    ~LuminaMemoryPool() override = default;

    LuminaMemoryPool(const LuminaMemoryPool&) = delete;
    LuminaMemoryPool& operator=(const LuminaMemoryPool&) = delete;

    std::shared_ptr<MemoryPool> GetPaimonPool() const {
        return pool_;
    }

 private:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        static const size_t min_alignment = alignof(std::max_align_t);
        if (alignment < min_alignment) {
            alignment = min_alignment;
        }
        return pool_->Malloc(bytes, alignment);
    }

    void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
        pool_->Free(p, bytes, alignment);
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
        return this == &other;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};
}  // namespace paimon::lumina
