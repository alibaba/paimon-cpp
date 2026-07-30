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
#include <new>

#include "arrow/status.h"
#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::test {
namespace {

// A MemoryPool whose allocations always fail, either by returning nullptr or by
// throwing std::bad_alloc, so the adaptor's out-of-memory paths can be exercised.
class FailingMemoryPool : public MemoryPool {
 public:
    enum class Mode { kReturnNull, kThrowBadAlloc };

    explicit FailingMemoryPool(Mode mode) : mode_(mode) {}

    void* Malloc(uint64_t /*size*/, uint64_t /*alignment*/ = 0) override {
        if (mode_ == Mode::kThrowBadAlloc) {
            throw std::bad_alloc();
        }
        return nullptr;
    }

    void* Realloc(void* /*p*/, size_t /*old_size*/, size_t /*new_size*/,
                  uint64_t /*alignment*/ = 0) override {
        if (mode_ == Mode::kThrowBadAlloc) {
            throw std::bad_alloc();
        }
        return nullptr;
    }

    void Free(void* /*p*/, uint64_t /*size*/) override {}
    uint64_t CurrentUsage() const override {
        return 0;
    }
    uint64_t MaxMemoryUsage() const override {
        return 0;
    }

 private:
    Mode mode_;
};

}  // namespace

TEST(MemUtilsTest, TestSimple) {
    const int64_t alignment = 64;
    auto pool = GetArrowPool(GetDefaultPool());
    ASSERT_EQ("Paimon Pool", pool->backend_name());
    ASSERT_EQ(0, pool->total_bytes_allocated());
    ASSERT_EQ(0, pool->num_allocations());

    uint8_t* ptr1 = nullptr;
    ASSERT_TRUE(pool->Allocate(10, alignment, &ptr1).ok());
    ASSERT_TRUE(ptr1);
    ASSERT_EQ(10, pool->total_bytes_allocated());
    ASSERT_EQ(10, pool->bytes_allocated());
    ASSERT_EQ(10, pool->max_memory());
    ASSERT_EQ(1, pool->num_allocations());

    // test malloc and free
    uint8_t* ptr2 = nullptr;
    ASSERT_TRUE(pool->Allocate(20, alignment, &ptr2).ok());
    ASSERT_TRUE(ptr2);
    ASSERT_EQ(30, pool->bytes_allocated());
    ASSERT_EQ(30, pool->max_memory());
    pool->Free(ptr2, 20, alignment);
    ASSERT_EQ(10, pool->bytes_allocated());
    ASSERT_EQ(30, pool->max_memory());
    ASSERT_EQ(2, pool->num_allocations());

    // test realloc with nullptr
    uint8_t* ptr3 = nullptr;
    ASSERT_TRUE(pool->Reallocate(/*old_size=*/0, /*new_size=*/40, alignment, &ptr3).ok());
    ASSERT_TRUE(ptr3);
    ASSERT_EQ(50, pool->bytes_allocated());
    ASSERT_EQ(50, pool->max_memory());
    ASSERT_EQ(3, pool->num_allocations());

    uint8_t* ptr3_old = ptr3;
    // test realloc with same size
    ASSERT_TRUE(pool->Reallocate(/*old_size=*/40, /*new_size=*/40, alignment, &ptr3).ok());
    ASSERT_EQ(ptr3_old, ptr3);
    ASSERT_EQ(50, pool->bytes_allocated());
    ASSERT_EQ(50, pool->max_memory());
    ASSERT_EQ(3, pool->num_allocations());

    pool->Free(ptr1, 10, alignment);
    pool->Free(ptr3, 40, alignment);
    ASSERT_EQ(0, pool->bytes_allocated());
    ASSERT_EQ(70, pool->total_bytes_allocated());
    ASSERT_EQ(3, pool->num_allocations());
    ASSERT_EQ(50, pool->max_memory());
}

TEST(MemUtilsTest, TestAllocateOutOfMemory) {
    uint8_t* ptr = nullptr;

    // Underlying pool returns nullptr for a positive size.
    auto null_pool =
        GetArrowPool(std::make_shared<FailingMemoryPool>(FailingMemoryPool::Mode::kReturnNull));
    ASSERT_TRUE(null_pool->Allocate(16, 64, &ptr).IsOutOfMemory());

    // Underlying pool throws std::bad_alloc.
    auto throw_pool =
        GetArrowPool(std::make_shared<FailingMemoryPool>(FailingMemoryPool::Mode::kThrowBadAlloc));
    ASSERT_TRUE(throw_pool->Allocate(16, 64, &ptr).IsOutOfMemory());
}

TEST(MemUtilsTest, TestReallocateOutOfMemory) {
    uint8_t* ptr = nullptr;

    // Underlying pool returns nullptr for a positive new size.
    auto null_pool =
        GetArrowPool(std::make_shared<FailingMemoryPool>(FailingMemoryPool::Mode::kReturnNull));
    ASSERT_TRUE(null_pool->Reallocate(/*old_size=*/0, /*new_size=*/16, 64, &ptr).IsOutOfMemory());

    // Underlying pool throws std::bad_alloc.
    auto throw_pool =
        GetArrowPool(std::make_shared<FailingMemoryPool>(FailingMemoryPool::Mode::kThrowBadAlloc));
    ASSERT_TRUE(throw_pool->Reallocate(/*old_size=*/0, /*new_size=*/16, 64, &ptr).IsOutOfMemory());
}

}  // namespace paimon::test
