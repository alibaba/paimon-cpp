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

#include "paimon/common/utils/arrow/arrow_memory_pool_adaptor.h"

#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::test {

TEST(MemUtilsTest, TestSimple) {
    const int64_t alignment = 64;
    auto pool = GetDefaultPool()->AsArrowMemoryPool();
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

}  // namespace paimon::test
