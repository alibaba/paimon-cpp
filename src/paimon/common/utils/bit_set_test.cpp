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

#include "paimon/common/utils/bit_set.h"

#include <cstring>
#include <limits>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(BitSetTest, TestBitSet) {
    auto bit_set = std::make_shared<BitSet>(1024);
    auto pool = GetDefaultPool();
    auto seg = MemorySegment::AllocateHeapMemory(1024, pool.get());
    ASSERT_OK(bit_set->SetMemorySegment(seg));
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_OK(bit_set->Set(i * 2 + 1));
    }
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_TRUE(bit_set->Get(i * 2 + 1));
    }
    bit_set->Clear();
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_FALSE(bit_set->Get(i * 2 + 1));
    }
}

TEST(BitSetTest, TestGetOutOfBound) {
    auto pool = GetDefaultPool();
    // 2 bytes = 16 bits
    auto bit_set = std::make_shared<BitSet>(2);
    auto seg = MemorySegment::AllocateHeapMemory(2, pool.get());
    ASSERT_OK(bit_set->SetMemorySegment(seg));

    ASSERT_OK(bit_set->Set(0));
    ASSERT_OK(bit_set->Set(15));
    ASSERT_TRUE(bit_set->Get(0));
    ASSERT_TRUE(bit_set->Get(15));

    // Out-of-bound access should return false (bit_size_ = 16)
    ASSERT_FALSE(bit_set->Get(16));
    ASSERT_FALSE(bit_set->Get(100));
    ASSERT_FALSE(bit_set->Get(std::numeric_limits<uint32_t>::max()));

    // Set out-of-bound should return error
    ASSERT_NOK_WITH_MSG(bit_set->Set(16), "Index out of bound");
}

TEST(BitSetTest, TestClearNonAligned) {
    auto pool = GetDefaultPool();
    // 5 bytes = 40 bits, not a multiple of 8 bytes,
    // exercises both the 8-byte loop (never enters) and the tail byte-by-byte loop
    auto bit_set = std::make_shared<BitSet>(5);
    auto seg = MemorySegment::AllocateHeapMemory(8, pool.get());
    ASSERT_OK(bit_set->SetMemorySegment(seg));

    // Set every bit
    for (uint32_t i = 0; i < 40; i++) {
        ASSERT_OK(bit_set->Set(i));
    }
    for (uint32_t i = 0; i < 40; i++) {
        ASSERT_TRUE(bit_set->Get(i));
    }

    // Clear and verify all bits are unset
    bit_set->Clear();
    for (uint32_t i = 0; i < 40; i++) {
        ASSERT_FALSE(bit_set->Get(i));
    }

    // 16 bytes = 128 bits, exact multiple of 8 bytes, exercises the 8-byte loop + tail loop
    auto bit_set_aligned = std::make_shared<BitSet>(16);
    auto seg2 = MemorySegment::AllocateHeapMemory(16, pool.get());
    ASSERT_OK(bit_set_aligned->SetMemorySegment(seg2));

    for (uint32_t i = 0; i < 128; i++) {
        ASSERT_OK(bit_set_aligned->Set(i));
    }
    bit_set_aligned->Clear();
    for (uint32_t i = 0; i < 128; i++) {
        ASSERT_FALSE(bit_set_aligned->Get(i));
    }
}
}  // namespace paimon::test
