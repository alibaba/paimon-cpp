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
    auto ptr = std::make_shared<MemorySegment>(seg);
    ASSERT_OK(bit_set->SetMemorySegment(ptr));
    for (int i = 0; i < 100; i++) {
        ASSERT_OK(bit_set->Set(i * 2 + 1));
    }
    for (int i = 0; i < 100; i++) {
        ASSERT_TRUE(bit_set->Get(i * 2 + 1));
    }
    bit_set->Clear();
    for (int i = 0; i < 100; i++) {
        ASSERT_FALSE(bit_set->Get(i * 2 + 1));
    }
}

}  // namespace paimon::test
