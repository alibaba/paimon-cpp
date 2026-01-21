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

#include "paimon/common/utils/bloom_filter.h"

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

TEST(BloomFilterTest, TestOneSegmentBuilder) {
    int32_t items = 100;
    auto pool = GetDefaultPool();
    auto bloom_filter = BloomFilter::Create(items, 0.01);
    auto seg = MemorySegment::AllocateHeapMemory(1024, pool.get());
    auto ptr = std::make_shared<MemorySegment>(seg);
    ASSERT_OK(bloom_filter->SetMemorySegment(ptr));

    std::mt19937_64 engine(std::random_device{}());  // NOLINT(whitespace/braces)
    std::uniform_int_distribution<int32_t> distribution(0, items);
    std::set<int32_t> test_data;
    for (int32_t i = 0; i < items; i++) {
        int32_t random = distribution(engine);
        test_data.insert(random);
        ASSERT_OK(bloom_filter->AddHash(random));
    }

    for (const auto& value : test_data) {
        ASSERT_TRUE(bloom_filter->TestHash(value));
    }
}

TEST(BloomFilterTest, TestEstimatedHashFunctions) {
    ASSERT_EQ(7, BloomFilter::Create(1000, 0.01)->GetNumHashFunctions());
    ASSERT_EQ(7, BloomFilter::Create(10000, 0.01)->GetNumHashFunctions());
    ASSERT_EQ(7, BloomFilter::Create(100000, 0.01)->GetNumHashFunctions());
    ASSERT_EQ(4, BloomFilter::Create(100000, 0.05)->GetNumHashFunctions());
    ASSERT_EQ(7, BloomFilter::Create(1000000, 0.01)->GetNumHashFunctions());
    ASSERT_EQ(4, BloomFilter::Create(1000000, 0.05)->GetNumHashFunctions());
}

TEST(BloomFilterTest, TestBloomNumBits) {
    ASSERT_EQ(0, BloomFilter::OptimalNumOfBits(0, 0));
    ASSERT_EQ(0, BloomFilter::OptimalNumOfBits(0, 1));
    ASSERT_EQ(0, BloomFilter::OptimalNumOfBits(1, 1));
    ASSERT_EQ(7, BloomFilter::OptimalNumOfBits(1, 0.03));
    ASSERT_EQ(72, BloomFilter::OptimalNumOfBits(10, 0.03));
    ASSERT_EQ(729, BloomFilter::OptimalNumOfBits(100, 0.03));
    ASSERT_EQ(7298, BloomFilter::OptimalNumOfBits(1000, 0.03));
    ASSERT_EQ(72984, BloomFilter::OptimalNumOfBits(10000, 0.03));
    ASSERT_EQ(729844, BloomFilter::OptimalNumOfBits(100000, 0.03));
    ASSERT_EQ(7298440, BloomFilter::OptimalNumOfBits(1000000, 0.03));
    ASSERT_EQ(6235224, BloomFilter::OptimalNumOfBits(1000000, 0.05));
    ASSERT_EQ(1870567268, BloomFilter::OptimalNumOfBits(300000000, 0.05));
    ASSERT_EQ(1437758756, BloomFilter::OptimalNumOfBits(300000000, 0.1));
    ASSERT_EQ(432808512, BloomFilter::OptimalNumOfBits(300000000, 0.5));
    ASSERT_EQ(1393332198, BloomFilter::OptimalNumOfBits(3000000000, 0.8));
    ASSERT_EQ(657882327, BloomFilter::OptimalNumOfBits(3000000000, 0.9));
    ASSERT_EQ(0, BloomFilter::OptimalNumOfBits(3000000000, 1));
}

TEST(BloomFilterTest, TestBloomNumHashFunctions) {
    ASSERT_EQ(1, BloomFilter::OptimalNumOfHashFunctions(-1, -1));
    ASSERT_EQ(1, BloomFilter::OptimalNumOfHashFunctions(0, 0));
    ASSERT_EQ(1, BloomFilter::OptimalNumOfHashFunctions(10, 0));
    ASSERT_EQ(1, BloomFilter::OptimalNumOfHashFunctions(10, 10));
    ASSERT_EQ(7, BloomFilter::OptimalNumOfHashFunctions(10, 100));
    ASSERT_EQ(1, BloomFilter::OptimalNumOfHashFunctions(100, 100));
    ASSERT_EQ(1, BloomFilter::OptimalNumOfHashFunctions(1000, 100));
    ASSERT_EQ(1, BloomFilter::OptimalNumOfHashFunctions(10000, 100));
    ASSERT_EQ(1, BloomFilter::OptimalNumOfHashFunctions(100000, 100));
    ASSERT_EQ(1, BloomFilter::OptimalNumOfHashFunctions(1000000, 100));
    ASSERT_EQ(3634, BloomFilter::OptimalNumOfHashFunctions(100, 64 * 1024 * 8));
    ASSERT_EQ(363, BloomFilter::OptimalNumOfHashFunctions(1000, 64 * 1024 * 8));
    ASSERT_EQ(36, BloomFilter::OptimalNumOfHashFunctions(10000, 64 * 1024 * 8));
    ASSERT_EQ(4, BloomFilter::OptimalNumOfHashFunctions(100000, 64 * 1024 * 8));
    ASSERT_EQ(1, BloomFilter::OptimalNumOfHashFunctions(1000000, 64 * 1024 * 8));
}

TEST(BloomFilterTest, TestBloomFilter) {
    int32_t items = 100;
    auto pool = GetDefaultPool();
    auto bloom_filter = std::make_shared<BloomFilter>(100, 1024);

    std::mt19937_64 engine(std::random_device{}());  // NOLINT(whitespace/braces)
    std::uniform_int_distribution<int32_t> distribution(0, items);

    // segments 1
    auto seg1 = MemorySegment::AllocateHeapMemory(1024, pool.get());
    auto ptr1 = std::make_shared<MemorySegment>(seg1);
    ASSERT_OK(bloom_filter->SetMemorySegment(ptr1));

    std::set<int32_t> test_data1;
    for (int32_t i = 0; i < items; i++) {
        int32_t random = distribution(engine);
        test_data1.insert(random);
        ASSERT_OK(bloom_filter->AddHash(random));
    }
    for (const auto& value : test_data1) {
        ASSERT_TRUE(bloom_filter->TestHash(value));
    }

    // segments 2
    std::set<int32_t> test_data2;
    auto seg2 = MemorySegment::AllocateHeapMemory(1024, pool.get());
    auto ptr2 = std::make_shared<MemorySegment>(seg2);
    ASSERT_OK(bloom_filter->SetMemorySegment(ptr2));
    for (int32_t i = 0; i < items; i++) {
        int32_t random = distribution(engine);
        test_data2.insert(random);
        ASSERT_OK(bloom_filter->AddHash(random));
    }
    for (const auto& value : test_data2) {
        ASSERT_TRUE(bloom_filter->TestHash(value));
    }
    // switch to segment1
    ASSERT_OK(bloom_filter->SetMemorySegment(ptr1));
    for (const auto& value : test_data1) {
        ASSERT_TRUE(bloom_filter->TestHash(value));
    }

    // clear segment1
    bloom_filter->Reset();
    for (const auto& value : test_data1) {
        ASSERT_FALSE(bloom_filter->TestHash(value));
    }

    // switch to segment2 and clear
    ASSERT_OK(bloom_filter->SetMemorySegment(ptr2));
    bloom_filter->Reset();
    for (const auto& value : test_data2) {
        ASSERT_FALSE(bloom_filter->TestHash(value));
    }
}

}  // namespace paimon::test
