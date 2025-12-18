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

#include "paimon/common/utils/bloom_filter64.h"

#include <cstring>
#include <limits>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::test {

TEST(BloomFilter64Test, TestSimple) {
    int32_t items = 10000;
    auto pool = GetDefaultPool();
    BloomFilter64 bloom_filter(items, 0.02, pool);
    std::mt19937_64 engine(std::random_device{}());  // NOLINT(whitespace/braces)
    std::uniform_int_distribution<int64_t> distribution(std::numeric_limits<int64_t>::min(),
                                                        std::numeric_limits<int64_t>::max());
    std::set<int64_t> test_data;
    for (int32_t i = 0; i < items; i++) {
        int64_t random = distribution(engine);
        test_data.insert(random);
        bloom_filter.AddHash(random);
    }

    for (const auto& value : test_data) {
        ASSERT_TRUE(bloom_filter.TestHash(value));
    }

    // test false positive
    int32_t false_positives = 0;
    int32_t num = 1000000;
    for (int32_t i = 0; i < num; i++) {
        int64_t random = distribution(engine);
        if (bloom_filter.TestHash(random) && test_data.find(random) == test_data.end()) {
            false_positives++;
        }
    }
    ASSERT_TRUE(static_cast<double>(false_positives) / num < 0.03);
}

TEST(BloomFilter64Test, TestCompatibleWithJava) {
    // data: -10, -5, 0, 13, 100, 200, 500
    std::vector<uint8_t> se_bytes = {241, 255, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    auto pool = GetDefaultPool();
    auto bytes = std::make_shared<Bytes>(se_bytes.size(), pool.get());
    memcpy(bytes->data(), reinterpret_cast<char*>(se_bytes.data()), bytes->size());
    auto bit_set = std::make_unique<BloomFilter64::BitSet>(bytes, /*offset=*/0);
    BloomFilter64 bloom_filter(/*num_hash_functions=*/7, std::move(bit_set));
    std::vector<int64_t> expected_data = {-10, -5, 0, 13, 100, 200, 500};
    for (const auto& value : expected_data) {
        ASSERT_TRUE(bloom_filter.TestHash(value));
    }

    BloomFilter64 bloom_filter2(10, 0.01, pool);
    ASSERT_EQ(7, bloom_filter2.GetNumHashFunctions());
    ASSERT_EQ(se_bytes.size() * BloomFilter64::BYTE_SIZE, bloom_filter2.num_bits_);
    ASSERT_EQ(se_bytes.size(), bloom_filter2.GetBitSet().bytes_->size());
}

}  // namespace paimon::test
