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

#include "paimon/common/io/cache/lru_cache.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/io/cache/cache.h"
#include "paimon/common/io/cache/cache_key.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class LruCacheTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    std::shared_ptr<CacheKey> MakeKey(int64_t position, bool is_index = false) const {
        return CacheKey::ForPosition("test_file", position, 64, is_index);
    }

    std::shared_ptr<CacheValue> MakeValue(int32_t size, char fill_byte = 0,
                                          CacheCallback callback = {}) const {
        auto segment = MemorySegment::AllocateHeapMemory(size, pool_.get());
        std::memset(segment.GetHeapMemory()->data(), fill_byte, size);
        return std::make_shared<CacheValue>(segment, std::move(callback));
    }

    auto MakeSupplier(int32_t size, char fill_byte = 0, CacheCallback callback = {}) const {
        return [this, size, fill_byte, callback = std::move(callback)](
                   const std::shared_ptr<CacheKey>&) -> Result<std::shared_ptr<CacheValue>> {
            return MakeValue(size, fill_byte, callback);
        };
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

/// Verifies basic Get with supplier: cache miss loads via supplier, cache hit returns existing.
TEST_F(LruCacheTest, TestGetCacheHitAndMiss) {
    LruCache cache(1024);

    auto key = MakeKey(0);
    int32_t supplier_call_count = 0;
    auto supplier = [&](const std::shared_ptr<CacheKey>&) -> Result<std::shared_ptr<CacheValue>> {
        supplier_call_count++;
        return MakeValue(64, 'A');
    };

    // First Get: cache miss, supplier should be called
    ASSERT_OK_AND_ASSIGN(auto value1, cache.Get(key, supplier));
    ASSERT_EQ(supplier_call_count, 1);
    ASSERT_EQ(value1->GetSegment().Size(), 64);
    ASSERT_EQ(value1->GetSegment().Get(0), 'A');
    ASSERT_EQ(cache.Size(), 1);

    // Second Get with same key: cache hit, supplier should NOT be called
    ASSERT_OK_AND_ASSIGN(auto value2, cache.Get(key, supplier));
    ASSERT_EQ(supplier_call_count, 1);
    ASSERT_EQ(value2->GetSegment().Get(0), 'A');
    ASSERT_EQ(cache.Size(), 1);
}

/// Verifies Put inserts new entries and updates existing ones.
TEST_F(LruCacheTest, TestPutInsertAndUpdate) {
    LruCache cache(1024);

    auto key = MakeKey(0);
    auto value_a = MakeValue(64, 'A');
    auto value_b = MakeValue(128, 'B');

    // Insert
    cache.Put(key, value_a);
    ASSERT_EQ(cache.Size(), 1);
    ASSERT_EQ(cache.GetCurrentWeight(), 64);

    // Update with larger value
    cache.Put(key, value_b);
    ASSERT_EQ(cache.Size(), 1);
    ASSERT_EQ(cache.GetCurrentWeight(), 128);

    // Verify the updated value is returned
    ASSERT_OK_AND_ASSIGN(auto result, cache.Get(key, MakeSupplier(0)));
    ASSERT_EQ(result->GetSegment().Size(), 128);
    ASSERT_EQ(result->GetSegment().Get(0), 'B');
}

/// Verifies weight-based eviction: when total weight exceeds max, LRU entries are evicted.
TEST_F(LruCacheTest, TestWeightBasedEviction) {
    // Cache can hold at most 200 bytes
    LruCache cache(200);

    auto key0 = MakeKey(0);
    auto key1 = MakeKey(1);
    auto key2 = MakeKey(2);

    // Insert 2 entries of 100 bytes each (total 200, at capacity)
    cache.Put(key0, MakeValue(100, 'A'));
    cache.Put(key1, MakeValue(100, 'B'));
    ASSERT_EQ(cache.Size(), 2);
    ASSERT_EQ(cache.GetCurrentWeight(), 200);

    // Insert a 3rd entry: should evict key0 (LRU, inserted first)
    cache.Put(key2, MakeValue(100, 'C'));
    ASSERT_EQ(cache.Size(), 2);
    ASSERT_EQ(cache.GetCurrentWeight(), 200);

    // key0 should be evicted, Get should call supplier
    int32_t supplier_called = 0;
    auto supplier = [&](const std::shared_ptr<CacheKey>&) -> Result<std::shared_ptr<CacheValue>> {
        supplier_called++;
        return MakeValue(100, 'X');
    };
    ASSERT_OK_AND_ASSIGN(auto result, cache.Get(key0, supplier));
    ASSERT_EQ(supplier_called, 1);
    ASSERT_EQ(result->GetSegment().Get(0), 'X');
}

/// Verifies that eviction invokes the CacheCallback on evicted entries.
TEST_F(LruCacheTest, TestEvictionCallback) {
    LruCache cache(200);

    std::vector<int64_t> evicted_positions;
    auto make_callback = [&evicted_positions](int64_t position) -> CacheCallback {
        return [&evicted_positions, position](const std::shared_ptr<CacheKey>&) {
            evicted_positions.push_back(position);
        };
    };

    auto key0 = MakeKey(0);
    auto key1 = MakeKey(1);
    auto key2 = MakeKey(2);

    cache.Put(key0, MakeValue(100, 'A', make_callback(0)));
    cache.Put(key1, MakeValue(100, 'B', make_callback(1)));
    ASSERT_TRUE(evicted_positions.empty());

    // Insert key2: should evict key0 and trigger its callback
    cache.Put(key2, MakeValue(100, 'C', make_callback(2)));
    ASSERT_EQ(evicted_positions.size(), 1);
    ASSERT_EQ(evicted_positions[0], 0);
}

/// Verifies LRU ordering: accessing an entry moves it to the front, preventing eviction.
TEST_F(LruCacheTest, TestLruOrdering) {
    LruCache cache(200);

    auto key0 = MakeKey(0);
    auto key1 = MakeKey(1);
    auto key2 = MakeKey(2);

    cache.Put(key0, MakeValue(100, 'A'));
    cache.Put(key1, MakeValue(100, 'B'));

    // Access key0 via Get to move it to front (most recently used)
    ASSERT_OK_AND_ASSIGN(auto val, cache.Get(key0, MakeSupplier(0)));
    ASSERT_EQ(val->GetSegment().Get(0), 'A');

    // Insert key2: should evict key1 (now LRU), NOT key0
    std::vector<int64_t> evicted;
    auto callback0 = [&evicted](const std::shared_ptr<CacheKey>&) { evicted.push_back(0); };
    auto callback1 = [&evicted](const std::shared_ptr<CacheKey>&) { evicted.push_back(1); };

    // Re-insert with callbacks to track eviction
    cache.InvalidateAll();
    cache.Put(key0, MakeValue(100, 'A', callback0));
    cache.Put(key1, MakeValue(100, 'B', callback1));

    // Access key0 to move it to front
    ASSERT_OK_AND_ASSIGN(val, cache.Get(key0, MakeSupplier(0)));

    // Insert key2: key1 should be evicted (it's at the back)
    cache.Put(key2, MakeValue(100, 'C'));
    ASSERT_EQ(evicted.size(), 1);
    ASSERT_EQ(evicted[0], 1);
    ASSERT_EQ(cache.Size(), 2);
}

/// Verifies Invalidate removes a specific entry and adjusts weight.
TEST_F(LruCacheTest, TestInvalidate) {
    LruCache cache(1024);

    auto key0 = MakeKey(0);
    auto key1 = MakeKey(1);

    std::vector<int64_t> evicted;
    auto callback0 = [&evicted](const std::shared_ptr<CacheKey>&) { evicted.push_back(0); };
    auto callback1 = [&evicted](const std::shared_ptr<CacheKey>&) { evicted.push_back(1); };
    cache.Put(key0, MakeValue(100, 'A', callback0));
    cache.Put(key1, MakeValue(200, 'B', callback1));
    ASSERT_EQ(cache.Size(), 2);
    ASSERT_EQ(cache.GetCurrentWeight(), 300);
    ASSERT_TRUE(evicted.empty());

    // Invalidate key0
    cache.Invalidate(key0);
    ASSERT_EQ(cache.Size(), 1);
    ASSERT_EQ(cache.GetCurrentWeight(), 200);
    ASSERT_EQ(evicted, std::vector<int64_t>({0}));

    // Invalidating a non-existent key is a no-op
    cache.Invalidate(MakeKey(999));
    ASSERT_EQ(cache.Size(), 1);
    ASSERT_EQ(cache.GetCurrentWeight(), 200);
    ASSERT_EQ(evicted, std::vector<int64_t>({0}));
}

/// Verifies InvalidateAll clears all entries and resets weight.
TEST_F(LruCacheTest, TestInvalidateAll) {
    LruCache cache(1024);

    std::vector<int64_t> evicted;
    for (int32_t i = 0; i < 5; i++) {
        auto callback = [&evicted, id = i](const std::shared_ptr<CacheKey>&) {
            evicted.push_back(id);
        };
        cache.Put(MakeKey(i), MakeValue(50, 'A', callback));
    }
    ASSERT_EQ(cache.Size(), 5);
    ASSERT_EQ(cache.GetCurrentWeight(), 250);
    ASSERT_TRUE(evicted.empty());

    cache.InvalidateAll();
    ASSERT_EQ(cache.Size(), 0);
    ASSERT_EQ(cache.GetCurrentWeight(), 0);
    ASSERT_EQ(evicted, std::vector<int64_t>({0, 1, 2, 3, 4}));
}

/// Verifies weight tracking is accurate across Put, Update, Invalidate, and Eviction.
TEST_F(LruCacheTest, TestWeightTracking) {
    LruCache cache(500);

    auto key0 = MakeKey(0);
    auto key1 = MakeKey(1);

    // Put 100 bytes
    cache.Put(key0, MakeValue(100));
    ASSERT_EQ(cache.GetCurrentWeight(), 100);

    // Put 200 bytes
    cache.Put(key1, MakeValue(200));
    ASSERT_EQ(cache.GetCurrentWeight(), 300);

    // Update key0 from 100 to 150 bytes
    cache.Put(key0, MakeValue(150));
    ASSERT_EQ(cache.GetCurrentWeight(), 350);

    // Invalidate key1 (200 bytes)
    cache.Invalidate(key1);
    ASSERT_EQ(cache.GetCurrentWeight(), 150);

    // Put 200 bytes
    cache.Put(MakeKey(2), MakeValue(200));
    ASSERT_EQ(cache.GetCurrentWeight(), 350);

    // Add: total would be 550 > 500, should evict key0 (150 bytes)
    cache.Put(MakeKey(3), MakeValue(200));
    ASSERT_EQ(cache.GetCurrentWeight(), 400);
    ASSERT_EQ(cache.Size(), 2);
}

/// Verifies that Get via supplier correctly handles the case where the supplier returns an error.
TEST_F(LruCacheTest, TestSupplierError) {
    LruCache cache(1024);

    auto key = MakeKey(0);
    auto error_supplier =
        [](const std::shared_ptr<CacheKey>&) -> Result<std::shared_ptr<CacheValue>> {
        return Status::IOError("simulated read failure");
    };

    // Get should propagate the supplier error
    ASSERT_NOK_WITH_MSG(cache.Get(key, error_supplier), "simulated read failure");

    // Cache should remain empty after failed supplier
    ASSERT_EQ(cache.Size(), 0);
    ASSERT_EQ(cache.GetCurrentWeight(), 0);
}

/// Verifies thread safety: concurrent Get/Put operations should not corrupt internal state.
TEST_F(LruCacheTest, TestConcurrentAccess) {
    LruCache cache(10000);
    constexpr int32_t num_threads = 8;
    constexpr int32_t ops_per_thread = 100;

    std::atomic<int32_t> supplier_calls{0};
    auto supplier = [&](const std::shared_ptr<CacheKey>&) -> Result<std::shared_ptr<CacheValue>> {
        supplier_calls++;
        return MakeValue(10, 'X');
    };

    std::vector<std::thread> threads;
    for (int32_t t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (int32_t i = 0; i < ops_per_thread; i++) {
                auto key = MakeKey(t * ops_per_thread + i);
                auto result = cache.Get(key, supplier);
                ASSERT_TRUE(result.ok());
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // All entries should be in cache (capacity is large enough)
    ASSERT_EQ(cache.Size(), num_threads * ops_per_thread);
    ASSERT_EQ(supplier_calls.load(), num_threads * ops_per_thread);
}

/// Verifies that Put moves an existing entry to the front of the LRU list.
TEST_F(LruCacheTest, TestPutMovesToFront) {
    LruCache cache(200);

    auto key0 = MakeKey(0);
    auto key1 = MakeKey(1);
    auto key2 = MakeKey(2);

    std::vector<int64_t> evicted;
    auto make_callback = [&evicted](int64_t pos) -> CacheCallback {
        return [&evicted, pos](const std::shared_ptr<CacheKey>&) { evicted.push_back(pos); };
    };

    cache.Put(key0, MakeValue(100, 'A', make_callback(0)));
    cache.Put(key1, MakeValue(100, 'B', make_callback(1)));

    // Update key0 via Put (should move it to front)
    cache.Put(key0, MakeValue(100, 'A', make_callback(0)));

    // Insert key2: should evict key1 (now at back), not key0
    cache.Put(key2, MakeValue(100, 'C', make_callback(2)));
    ASSERT_EQ(evicted.size(), 1);
    ASSERT_EQ(evicted[0], 1);
}

/// Verifies that multiple evictions happen when a single large entry is inserted.
TEST_F(LruCacheTest, TestMultipleEvictions) {
    LruCache cache(300);

    std::vector<int64_t> evicted;
    auto make_callback = [&evicted](int64_t pos) -> CacheCallback {
        return [&evicted, pos](const std::shared_ptr<CacheKey>&) { evicted.push_back(pos); };
    };

    // Insert 3 entries of 100 bytes each
    cache.Put(MakeKey(0), MakeValue(100, 'A', make_callback(0)));
    cache.Put(MakeKey(1), MakeValue(100, 'B', make_callback(1)));
    cache.Put(MakeKey(2), MakeValue(100, 'C', make_callback(2)));
    ASSERT_EQ(cache.Size(), 3);
    ASSERT_EQ(cache.GetCurrentWeight(), 300);

    // Insert a 250-byte entry: should evict key0, key1 and key2.
    cache.Put(MakeKey(3), MakeValue(250, 'D'));
    ASSERT_EQ(cache.Size(), 1);
    ASSERT_EQ(cache.GetCurrentWeight(), 250);

    ASSERT_EQ(evicted, std::vector<int64_t>({0, 1, 2}));
}

}  // namespace paimon::test
