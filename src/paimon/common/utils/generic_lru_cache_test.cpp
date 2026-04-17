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

#include "paimon/common/utils/generic_lru_cache.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class GenericLruCacheTest : public ::testing::Test {
 public:
    using StringIntCache = GenericLruCache<std::string, int>;
    using StringStringCache = GenericLruCache<std::string, std::string>;
    using IntIntCache = GenericLruCache<int, int>;
    using StringSharedPtrCache = GenericLruCache<std::string, std::shared_ptr<int>>;
    using RemovalCause = StringIntCache::RemovalCause;

    struct RemovalRecord {
        std::string key;
        std::string value;
        RemovalCause cause;
    };
};

// ==================== Basic Operations ====================

TEST_F(GenericLruCacheTest, ConstructorAndDefaults) {
    {
        StringIntCache::Options options;
        StringIntCache cache(options);

        ASSERT_EQ(cache.Size(), 0);
        ASSERT_EQ(cache.GetCurrentWeight(), 0);
        ASSERT_EQ(cache.GetMaxWeight(), INT64_MAX);
    }
    {
        StringIntCache::Options options;
        options.max_weight = 42;
        StringIntCache cache(options);
        ASSERT_EQ(cache.GetMaxWeight(), 42);
    }
}

// ==================== GetIfPresent ====================

TEST_F(GenericLruCacheTest, GetIfPresentMissAndHit) {
    StringIntCache::Options options;
    StringIntCache cache(options);

    auto result = cache.GetIfPresent("nonexistent");
    ASSERT_FALSE(result.has_value());

    ASSERT_OK(cache.Put("key1", 100));
    result = cache.GetIfPresent("key1");
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), 100);
}

TEST_F(GenericLruCacheTest, GetIfPresentExpired) {
    std::vector<RemovalRecord> removals;
    StringIntCache::Options options;
    options.expire_after_access_ms = 50;
    options.removal_callback = [&](const std::string& key, const int& value, auto cause) {
        removals.push_back({key, std::to_string(value), static_cast<RemovalCause>(cause)});
    };
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("key1", 100));
    ASSERT_EQ(cache.Size(), 1);

    std::this_thread::sleep_for(std::chrono::milliseconds(80));

    auto result = cache.GetIfPresent("key1");
    ASSERT_FALSE(result.has_value());
    ASSERT_EQ(cache.Size(), 0);

    ASSERT_EQ(removals.size(), 1);
    ASSERT_EQ(removals[0].key, "key1");
    ASSERT_EQ(removals[0].cause, RemovalCause::EXPIRED);
}

TEST_F(GenericLruCacheTest, GetIfPresentPromotesEntry) {
    std::vector<RemovalRecord> removals;
    StringIntCache::Options options;
    options.max_weight = 2;
    options.removal_callback = [&](const std::string& key, const int& value, auto cause) {
        removals.push_back({key, std::to_string(value), static_cast<RemovalCause>(cause)});
    };
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("a", 1));
    ASSERT_OK(cache.Put("b", 2));

    // Access "a" to promote it
    auto result = cache.GetIfPresent("a");
    ASSERT_TRUE(result.has_value());

    // Insert "c": should evict "b" (LRU), not "a"
    ASSERT_OK(cache.Put("c", 3));
    ASSERT_EQ(removals.size(), 1);
    ASSERT_EQ(removals[0].key, "b");
    ASSERT_EQ(removals[0].cause, RemovalCause::SIZE);
}

// ==================== Get with supplier ====================

TEST_F(GenericLruCacheTest, GetCacheMissLoadsViaSupplier) {
    StringIntCache::Options options;
    StringIntCache cache(options);

    int32_t supplier_calls = 0;
    auto supplier = [&](const std::string& key) -> Result<int> {
        supplier_calls++;
        return 42;
    };

    ASSERT_OK_AND_ASSIGN(auto value, cache.Get("key1", supplier));
    ASSERT_EQ(value, 42);
    ASSERT_EQ(supplier_calls, 1);
    ASSERT_EQ(cache.Size(), 1);
}

TEST_F(GenericLruCacheTest, GetCacheHitSkipsSupplier) {
    StringIntCache::Options options;
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("key1", 100));

    int32_t supplier_calls = 0;
    auto supplier = [&](const std::string& key) -> Result<int> {
        supplier_calls++;
        return 999;
    };

    ASSERT_OK_AND_ASSIGN(auto value, cache.Get("key1", supplier));
    ASSERT_EQ(value, 100);
    ASSERT_EQ(supplier_calls, 0);
}

TEST_F(GenericLruCacheTest, GetSupplierError) {
    StringIntCache::Options options;
    StringIntCache cache(options);

    auto supplier = [](const std::string& key) -> Result<int> {
        return Status::IOError("load failed");
    };

    ASSERT_NOK_WITH_MSG(cache.Get("key1", supplier), "load failed");
    ASSERT_EQ(cache.Size(), 0);
}

TEST_F(GenericLruCacheTest, GetWeightExceedsMaxReturnsWithoutCaching) {
    StringStringCache::Options options;
    options.max_weight = 5;
    options.weigh_func = [](const std::string& key, const std::string& value) -> int64_t {
        return static_cast<int64_t>(value.size());
    };
    StringStringCache cache(options);

    auto supplier = [](const std::string& key) -> Result<std::string> {
        return std::string("this_is_a_very_long_value");
    };

    ASSERT_OK_AND_ASSIGN(auto value, cache.Get("key1", supplier));
    ASSERT_EQ(value, "this_is_a_very_long_value");
    ASSERT_EQ(cache.Size(), 0);
}

TEST_F(GenericLruCacheTest, GetTriggersEviction) {
    std::vector<RemovalRecord> removals;
    StringIntCache::Options options;
    options.max_weight = 2;
    options.removal_callback = [&](const std::string& key, const int& value, auto cause) {
        removals.push_back({key, std::to_string(value), static_cast<RemovalCause>(cause)});
    };
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("a", 1));
    ASSERT_OK(cache.Put("b", 2));
    ASSERT_EQ(cache.Size(), 2);

    auto supplier = [](const std::string& key) -> Result<int> { return 3; };
    ASSERT_OK_AND_ASSIGN(auto value, cache.Get("c", supplier));
    ASSERT_EQ(value, 3);
    ASSERT_EQ(cache.Size(), 2);

    ASSERT_EQ(removals.size(), 1);
    ASSERT_EQ(removals[0].key, "a");
    ASSERT_EQ(removals[0].cause, RemovalCause::SIZE);
}

// ==================== Put ====================

TEST_F(GenericLruCacheTest, PutNewEntry) {
    StringIntCache::Options options;
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("key1", 100));
    ASSERT_EQ(cache.Size(), 1);
    ASSERT_EQ(cache.GetCurrentWeight(), 1);

    auto result = cache.GetIfPresent("key1");
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), 100);
}

TEST_F(GenericLruCacheTest, PutReplaceWithDifferentValue) {
    std::vector<RemovalRecord> removals;
    StringIntCache::Options options;
    options.removal_callback = [&](const std::string& key, const int& value, auto cause) {
        removals.push_back({key, std::to_string(value), static_cast<RemovalCause>(cause)});
    };
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("key1", 100));
    ASSERT_OK(cache.Put("key1", 200));
    ASSERT_EQ(cache.Size(), 1);

    auto result = cache.GetIfPresent("key1");
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), 200);

    ASSERT_EQ(removals.size(), 1);
    ASSERT_EQ(removals[0].key, "key1");
    ASSERT_EQ(removals[0].value, "100");
    ASSERT_EQ(removals[0].cause, RemovalCause::REPLACED);
}

TEST_F(GenericLruCacheTest, PutReplaceWithSameValuePromotes) {
    std::vector<RemovalRecord> removals;
    StringIntCache::Options options;
    options.max_weight = 2;
    options.removal_callback = [&](const std::string& key, const int& value, auto cause) {
        removals.push_back({key, std::to_string(value), static_cast<RemovalCause>(cause)});
    };
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("a", 1));
    ASSERT_OK(cache.Put("b", 2));

    // Put same value for "a" — should promote without REPLACED callback
    ASSERT_OK(cache.Put("a", 1));
    ASSERT_TRUE(removals.empty());

    // Insert "c": should evict "b" (LRU after "a" was promoted)
    ASSERT_OK(cache.Put("c", 3));
    ASSERT_EQ(removals.size(), 1);
    ASSERT_EQ(removals[0].key, "b");
    ASSERT_EQ(removals[0].cause, RemovalCause::SIZE);
}

TEST_F(GenericLruCacheTest, PutWeightExceedsMaxReturnsInvalid) {
    StringStringCache::Options options;
    options.max_weight = 5;
    options.weigh_func = [](const std::string& key, const std::string& value) -> int64_t {
        return static_cast<int64_t>(value.size());
    };
    StringStringCache cache(options);

    ASSERT_NOK_WITH_MSG(cache.Put("key1", "this_is_too_long"),
                        "Entry weight 16 exceeds cache max weight 5, entry will not be cached");
    ASSERT_EQ(cache.Size(), 0);
}

TEST_F(GenericLruCacheTest, PutTriggersWeightEviction) {
    std::vector<RemovalRecord> removals;
    StringStringCache::Options options;
    options.max_weight = 10;
    options.weigh_func = [](const std::string& key, const std::string& value) -> int64_t {
        return static_cast<int64_t>(value.size());
    };
    options.removal_callback = [&](const std::string& key, const std::string& value, auto cause) {
        removals.push_back({key, value, static_cast<RemovalCause>(cause)});
    };
    StringStringCache cache(options);

    ASSERT_OK(cache.Put("a", "aaaa"));   // weight 4
    ASSERT_OK(cache.Put("b", "bbbbb"));  // weight 5, total 9
    ASSERT_EQ(cache.GetCurrentWeight(), 9);

    // Insert "c" with weight 5: total would be 14 > 10, evict "a" (4), total becomes 10
    ASSERT_OK(cache.Put("c", "ccccc"));
    ASSERT_EQ(cache.Size(), 2);
    ASSERT_EQ(cache.GetCurrentWeight(), 10);

    ASSERT_EQ(removals.size(), 1);
    ASSERT_EQ(removals[0].key, "a");
    ASSERT_EQ(removals[0].cause, RemovalCause::SIZE);
}

TEST_F(GenericLruCacheTest, PutMultipleEvictions) {
    std::vector<std::string> evicted_keys;
    StringStringCache::Options options;
    options.max_weight = 10;
    options.weigh_func = [](const std::string& key, const std::string& value) -> int64_t {
        return static_cast<int64_t>(value.size());
    };
    options.removal_callback = [&](const std::string& key, const std::string& value, auto cause) {
        evicted_keys.push_back(key);
    };
    StringStringCache cache(options);

    ASSERT_OK(cache.Put("a", "aaa"));  // weight 3
    ASSERT_OK(cache.Put("b", "bbb"));  // weight 3
    ASSERT_OK(cache.Put("c", "ccc"));  // weight 3, total 9

    // Insert "d" with weight 9: total would be 18 > 10, evict a(3), b(3), c(3) then add d(9)
    ASSERT_OK(cache.Put("d", "ddddddddd"));
    ASSERT_EQ(cache.Size(), 1);
    ASSERT_EQ(cache.GetCurrentWeight(), 9);

    ASSERT_EQ(evicted_keys.size(), 3);
    ASSERT_EQ(evicted_keys[0], "a");
    ASSERT_EQ(evicted_keys[1], "b");
    ASSERT_EQ(evicted_keys[2], "c");
}

// ==================== Invalidate ====================

TEST_F(GenericLruCacheTest, InvalidateExistingKey) {
    std::vector<RemovalRecord> removals;
    StringIntCache::Options options;
    options.removal_callback = [&](const std::string& key, const int& value, auto cause) {
        removals.push_back({key, std::to_string(value), static_cast<RemovalCause>(cause)});
    };
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("key1", 100));
    ASSERT_EQ(cache.Size(), 1);

    cache.Invalidate("key1");
    ASSERT_EQ(cache.Size(), 0);
    ASSERT_EQ(cache.GetCurrentWeight(), 0);

    ASSERT_EQ(removals.size(), 1);
    ASSERT_EQ(removals[0].key, "key1");
    ASSERT_EQ(removals[0].cause, RemovalCause::EXPLICIT);
}

TEST_F(GenericLruCacheTest, InvalidateNonExistentKey) {
    StringIntCache::Options options;
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("key1", 100));
    cache.Invalidate("nonexistent");
    ASSERT_EQ(cache.Size(), 1);
}

// ==================== InvalidateAll ====================

TEST_F(GenericLruCacheTest, InvalidateAllClearsEverything) {
    std::vector<RemovalRecord> removals;
    StringIntCache::Options options;
    options.removal_callback = [&](const std::string& key, const int& value, auto cause) {
        removals.push_back({key, std::to_string(value), static_cast<RemovalCause>(cause)});
    };
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("a", 1));
    ASSERT_OK(cache.Put("b", 2));
    ASSERT_OK(cache.Put("c", 3));
    ASSERT_EQ(cache.Size(), 3);

    cache.InvalidateAll();
    ASSERT_EQ(cache.Size(), 0);
    ASSERT_EQ(cache.GetCurrentWeight(), 0);

    ASSERT_EQ(removals.size(), 3);
    for (const auto& record : removals) {
        ASSERT_EQ(record.cause, RemovalCause::EXPLICIT);
    }
}

TEST_F(GenericLruCacheTest, InvalidateAllOnEmptyCache) {
    StringIntCache::Options options;
    StringIntCache cache(options);

    cache.InvalidateAll();
    ASSERT_EQ(cache.Size(), 0);
    ASSERT_EQ(cache.GetCurrentWeight(), 0);
}

// ==================== Weight Function ====================

TEST_F(GenericLruCacheTest, DefaultWeightIsOne) {
    StringIntCache::Options options;
    options.max_weight = 3;
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("a", 1));
    ASSERT_OK(cache.Put("b", 2));
    ASSERT_OK(cache.Put("c", 3));
    ASSERT_EQ(cache.GetCurrentWeight(), 3);
    ASSERT_EQ(cache.Size(), 3);

    // Adding one more should evict the LRU entry
    ASSERT_OK(cache.Put("d", 4));
    ASSERT_EQ(cache.Size(), 3);
    ASSERT_EQ(cache.GetCurrentWeight(), 3);
    ASSERT_FALSE(cache.GetIfPresent("a").has_value());
}

TEST_F(GenericLruCacheTest, WeightUpdatedOnReplace) {
    StringStringCache::Options options;
    options.max_weight = 100;
    options.weigh_func = [](const std::string& key, const std::string& value) -> int64_t {
        return static_cast<int64_t>(value.size());
    };
    StringStringCache cache(options);

    ASSERT_OK(cache.Put("a", std::string(30, 'x')));
    ASSERT_EQ(cache.GetCurrentWeight(), 30);

    // Replace with larger value
    ASSERT_OK(cache.Put("a", std::string(70, 'y')));
    ASSERT_EQ(cache.GetCurrentWeight(), 70);
    ASSERT_EQ(cache.Size(), 1);
}

// ==================== Expiration ====================

TEST_F(GenericLruCacheTest, ExpirationDisabledByDefault) {
    StringIntCache::Options options;
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("key1", 100));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto result = cache.GetIfPresent("key1");
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), 100);
}

TEST_F(GenericLruCacheTest, ExpirationOnGet) {
    StringIntCache::Options options;
    options.expire_after_access_ms = 50;
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("key1", 100));

    // Access before expiration
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    auto result = cache.GetIfPresent("key1");
    ASSERT_TRUE(result.has_value());

    // Wait for expiration
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    result = cache.GetIfPresent("key1");
    ASSERT_FALSE(result.has_value());
    ASSERT_EQ(cache.Size(), 0);
}

TEST_F(GenericLruCacheTest, ExpirationOnGetWithSupplier) {
    StringIntCache::Options options;
    options.expire_after_access_ms = 50;
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("key1", 100));

    std::this_thread::sleep_for(std::chrono::milliseconds(80));

    int32_t supplier_calls = 0;
    auto supplier = [&](const std::string& key) -> Result<int> {
        supplier_calls++;
        return 200;
    };

    ASSERT_OK_AND_ASSIGN(auto value, cache.Get("key1", supplier));
    ASSERT_EQ(value, 200);
    ASSERT_EQ(supplier_calls, 1);
}

TEST_F(GenericLruCacheTest, AccessResetsExpirationTimer) {
    StringIntCache::Options options;
    options.expire_after_access_ms = 100;
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("key1", 100));

    // Access at 40ms to reset the timer
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    auto result = cache.GetIfPresent("key1");
    ASSERT_TRUE(result.has_value());

    // At 80ms from last access (40ms from the GetIfPresent), should still be valid
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    result = cache.GetIfPresent("key1");
    ASSERT_TRUE(result.has_value());

    // Wait for full expiration from last access
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    result = cache.GetIfPresent("key1");
    ASSERT_FALSE(result.has_value());
}

TEST_F(GenericLruCacheTest, ExpiredEntriesEvictedOnPut) {
    std::vector<RemovalRecord> removals;
    StringIntCache::Options options;
    options.expire_after_access_ms = 50;
    options.max_weight = 100;
    options.removal_callback = [&](const std::string& key, const int& value, auto cause) {
        removals.push_back({key, std::to_string(value), static_cast<RemovalCause>(cause)});
    };
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("a", 1));
    ASSERT_OK(cache.Put("b", 2));

    std::this_thread::sleep_for(std::chrono::milliseconds(80));

    // Put triggers EvictIfNeeded which calls EvictExpired
    ASSERT_OK(cache.Put("c", 3));

    // "a" and "b" should have been expired
    int32_t expired_count = 0;
    for (const auto& record : removals) {
        if (record.cause == RemovalCause::EXPIRED) {
            expired_count++;
        }
    }
    ASSERT_EQ(expired_count, 2);
    ASSERT_EQ(cache.Size(), 1);
}

// ==================== Removal Callback ====================

TEST_F(GenericLruCacheTest, NoCallbackConfigured) {
    StringIntCache::Options options;
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("key1", 100));
    cache.Invalidate("key1");
    ASSERT_EQ(cache.Size(), 0);
}

TEST_F(GenericLruCacheTest, AllRemovalCauses) {
    std::vector<RemovalRecord> removals;
    StringIntCache::Options options;
    options.max_weight = 2;
    options.expire_after_access_ms = 50;
    options.removal_callback = [&](const std::string& key, const int& value, auto cause) {
        removals.push_back({key, std::to_string(value), static_cast<RemovalCause>(cause)});
    };
    StringIntCache cache(options);

    // REPLACED: put same key with different value
    ASSERT_OK(cache.Put("r", 1));
    ASSERT_OK(cache.Put("r", 2));
    ASSERT_EQ(removals.back().cause, RemovalCause::REPLACED);

    // EXPLICIT: invalidate
    cache.Invalidate("r");
    ASSERT_EQ(removals.back().cause, RemovalCause::EXPLICIT);

    // SIZE: evict due to weight
    ASSERT_OK(cache.Put("s1", 10));
    ASSERT_OK(cache.Put("s2", 20));
    ASSERT_OK(cache.Put("s3", 30));
    ASSERT_EQ(removals.back().cause, RemovalCause::SIZE);

    // EXPIRED: wait and access
    cache.InvalidateAll();
    removals.clear();
    ASSERT_OK(cache.Put("e", 99));
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    ASSERT_FALSE(cache.GetIfPresent("e").has_value());
    ASSERT_EQ(removals.back().cause, RemovalCause::EXPIRED);
}

// ==================== valuesequal with shared_ptr ====================

TEST_F(GenericLruCacheTest, SharedPtrSamePointerNoReplace) {
    using Cause = StringSharedPtrCache::RemovalCause;
    std::vector<Cause> causes;
    StringSharedPtrCache::Options options;
    options.removal_callback = [&](const std::string& key, const std::shared_ptr<int>& value,
                                   auto cause) { causes.push_back(static_cast<Cause>(cause)); };
    StringSharedPtrCache cache(options);

    auto ptr = std::make_shared<int>(42);
    ASSERT_OK(cache.Put("key1", ptr));

    // Put same pointer — ValuesEqual returns true, should promote without REPLACED
    ASSERT_OK(cache.Put("key1", ptr));
    ASSERT_TRUE(causes.empty());
    ASSERT_EQ(cache.Size(), 1);
}

TEST_F(GenericLruCacheTest, SharedPtrDifferentPointerSameValueNoReplace) {
    using Cause = StringSharedPtrCache::RemovalCause;
    std::vector<Cause> causes;
    StringSharedPtrCache::Options options;
    options.removal_callback = [&](const std::string& key, const std::shared_ptr<int>& value,
                                   auto cause) { causes.push_back(static_cast<Cause>(cause)); };
    StringSharedPtrCache cache(options);

    auto ptr1 = std::make_shared<int>(42);
    auto ptr2 = std::make_shared<int>(42);
    ASSERT_NE(ptr1.get(), ptr2.get());

    ASSERT_OK(cache.Put("key1", ptr1));
    // Different pointer but same dereferenced value — ValuesEqual returns true
    ASSERT_OK(cache.Put("key1", ptr2));
    ASSERT_TRUE(causes.empty());
}

TEST_F(GenericLruCacheTest, SharedPtrDifferentValueReplaces) {
    using Cause = StringSharedPtrCache::RemovalCause;
    std::vector<Cause> causes;
    StringSharedPtrCache::Options options;
    options.removal_callback = [&](const std::string& key, const std::shared_ptr<int>& value,
                                   auto cause) { causes.push_back(static_cast<Cause>(cause)); };
    StringSharedPtrCache cache(options);

    ASSERT_OK(cache.Put("key1", std::make_shared<int>(1)));
    ASSERT_OK(cache.Put("key1", std::make_shared<int>(2)));

    ASSERT_EQ(causes.size(), 1);
    ASSERT_EQ(causes[0], Cause::REPLACED);
}

TEST_F(GenericLruCacheTest, SharedPtrNullptrComparison) {
    using Cause = StringSharedPtrCache::RemovalCause;
    std::vector<Cause> causes;
    StringSharedPtrCache::Options options;
    options.removal_callback = [&](const std::string& key, const std::shared_ptr<int>& value,
                                   auto cause) { causes.push_back(static_cast<Cause>(cause)); };
    StringSharedPtrCache cache(options);

    // Put nullptr
    ASSERT_OK(cache.Put("key1", nullptr));

    // Put nullptr again — same value, should not replace
    ASSERT_OK(cache.Put("key1", nullptr));
    ASSERT_TRUE(causes.empty());

    // Put non-null — different from nullptr, should replace
    ASSERT_OK(cache.Put("key1", std::make_shared<int>(1)));
    ASSERT_EQ(causes.size(), 1);
    ASSERT_EQ(causes[0], Cause::REPLACED);

    // Put nullptr again — different from non-null, should replace
    causes.clear();
    ASSERT_OK(cache.Put("key1", nullptr));
    ASSERT_EQ(causes.size(), 1);
    ASSERT_EQ(causes[0], Cause::REPLACED);
}

// ==================== Custom Hash and KeyEqual ====================

TEST_F(GenericLruCacheTest, CustomHashAndKeyEqual) {
    struct CaseInsensitiveHash {
        size_t operator()(const std::string& str) const {
            std::string lower = str;
            for (auto& ch : lower) {
                ch = static_cast<char>(std::tolower(ch));
            }
            return std::hash<std::string>{}(lower);
        }
    };
    struct CaseInsensitiveEqual {
        bool operator()(const std::string& lhs, const std::string& rhs) const {
            if (lhs.size() != rhs.size()) return false;
            for (size_t i = 0; i < lhs.size(); i++) {
                if (std::tolower(lhs[i]) != std::tolower(rhs[i])) return false;
            }
            return true;
        }
    };

    using CICache = GenericLruCache<std::string, int, CaseInsensitiveHash, CaseInsensitiveEqual>;
    CICache::Options options;
    CICache cache(options);

    ASSERT_OK(cache.Put("Hello", 1));
    ASSERT_EQ(cache.Size(), 1);

    // "hello" should match "Hello" with case-insensitive comparison
    auto result = cache.GetIfPresent("hello");
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), 1);

    // Put with different case should replace
    ASSERT_OK(cache.Put("HELLO", 2));
    ASSERT_EQ(cache.Size(), 1);

    result = cache.GetIfPresent("Hello");
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), 2);
}

// ==================== Thread Safety ====================

TEST_F(GenericLruCacheTest, ConcurrentPutAndGet) {
    IntIntCache::Options options;
    options.max_weight = 10000;
    IntIntCache cache(options);

    constexpr int32_t num_threads = 8;
    constexpr int32_t ops_per_thread = 200;

    std::vector<std::thread> threads;
    std::atomic<int32_t> errors{0};

    for (int32_t t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (int32_t i = 0; i < ops_per_thread; i++) {
                int32_t key = t * ops_per_thread + i;
                auto status = cache.Put(key, key * 10);
                if (!status.ok()) {
                    errors++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(errors.load(), 0);
    ASSERT_EQ(static_cast<int32_t>(cache.Size()), num_threads * ops_per_thread);

    // Concurrent reads
    threads.clear();
    for (int32_t t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (int32_t i = 0; i < ops_per_thread; i++) {
                int32_t key = t * ops_per_thread + i;
                auto result = cache.GetIfPresent(key);
                if (!result.has_value() || result.value() != key * 10) {
                    errors++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(errors.load(), 0);
}

TEST_F(GenericLruCacheTest, ConcurrentGetWithSupplier) {
    IntIntCache::Options options;
    options.max_weight = 10000;
    IntIntCache cache(options);

    constexpr int32_t num_threads = 8;
    constexpr int32_t ops_per_thread = 100;

    std::atomic<int> supplier_calls{0};
    std::vector<std::thread> threads;

    for (int32_t t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (int32_t i = 0; i < ops_per_thread; i++) {
                int32_t key = t * ops_per_thread + i;
                auto supplier = [&, key](const int&) -> Result<int> {
                    supplier_calls++;
                    return key * 10;
                };
                auto result = cache.Get(key, supplier);
                ASSERT_TRUE(result.ok());
                ASSERT_EQ(result.value(), key * 10);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(static_cast<int>(cache.Size()), num_threads * ops_per_thread);
}

TEST_F(GenericLruCacheTest, ConcurrentInvalidate) {
    IntIntCache::Options options;
    IntIntCache cache(options);

    for (int32_t i = 0; i < 100; i++) {
        ASSERT_OK(cache.Put(i, i));
    }

    std::vector<std::thread> threads;
    for (int32_t t = 0; t < 4; t++) {
        threads.emplace_back([&, t]() {
            for (int32_t i = t * 25; i < (t + 1) * 25; i++) {
                cache.Invalidate(i);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(cache.Size(), 0);
}

// ==================== Edge Cases ====================

TEST_F(GenericLruCacheTest, PutAndGetSingleEntry) {
    StringIntCache::Options options;
    options.max_weight = 1;
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("only", 42));
    ASSERT_EQ(cache.Size(), 1);

    auto result = cache.GetIfPresent("only");
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), 42);

    // Adding another entry should evict the first
    ASSERT_OK(cache.Put("new", 99));
    ASSERT_EQ(cache.Size(), 1);
    ASSERT_FALSE(cache.GetIfPresent("only").has_value());
    ASSERT_TRUE(cache.GetIfPresent("new").has_value());
}

TEST_F(GenericLruCacheTest, ReplaceUpdatesWeight) {
    StringStringCache::Options options;
    options.max_weight = 100;
    options.weigh_func = [](const std::string& key, const std::string& value) -> int64_t {
        return static_cast<int64_t>(value.size());
    };
    StringStringCache cache(options);

    ASSERT_OK(cache.Put("a", std::string(50, 'x')));
    ASSERT_EQ(cache.GetCurrentWeight(), 50);

    // Replace with smaller value
    ASSERT_OK(cache.Put("a", std::string(20, 'y')));
    ASSERT_EQ(cache.GetCurrentWeight(), 20);

    // Replace with larger value
    ASSERT_OK(cache.Put("a", std::string(80, 'z')));
    ASSERT_EQ(cache.GetCurrentWeight(), 80);
}

TEST_F(GenericLruCacheTest, EvictionOrderIsLru) {
    std::vector<std::string> evicted_keys;
    StringIntCache::Options options;
    options.max_weight = 3;
    options.removal_callback = [&](const std::string& key, const int& value, auto cause) {
        if (static_cast<RemovalCause>(cause) == RemovalCause::SIZE) {
            evicted_keys.push_back(key);
        }
    };
    StringIntCache cache(options);

    ASSERT_OK(cache.Put("a", 1));
    ASSERT_OK(cache.Put("b", 2));
    ASSERT_OK(cache.Put("c", 3));

    // Access "a" and "b" to make "c" the LRU
    cache.GetIfPresent("a");
    cache.GetIfPresent("b");

    // Insert "d": should evict "c" (LRU)
    ASSERT_OK(cache.Put("d", 4));
    ASSERT_EQ(evicted_keys.size(), 1);
    ASSERT_EQ(evicted_keys[0], "c");
}

}  // namespace paimon::test
