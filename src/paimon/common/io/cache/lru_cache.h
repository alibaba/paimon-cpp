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

#pragma once
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "paimon/common/io/cache/cache.h"
#include "paimon/common/io/cache/cache_key.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/result.h"

namespace paimon {
/// LRU Cache implementation with weight-based eviction.
/// Uses std::list + unordered_map for O(1) get/put/evict:
/// list stores entries in LRU order (most recently used at front)
/// map stores key -> list::iterator for O(1) lookup
/// capacity is measured in bytes (sum of MemorySegment sizes)
/// when an entry is evicted, its CacheCallback is invoked to notify the upper layer
/// @note Thread-safe: all public methods are protected by mutex (read-write lock).
class LruCache : public Cache {
 public:
    explicit LruCache(int64_t max_weight);

    Result<std::shared_ptr<CacheValue>> Get(
        const std::shared_ptr<CacheKey>& key,
        std::function<Result<std::shared_ptr<CacheValue>>(const std::shared_ptr<CacheKey>&)>
            supplier) override;

    void Put(const std::shared_ptr<CacheKey>& key,
             const std::shared_ptr<CacheValue>& value) override;

    void Invalidate(const std::shared_ptr<CacheKey>& key) override;

    void InvalidateAll() override;

    size_t Size() const override;

    int64_t GetCurrentWeight() const;

    int64_t GetMaxWeight() const;

 private:
    using LruEntry = std::pair<std::shared_ptr<CacheKey>, std::shared_ptr<CacheValue>>;
    using LruList = std::list<LruEntry>;
    using LruMap = std::unordered_map<std::shared_ptr<CacheKey>, LruList::iterator, CacheKeyHash,
                                      CacheKeyEqual>;

    std::optional<std::shared_ptr<CacheValue>> FindAndPromote(const std::shared_ptr<CacheKey>& key);
    void Insert(const std::shared_ptr<CacheKey>& key, const std::shared_ptr<CacheValue>& value);
    void RemoveEntry(LruList::iterator list_it);

    void EvictIfNeeded();

    static int64_t GetWeight(const std::shared_ptr<CacheValue>& value);

    int64_t max_weight_;
    int64_t current_weight_;
    LruList lru_list_;
    LruMap lru_map_;
    mutable std::shared_mutex mutex_;
};

}  // namespace paimon
