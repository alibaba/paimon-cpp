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
#include <memory>

#include "paimon/common/io/cache/cache.h"
#include "paimon/common/io/cache/cache_key.h"
#include "paimon/common/utils/generic_lru_cache.h"
#include "paimon/result.h"

namespace paimon {

/// LRU Cache implementation with weight-based eviction for block cache.
///
/// Wraps GenericLruCache with CacheKey/CacheValue types. Capacity is measured
/// in bytes (sum of MemorySegment sizes). When an entry is evicted, its
/// CacheCallback is invoked to notify the upper layer.
///
/// @note Thread-safe: all public methods are protected by the underlying GenericLruCache lock.
class LruCache : public Cache {
 public:
    explicit LruCache(int64_t max_weight);

    Result<std::shared_ptr<CacheValue>> Get(
        const std::shared_ptr<CacheKey>& key,
        std::function<Result<std::shared_ptr<CacheValue>>(const std::shared_ptr<CacheKey>&)>
            supplier) override;

    Status Put(const std::shared_ptr<CacheKey>& key,
               const std::shared_ptr<CacheValue>& value) override;

    void Invalidate(const std::shared_ptr<CacheKey>& key) override;

    void InvalidateAll() override;

    size_t Size() const override;

    int64_t GetCurrentWeight() const;

    int64_t GetMaxWeight() const;

 private:
    using InnerCache = GenericLruCache<std::shared_ptr<CacheKey>, std::shared_ptr<CacheValue>,
                                       CacheKeyHash, CacheKeyEqual>;

    InnerCache inner_cache_;
};

}  // namespace paimon
