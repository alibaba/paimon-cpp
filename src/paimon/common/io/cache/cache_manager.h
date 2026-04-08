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
#include <string>

#include "paimon/common/io/cache/cache.h"
#include "paimon/common/io/cache/cache_key.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/result.h"

namespace paimon {
class PAIMON_EXPORT CacheManager {
 public:
    /// Refreshing the cache comes with some costs, so not every time we visit the CacheManager, but
    /// every 10 visits, refresh the LRU strategy.
    static constexpr int32_t REFRESH_COUNT = 10;

    /// Container that wraps a MemorySegment with an access counter for refresh.
    class SegmentContainer {
     public:
        explicit SegmentContainer(const MemorySegment& segment) : segment_(segment) {}

        const MemorySegment& Access() {
            access_count_++;
            return segment_;
        }

        int32_t GetAccessCount() const {
            return access_count_;
        }

     private:
        MemorySegment segment_;
        int32_t access_count_ = 0;
    };

    /// Constructs a CacheManager with LRU caching.
    /// @param max_memory_bytes Total cache capacity in bytes.
    /// @param high_priority_pool_ratio Ratio of capacity reserved for index cache [0.0, 1.0).
    ///        If 0, index and data share the same cache.
    CacheManager(int64_t max_memory_bytes, double high_priority_pool_ratio) {
        auto index_cache_bytes = static_cast<int64_t>(max_memory_bytes * high_priority_pool_ratio);
        auto data_cache_bytes =
            static_cast<int64_t>(max_memory_bytes * (1.0 - high_priority_pool_ratio));
        data_cache_ = std::make_shared<LruCache>(data_cache_bytes);
        if (high_priority_pool_ratio == 0.0) {
            index_cache_ = data_cache_;
        } else {
            index_cache_ = std::make_shared<LruCache>(index_cache_bytes);
        }
    }

    Result<MemorySegment> GetPage(
        std::shared_ptr<CacheKey>& key,
        std::function<Result<MemorySegment>(const std::shared_ptr<CacheKey>&)> reader,
        CacheCallback eviction_callback);

    void InvalidPage(const std::shared_ptr<CacheKey>& key);

    const std::shared_ptr<Cache>& DataCache() const {
        return data_cache_;
    }

    const std::shared_ptr<Cache>& IndexCache() const {
        return index_cache_;
    }

 private:
    std::shared_ptr<Cache> data_cache_;
    std::shared_ptr<Cache> index_cache_;
};

}  // namespace paimon
