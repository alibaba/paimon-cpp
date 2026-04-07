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

#include "paimon/common/io/cache/cache_key.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/result.h"

namespace paimon {

class CacheValue;

/// Callback invoked when a cache entry is evicted by the LRU policy.
using CacheCallback = std::function<void(const std::shared_ptr<CacheKey>&)>;

class Cache {
 public:
    virtual ~Cache() = default;
    virtual Result<std::shared_ptr<CacheValue>> Get(
        const std::shared_ptr<CacheKey>& key,
        std::function<Result<std::shared_ptr<CacheValue>>(const std::shared_ptr<CacheKey>&)>
            supplier) = 0;

    virtual void Put(const std::shared_ptr<CacheKey>& key,
                     const std::shared_ptr<CacheValue>& value) = 0;

    virtual void Invalidate(const std::shared_ptr<CacheKey>& key) = 0;

    virtual void InvalidateAll() = 0;

    virtual size_t Size() const = 0;
};

class CacheValue {
 public:
    CacheValue(const MemorySegment& segment, CacheCallback callback)
        : segment_(segment), callback_(std::move(callback)) {}

    const MemorySegment& GetSegment() const {
        return segment_;
    }

    /// Invoke the eviction callback, if one was registered.
    void OnEvict(const std::shared_ptr<CacheKey>& key) const {
        if (callback_) {
            callback_(key);
        }
    }

 private:
    MemorySegment segment_;
    CacheCallback callback_;
};
}  // namespace paimon
