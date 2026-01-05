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

#pragma once
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "paimon/common/io/cache/cache_key.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/status.h"

namespace paimon {
class CacheValue;

class Cache {
 public:
    virtual std::shared_ptr<CacheValue> Get(
        const std::shared_ptr<CacheKey>& key,
        std::function<std::shared_ptr<CacheValue>(const std::shared_ptr<CacheKey>&)> supplier) = 0;

    virtual void Put(const std::shared_ptr<CacheKey>& key, std::shared_ptr<CacheValue>& value) = 0;

    virtual void Invalidate(const std::shared_ptr<CacheKey>& key) = 0;

    virtual void InvalidateAll() = 0;

    virtual std::unordered_map<std::shared_ptr<CacheKey>, std::shared_ptr<CacheValue>> AsMap() = 0;
};

class NoCache : public Cache {
 public:
    NoCache() {}

    std::shared_ptr<CacheValue> Get(
        const std::shared_ptr<CacheKey>& key,
        std::function<std::shared_ptr<CacheValue>(const std::shared_ptr<CacheKey>&)> supplier)
        override {
        return supplier(key);
    }

    void Put(const std::shared_ptr<CacheKey>& key, std::shared_ptr<CacheValue>& value) override {
        // do nothing
    }

    void Invalidate(const std::shared_ptr<CacheKey>& key) override {
        // do nothing
    }

    void InvalidateAll() override {
        // do nothing
    }

    std::unordered_map<std::shared_ptr<CacheKey>, std::shared_ptr<CacheValue>> AsMap() override {
        return {};
    }
};

class CacheValue {
 public:
    CacheValue(MemorySegment&& segment) : segment_(std::make_shared<MemorySegment>(segment)) {}

    std::shared_ptr<MemorySegment> GetSegment() {
        return segment_;
    }

 public:
    std::shared_ptr<MemorySegment> segment_;
};
}  // namespace paimon
