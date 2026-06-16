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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/cache/cache.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/result.h"

namespace paimon::test {

class CountingManifestRoutingCache : public Cache {
 public:
    explicit CountingManifestRoutingCache(int64_t max_weight = 64 * 1024 * 1024) {
        caches_[CacheKind::MANIFEST] = std::make_shared<LruCache>(max_weight);
    }

    Result<std::shared_ptr<CacheValue>> Get(
        const std::shared_ptr<CacheKey>& key,
        std::function<Result<std::shared_ptr<CacheValue>>(const std::shared_ptr<CacheKey>&)>
            supplier) override {
        ++get_count_;
        return GetCache(key)->Get(
            key,
            [this, supplier = std::move(supplier)](const std::shared_ptr<CacheKey>& supplier_key)
                -> Result<std::shared_ptr<CacheValue>> {
                ++supplier_call_count_;
                return supplier(supplier_key);
            });
    }

    Status Put(const std::shared_ptr<CacheKey>& key,
               const std::shared_ptr<CacheValue>& value) override {
        return GetCache(key)->Put(key, value);
    }

    void Invalidate(const std::shared_ptr<CacheKey>& key) override {
        GetCache(key)->Invalidate(key);
    }

    void InvalidateAll() override {
        for (const auto& [kind, cache] : caches_) {
            cache->InvalidateAll();
        }
    }

    size_t Size() const override {
        size_t size = 0;
        for (const auto& [kind, cache] : caches_) {
            size += cache->Size();
        }
        return size;
    }

    int64_t GetCount() const {
        return get_count_;
    }

    int64_t SupplierCallCount() const {
        return supplier_call_count_;
    }

 private:
    std::shared_ptr<Cache> GetCache(const std::shared_ptr<CacheKey>& key) const {
        EXPECT_EQ(CacheKind::MANIFEST, key->GetKind());
        auto iter = caches_.find(key->GetKind());
        EXPECT_NE(caches_.end(), iter);
        return iter == caches_.end() ? nullptr : iter->second;
    }

    std::map<CacheKind, std::shared_ptr<Cache>> caches_;
    int64_t get_count_ = 0;
    int64_t supplier_call_count_ = 0;
};

}  // namespace paimon::test
