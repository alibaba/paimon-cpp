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

#include "paimon/cache/cache.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/result.h"

namespace paimon::test {

class CountingRoutingCache : public Cache {
 public:
    CountingRoutingCache(CacheKind kind, int64_t max_weight) {
        caches_[kind] = std::make_shared<LruCache>(max_weight);
    }

    explicit CountingRoutingCache(const std::map<CacheKind, int64_t>& max_weights) {
        for (const auto& [kind, max_weight] : max_weights) {
            caches_[kind] = std::make_shared<LruCache>(max_weight);
        }
    }

    Result<std::shared_ptr<CacheValue>> Get(
        const std::shared_ptr<CacheKey>& key,
        std::function<Result<std::shared_ptr<CacheValue>>(const std::shared_ptr<CacheKey>&)>
            supplier) override {
        ++get_count_;
        last_kind_ = key->GetKind();
        ++get_count_by_kind_[key->GetKind()];
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Cache> cache, GetCache(key));
        return cache->Get(
            key,
            [this, supplier = std::move(supplier)](const std::shared_ptr<CacheKey>& supplier_key)
                -> Result<std::shared_ptr<CacheValue>> {
                ++supplier_call_count_;
                ++supplier_call_count_by_kind_[supplier_key->GetKind()];
                return supplier(supplier_key);
            });
    }

    Status Put(const std::shared_ptr<CacheKey>& key,
               const std::shared_ptr<CacheValue>& value) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Cache> cache, GetCache(key));
        return cache->Put(key, value);
    }

    void Invalidate(const std::shared_ptr<CacheKey>& key) override {
        Result<std::shared_ptr<Cache>> cache = GetCache(key);
        if (cache.ok()) {
            cache.value()->Invalidate(key);
        }
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

    int64_t GetCount(CacheKind kind) const {
        return GetCount(get_count_by_kind_, kind);
    }

    int64_t SupplierCallCount() const {
        return supplier_call_count_;
    }

    int64_t SupplierCallCount(CacheKind kind) const {
        return GetCount(supplier_call_count_by_kind_, kind);
    }

    CacheKind LastKind() const {
        return last_kind_;
    }

 private:
    static int64_t GetCount(const std::map<CacheKind, int64_t>& counts, CacheKind kind) {
        auto iter = counts.find(kind);
        if (iter == counts.end()) {
            return 0;
        }
        return iter->second;
    }

    Result<std::shared_ptr<Cache>> GetCache(const std::shared_ptr<CacheKey>& key) const {
        auto iter = caches_.find(key->GetKind());
        if (iter == caches_.end()) {
            return Status::Invalid("unexpected cache kind");
        }
        return iter->second;
    }

    std::map<CacheKind, std::shared_ptr<Cache>> caches_;
    std::map<CacheKind, int64_t> get_count_by_kind_;
    std::map<CacheKind, int64_t> supplier_call_count_by_kind_;
    int64_t get_count_ = 0;
    int64_t supplier_call_count_ = 0;
    CacheKind last_kind_ = CacheKind::DEFAULT;
};

}  // namespace paimon::test
