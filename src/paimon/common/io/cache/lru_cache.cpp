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

namespace paimon {

LruCache::LruCache(int64_t max_weight)
    : inner_cache_(InnerCache::Options{
          .max_weight = max_weight,
          .expire_after_access_ms = -1,
          .weigh_func = [](const std::shared_ptr<CacheKey>& /*key*/,
                           const std::shared_ptr<CacheValue>& value) -> int64_t {
              return value ? value->GetSegment().Size() : 0;
          },
          .removal_callback =
              [](const std::shared_ptr<CacheKey>& key, const std::shared_ptr<CacheValue>& value,
                 auto cause) {
                  if (value) {
                      value->OnEvict(key);
                  }
              }}) {}

Result<std::shared_ptr<CacheValue>> LruCache::Get(
    const std::shared_ptr<CacheKey>& key,
    std::function<Result<std::shared_ptr<CacheValue>>(const std::shared_ptr<CacheKey>&)> supplier) {
    return inner_cache_.Get(key, std::move(supplier));
}

Status LruCache::Put(const std::shared_ptr<CacheKey>& key,
                     const std::shared_ptr<CacheValue>& value) {
    return inner_cache_.Put(key, value);
}

void LruCache::Invalidate(const std::shared_ptr<CacheKey>& key) {
    inner_cache_.Invalidate(key);
}

void LruCache::InvalidateAll() {
    inner_cache_.InvalidateAll();
}

size_t LruCache::Size() const {
    return inner_cache_.Size();
}

int64_t LruCache::GetCurrentWeight() const {
    return inner_cache_.GetCurrentWeight();
}

int64_t LruCache::GetMaxWeight() const {
    return inner_cache_.GetMaxWeight();
}

}  // namespace paimon
