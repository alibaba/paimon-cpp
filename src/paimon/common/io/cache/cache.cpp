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

#include "paimon/common/io/cache/cache.h"

namespace paimon {
std::shared_ptr<CacheValue> NoCache::Get(
    const std::shared_ptr<CacheKey>& key,
    std::function<std::shared_ptr<CacheValue>(const std::shared_ptr<CacheKey>&)> supplier) {
    return supplier(key);
}

void NoCache::Put(const std::shared_ptr<CacheKey>& key, const std::shared_ptr<CacheValue>& value) {
    // do nothing
}

void NoCache::Invalidate(const std::shared_ptr<CacheKey>& key) {
    // do nothing
}

void NoCache::InvalidateAll() {
    // do nothing
}

std::unordered_map<std::shared_ptr<CacheKey>, std::shared_ptr<CacheValue>> NoCache::AsMap() {
    return {};
}

}  // namespace paimon
