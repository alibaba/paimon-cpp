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

#include "paimon/common/io/cache/cache_manager.h"

namespace paimon {

std::shared_ptr<MemorySegment> CacheManager::GetPage(
    std::shared_ptr<CacheKey>& key,
    std::function<Result<MemorySegment>(const std::shared_ptr<CacheKey>&)> reader) {
    auto& cache = key->IsIndex() ? index_cache_ : data_cache_;
    auto supplier = [=](const std::shared_ptr<CacheKey>& k) -> std::shared_ptr<CacheValue> {
        auto ret = reader(k);
        if (!ret.ok()) {
            return nullptr;
        }
        auto segment = ret.value();
        auto ptr = std::make_shared<MemorySegment>(segment);
        return std::make_shared<CacheValue>(ptr);
    };
    return cache->Get(key, supplier)->GetSegment();
}

void CacheManager::InvalidPage(std::shared_ptr<CacheKey>& key) {
    if (key->IsIndex()) {
        index_cache_->Invalidate(key);
    } else {
        data_cache_->Invalidate(key);
    }
}

}  // namespace paimon
