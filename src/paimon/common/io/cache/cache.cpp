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

#include "paimon/common/io/cache/cache.h"

namespace paimon {
    
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
}  // namespace paimon
