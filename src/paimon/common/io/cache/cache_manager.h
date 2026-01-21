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
#include "paimon/common/memory/memory_segment.h"
#include "paimon/result.h"

namespace paimon {
class CacheManager {
 public:
    CacheManager() {
        // todo implements cache
        data_cache_ = std::make_shared<NoCache>();
        index_cache_ = std::make_shared<NoCache>();
    }

    std::shared_ptr<MemorySegment> GetPage(
        std::shared_ptr<CacheKey>& key,
        std::function<Result<MemorySegment>(const std::shared_ptr<CacheKey>&)> reader);

    void InvalidPage(std::shared_ptr<CacheKey>& key);

 private:
    std::shared_ptr<Cache> data_cache_;
    std::shared_ptr<Cache> index_cache_;
};

}  // namespace paimon
