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

#include "paimon/testing/utils/counting_cache_test_utils.h"

namespace paimon::test {

class CountingManifestRoutingCache : public CountingRoutingCache {
 public:
    CountingManifestRoutingCache() : CountingRoutingCache(CacheKind::MANIFEST, 64 * 1024 * 1024) {}

    explicit CountingManifestRoutingCache(int64_t max_weight)
        : CountingRoutingCache(CacheKind::MANIFEST, max_weight) {}
};

}  // namespace paimon::test
