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
#include <string>
#include <vector>

#include "paimon/result.h"

namespace paimon {

/// A path segment for variant get, representing either an object key access or an array index
/// access.
struct VariantPathSegment {
    enum class Kind { kObjectExtraction, kArrayExtraction };

    Kind kind;
    /// The object key when `kind` is `kObjectExtraction`.
    std::string key;
    /// The array index when `kind` is `kArrayExtraction`.
    int32_t index = 0;

    static VariantPathSegment ObjectExtraction(std::string key) {
        VariantPathSegment segment;
        segment.kind = Kind::kObjectExtraction;
        segment.key = std::move(key);
        return segment;
    }

    static VariantPathSegment ArrayExtraction(int32_t index) {
        VariantPathSegment segment;
        segment.kind = Kind::kArrayExtraction;
        segment.index = index;
        return segment;
    }

    /// Parses a path starting with `$`. Supported segments after the root are `.key`, `['key']`,
    /// `["key"]` and `[index]`, e.g. `$.user.addresses[0]['city']`.
    static Result<std::vector<VariantPathSegment>> Parse(const std::string& path);
};

}  // namespace paimon
