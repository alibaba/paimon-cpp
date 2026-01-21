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

#include <memory>

#include "paimon/common/memory/memory_segment.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"

namespace paimon {

class BloomFilterHandle {
 public:
    BloomFilterHandle(int64_t offset, int32_t size, int64_t expected_entries)
        : offset_(offset), size_(size), expected_entries_(expected_entries) {}

    BloomFilterHandle() = default;
    ~BloomFilterHandle() = default;

    int64_t Offset() const {
        return offset_;
    }

    int32_t Size() const {
        return size_;
    }

    int64_t ExpectedEntries() const {
        return expected_entries_;
    }

 public:
    static constexpr int32_t MAX_ENCODED_LENGTH = 9 + 5;

 private:
    int64_t offset_;
    int32_t size_;
    int64_t expected_entries_;
};
}  // namespace paimon
