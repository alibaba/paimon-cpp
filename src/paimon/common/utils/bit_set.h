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

#pragma once
#include <cstdint>
#include <functional>
#include <memory>

#include "paimon/common/memory/memory_segment.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {
class MemoryPool;

/// BitSet based on MemorySegment.
class PAIMON_EXPORT BitSet {
 public:
    BitSet(int64_t byte_length) : byte_length_(byte_length) {}

    Status SetMemorySegment(std::shared_ptr<MemorySegment> segment, int32_t offset);
    void UnsetMemorySegment();

    std::shared_ptr<MemorySegment> GetMemorySegment();
    int64_t GetBitLength();
    int64_t GetByteLength();

    Status Set(int32_t index);
    bool Get(int32_t index);
    void Clear();

 private:
    static constexpr int32_t BYTE_INDEX_MASK = 0x00000007;

 private:
    int64_t byte_length_;
    int64_t bit_length_;
    int32_t offset_;
    std::shared_ptr<MemorySegment> segment_;
};
}  // namespace paimon
