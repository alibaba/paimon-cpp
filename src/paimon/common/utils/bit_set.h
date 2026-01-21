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

#include "paimon/common/memory/memory_slice.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {
class MemoryPool;

/// BitSet based on MemorySegment.
class PAIMON_EXPORT BitSet {
 public:
    explicit BitSet(int64_t byte_length) : byte_length_(byte_length), bit_size_(byte_length << 3) {}

    Status SetMemorySegment(std::shared_ptr<MemorySegment> segment, int32_t offset = 0);
    void UnsetMemorySegment();

    std::shared_ptr<MemorySegment>& GetMemorySegment() {
        return segment_;
    }

    std::shared_ptr<MemorySlice> ToSlice() {
        return std::make_shared<MemorySlice>(segment_, offset_, byte_length_);
    }

    int32_t Offset() const {
        return offset_;
    }
    int64_t BitSize() const {
        return bit_size_;
    }
    int64_t ByteLength() const {
        return byte_length_;
    }

    Status Set(unsigned int index);
    bool Get(unsigned int index);
    void Clear();

 private:
    static constexpr int32_t BYTE_INDEX_MASK = 0x00000007;

 private:
    int64_t byte_length_;
    int64_t bit_size_;
    int32_t offset_;
    std::shared_ptr<MemorySegment> segment_;
};
}  // namespace paimon
