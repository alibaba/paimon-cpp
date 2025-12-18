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

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>

#include "paimon/memory/bytes.h"
#include "paimon/visibility.h"

namespace paimon {
class MemoryPool;

/// This class represents a piece of memory.
class PAIMON_EXPORT MemorySegment {
 public:
    MemorySegment() = default;

    static MemorySegment Wrap(const std::shared_ptr<Bytes>& buffer) {
        return MemorySegment(buffer);
    }

    static MemorySegment AllocateHeapMemory(int32_t size, MemoryPool* pool) {
        assert(pool);
        return Wrap(Bytes::AllocateBytes(size, pool));
    }

    MemorySegment(const MemorySegment& other) {
        heap_memory_ = other.heap_memory_;
    }

    MemorySegment& operator=(const MemorySegment& other) = default;

    inline int32_t Size() const {
        return heap_memory_->size();
    }
    inline bool IsOffHeap() const {
        return false;
    }
    inline Bytes* GetArray() const {
        return heap_memory_.get();
    }

    inline char Get(int32_t index) const {
        return *(heap_memory_->data() + index);
    }
    inline void Put(int32_t index, char b) {
        (*heap_memory_)[index] = b;
    }
    inline void Get(int32_t index, Bytes* dst) const {
        return Get(index, dst, /*offset=*/0, dst->size());
    }
    inline void Put(int32_t index, const Bytes& src) {
        return Put(index, src, /*offset=*/0, src.size());
    }
    template <typename T>
    inline void Get(int32_t index, T* dst, int32_t offset, int32_t length) const {
        // check the byte array offset and length and the status
        assert(static_cast<int32_t>(dst->size()) >= (offset + length));
        assert(static_cast<int32_t>(heap_memory_->size()) >= (index + length));
        std::memcpy(const_cast<char*>(dst->data()) + offset, heap_memory_->data() + index, length);
    }

    template <typename T>
    inline void Put(int32_t index, const T& src, int32_t offset, int32_t length) {
        // check the byte array offset and length
        assert(static_cast<int32_t>(src.size()) >= (offset + length));
        assert(static_cast<int32_t>(heap_memory_->size()) >= (index + length));
        std::memcpy(heap_memory_->data() + index, src.data() + offset, length);
    }

    // only support: bool, char, int16_t, int32_t, int64_t, double, float
    template <typename T>
    T GetValue(int32_t index) const {
        static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
        T value;
        std::memcpy(&value, heap_memory_->data() + index, sizeof(T));
        return value;
    }

    // only support: bool, char, int16_t, int32_t, int64_t, double, float
    template <typename T>
    void PutValue(int32_t index, const T& value) {
        static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
        std::memcpy(heap_memory_->data() + index, &value, sizeof(T));
    }

    // TODO(yonghao.fyh): support bulk read / write

    void CopyTo(int32_t offset, MemorySegment* target, int32_t target_offset,
                int32_t num_bytes) const {
        assert(offset >= 0);
        assert(target_offset >= 0);
        assert(num_bytes >= 0);
        std::memcpy(target->heap_memory_->data() + target_offset, heap_memory_->data() + offset,
                    num_bytes);
    }

    void CopyToUnsafe(int32_t offset, void* target, int32_t target_offset,
                      int32_t num_bytes) const {
        std::memcpy(static_cast<char*>(target) + target_offset, heap_memory_->data() + offset,
                    num_bytes);
    }

    int32_t Compare(const MemorySegment& seg2, int32_t offset1, int32_t offset2, int32_t len) const;

    int32_t Compare(const MemorySegment& seg2, int32_t offset1, int32_t offset2, int32_t len1,
                    int32_t len2) const;

    void SwapBytes(Bytes* temp_buffer, MemorySegment* seg2, int32_t offset1, int32_t offset2,
                   int32_t len);
    /// Equals two memory segment regions.
    ///
    /// @param seg2 Segment to equal this segment with
    /// @param offset1 Offset of this segment to start equaling
    /// @param offset2 Offset of seg2 to start equaling
    /// @param length Size of the equaled memory region
    /// @return true if equal, false otherwise
    bool EqualTo(const MemorySegment& seg2, int32_t offset1, int32_t offset2, int32_t length) const;

    /// Get the heap byte array object.
    ///
    /// @return Return non-null if the memory is on the heap, and return null if the
    /// memory if off the heap.
    std::shared_ptr<Bytes> GetHeapMemory() const {
        return heap_memory_;
    }

 private:
    explicit MemorySegment(const std::shared_ptr<Bytes>& heap_memory) : heap_memory_(heap_memory) {
        assert(heap_memory_);
    }

    std::shared_ptr<Bytes> heap_memory_;
};

template <>
inline bool MemorySegment::GetValue(int32_t index) const {
    return Get(index) != 0;
}

template <>
inline void MemorySegment::PutValue(int32_t index, const bool& value) {
    Put(index, static_cast<char>(value ? 1 : 0));
}

}  // namespace paimon
