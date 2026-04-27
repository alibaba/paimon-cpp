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
///
/// Supports two modes:
/// - Owning mode: holds a shared_ptr<Bytes> for lifetime management.
/// - Non-owning (view) mode: holds a raw pointer to external data.
///   The caller must ensure the underlying memory outlives this segment.
class PAIMON_EXPORT MemorySegment {
 public:
    MemorySegment() : data_(nullptr), size_(0) {}

    /// Wrap a shared_ptr<Bytes> to create an owning segment.
    static MemorySegment Wrap(const std::shared_ptr<Bytes>& buffer) {
        return MemorySegment(buffer);
    }

    /// Create a non-owning segment that references external memory.
    /// The caller must guarantee that `data` remains valid for the lifetime of this segment.
    static MemorySegment WrapView(const char* data, int32_t size) {
        return MemorySegment(data, size);
    }

    static MemorySegment AllocateHeapMemory(int32_t size, MemoryPool* pool) {
        assert(pool);
        return Wrap(Bytes::AllocateBytes(size, pool));
    }

    MemorySegment(const MemorySegment& other) = default;
    MemorySegment& operator=(const MemorySegment& other) = default;

    bool operator==(const MemorySegment& other) const {
        if (this == &other) {
            return true;
        }
        if (data_ == other.data_ && size_ == other.size_) {
            return true;
        }
        if (!data_ || !other.data_) {
            return false;
        }
        if (size_ != other.size_) {
            return false;
        }
        return std::memcmp(data_, other.data_, size_) == 0;
    }

    inline int32_t Size() const {
        return size_;
    }

    /// Returns the raw data pointer (valid for both owning and non-owning segments).
    inline const char* Data() const {
        return data_;
    }

    inline char Get(int32_t index) const {
        return *(data_ + index);
    }

    /// Returns a mutable pointer to the data. Use with caution on non-owning segments.
    inline char* MutableData() {
        return const_cast<char*>(data_);
    }

    inline void Put(int32_t index, char b) {
        MutableData()[index] = b;
    }

    inline void Get(int32_t index, Bytes* dst) const {
        return Get(index, dst, /*offset=*/0, dst->size());
    }

    inline void Put(int32_t index, const Bytes& src) {
        return Put(index, src, /*offset=*/0, src.size());
    }

    template <typename T>
    inline void Get(int32_t index, T* dst, int32_t offset, int32_t length) const {
        assert(static_cast<int32_t>(dst->size()) >= (offset + length));
        assert(size_ >= (index + length));
        std::memcpy(const_cast<char*>(dst->data()) + offset, data_ + index, length);
    }

    template <typename T>
    inline void Put(int32_t index, const T& src, int32_t offset, int32_t length) {
        assert(static_cast<int32_t>(src.size()) >= (offset + length));
        assert(size_ >= (index + length));
        std::memcpy(MutableData() + index, src.data() + offset, length);
    }

    template <typename T>
    T GetValue(int32_t index) const {
        static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
        T value;
        std::memcpy(&value, data_ + index, sizeof(T));
        return value;
    }

    template <typename T>
    void PutValue(int32_t index, const T& value) {
        static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");

        std::memcpy(MutableData() + index, &value, sizeof(T));
    }

    void CopyTo(int32_t offset, MemorySegment* target, int32_t target_offset,
                int32_t num_bytes) const {
        assert(offset >= 0);
        assert(target_offset >= 0);
        assert(num_bytes >= 0);

        std::memcpy(target->MutableData() + target_offset, data_ + offset, num_bytes);
    }

    void CopyToUnsafe(int32_t offset, void* target, int32_t target_offset,
                      int32_t num_bytes) const {
        std::memcpy(static_cast<char*>(target) + target_offset, data_ + offset, num_bytes);
    }

    int32_t Compare(const MemorySegment& seg2, int32_t offset1, int32_t offset2, int32_t len) const;

    int32_t Compare(const MemorySegment& seg2, int32_t offset1, int32_t offset2, int32_t len1,
                    int32_t len2) const;

    bool EqualTo(const MemorySegment& seg2, int32_t offset1, int32_t offset2, int32_t length) const;

    std::shared_ptr<Bytes> GetOrCreateHeapMemory(MemoryPool* pool) const {
        if (heap_memory_) {
            return heap_memory_;
        }
        if (!data_) {
            return nullptr;
        }
        auto copy = std::make_shared<Bytes>(size_, pool);
        std::memcpy(const_cast<char*>(copy->data()), data_, size_);
        return copy;
    }

 private:
    /// Owning constructor.
    explicit MemorySegment(const std::shared_ptr<Bytes>& heap_memory)
        : heap_memory_(heap_memory),
          data_(heap_memory->data()),
          size_(static_cast<int32_t>(heap_memory->size())) {
        assert(heap_memory_);
    }

    /// Non-owning constructor.
    MemorySegment(const char* data, int32_t size)
        : heap_memory_(nullptr), data_(data), size_(size) {
        assert(data != nullptr || size == 0);
    }

    std::shared_ptr<Bytes> heap_memory_;
    const char* data_;
    int32_t size_;
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
