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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {
class ByteArrayInputStream;
class RoaringBitmap64;

/// A compressed bitmap for 32-bit integer.
class PAIMON_EXPORT RoaringBitmap32 {
 public:
    RoaringBitmap32();
    ~RoaringBitmap32();

    RoaringBitmap32(const RoaringBitmap32&) noexcept;
    RoaringBitmap32& operator=(const RoaringBitmap32&) noexcept;

    RoaringBitmap32(RoaringBitmap32&&) noexcept;
    RoaringBitmap32& operator=(RoaringBitmap32&&) noexcept;

    class PAIMON_EXPORT Iterator {
     public:
        friend class RoaringBitmap32;
        explicit Iterator(const RoaringBitmap32& bitmap);
        ~Iterator();
        Iterator(const Iterator&) noexcept;
        Iterator(Iterator&&) noexcept;
        Iterator& operator=(const Iterator&) noexcept;
        Iterator& operator=(Iterator&&) noexcept;

        /// Return the current value of iterator.
        int32_t operator*() const;
        /// Move the iterator to next value.
        Iterator& operator++();
        bool operator==(const Iterator& other) const;
        bool operator!=(const Iterator& other) const;

     private:
        void* iterator_ = nullptr;
    };

    static constexpr int32_t MAX_VALUE = std::numeric_limits<int32_t>::max();

    /// @param x value added to bitmap
    void Add(int32_t x);

    /// @param x value added to bitmap
    /// @return false if contain x; true if not contain x
    bool CheckedAdd(int32_t x);

    /// @return true if contain x; false if not contain x
    bool Contains(int32_t x) const;

    /// @return true if bitmap is empty
    bool IsEmpty() const;

    /// @return the cardinality of bitmap, i.e., the number of unique value added
    int32_t Cardinality() const;

    /// Computes the negation of the roaring bitmap within the half-open interval [min, max).
    /// Areas outside the interval are unchanged.
    void Flip(int32_t min, int32_t max);

    /// Adds all values in the half-open interval [min, max).
    void AddRange(int32_t min, int32_t max);

    /// Removes all values in the half-open interval [min, max).
    void RemoveRange(int32_t min, int32_t max);

    /// Contain any value in the half-open interval [min, max).
    bool ContainsAny(int32_t min, int32_t max) const;

    /// Serialize bitmap to bytes.
    /// @note Cannot accurately compare the output byte streams with Java and C++ versions,
    /// as the `runOptimize` function of roaring bitmap may optimize consecutive integers
    /// differently.
    PAIMON_UNIQUE_PTR<Bytes> Serialize(MemoryPool* pool) const;

    /// Deserialize bitmap from input stream.
    Status Deserialize(ByteArrayInputStream* input_stream);

    /// Deserialize bitmap from buffer with begin and length.
    Status Deserialize(const char* begin, size_t length);

    /// @return How many bytes are required to serialize this bitmap.
    size_t GetSizeInBytes() const;

    bool operator==(const RoaringBitmap32& other) const noexcept;

    /// Compute the union of the current bitmap and the provided bitmap,
    /// writing the result in the current bitmap. The provided bitmap is not
    /// modified.
    RoaringBitmap32& operator|=(const RoaringBitmap32& other);

    /// Compute the intersection of the current bitmap and the provided bitmap,
    /// writing the result in the current bitmap. The provided bitmap is not
    /// modified.
    /// @note If you are computing the intersection between several
    /// bitmaps, two-by-two, it is best to start with the smallest bitmap.
    RoaringBitmap32& operator&=(const RoaringBitmap32& other);

    /// Compute the difference of the current bitmap and the provided bitmap,
    /// writing the result in the current bitmap. The provided bitmap is not
    /// modified.
    RoaringBitmap32& operator-=(const RoaringBitmap32& other);

    std::string ToString() const;

    Iterator Begin() const;
    Iterator End() const;
    /// @return the iterator moved to the value which is equal or larger than key
    Iterator EqualOrLarger(int32_t key) const;

    /// Computes the intersection between two bitmaps and returns new bitmap.
    /// The current bitmap and the provided bitmap are unchanged.
    ///
    /// @note If you are computing the intersection between several bitmaps, two-by-two, it is best
    /// to start with the smallest bitmap. Consider also using the operator &= to avoid needlessly
    /// creating many temporary bitmaps.
    static RoaringBitmap32 And(const RoaringBitmap32& lhs, const RoaringBitmap32& rhs);

    /// Computes the union between two bitmaps and returns new bitmap.
    /// The current bitmap and the provided bitmap are unchanged.
    static RoaringBitmap32 Or(const RoaringBitmap32& lhs, const RoaringBitmap32& rhs);

    /// Computes the difference between two bitmaps and returns new bitmap.
    /// The current bitmap and the provided bitmap are unchanged.
    static RoaringBitmap32 AndNot(const RoaringBitmap32& lhs, const RoaringBitmap32& rhs);

    /// @return a bitmap contains input values
    static RoaringBitmap32 From(const std::vector<int32_t>& values);

    /// Fast union multiple bitmaps.
    static RoaringBitmap32 FastUnion(const std::vector<const RoaringBitmap32*>& inputs);

    /// Fast union multiple bitmaps.
    static RoaringBitmap32 FastUnion(const std::vector<RoaringBitmap32>& inputs);

    friend class RoaringBitmap64;

 private:
    void* roaring_bitmap_ = nullptr;
};
}  // namespace paimon
