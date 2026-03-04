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
#include <memory>
#include <vector>

#include "paimon/utils/range.h"
#include "paimon/utils/roaring_bitmap64.h"
#include "paimon/visibility.h"

namespace paimon {

/**
 * A compressed bitmap for 64-bit integer aggregated by tree.
 * This is a wrapper around RoaringBitmap64 that provides additional functionality
 * and a more convenient interface.
 */
class PAIMON_EXPORT RoaringNavigableMap64 {
 public:
    /// Default constructor creates an empty bitmap
    RoaringNavigableMap64();

    /// Copy constructor
    RoaringNavigableMap64(const RoaringNavigableMap64& other);

    /// Move constructor
    RoaringNavigableMap64(RoaringNavigableMap64&& other) noexcept;

    /// Copy assignment operator
    RoaringNavigableMap64& operator=(const RoaringNavigableMap64& other);

    /// Move assignment operator
    RoaringNavigableMap64& operator=(RoaringNavigableMap64&& other) noexcept;

    /// Destructor
    ~RoaringNavigableMap64();

    /**
     * Adds a range of values to the bitmap.
     * @param range The range to add (inclusive of both endpoints)
     */
    void AddRange(const Range& range);

    /**
     * Checks if the bitmap contains the given value.
     * @param x The value to check
     * @return true if the value is in the bitmap, false otherwise
     */
    bool Contains(int64_t x) const;

    /**
     * Adds a single value to the bitmap.
     * @param x The value to add
     */
    void Add(int64_t x);

    /**
     * Performs a bitwise OR operation with another bitmap.
     * @param other The other bitmap to OR with
     */
    void Or(const RoaringNavigableMap64& other);

    /**
     * Performs a bitwise AND operation with another bitmap.
     * @param other The other bitmap to AND with
     */
    void And(const RoaringNavigableMap64& other);

    /**
     * Performs a bitwise AND NOT operation with another bitmap.
     * This removes all elements from this bitmap that are present in the other bitmap.
     * @param other The other bitmap to AND NOT with
     */
    void AndNot(const RoaringNavigableMap64& other);

    /**
     * Checks if the bitmap is empty.
     * @return true if the bitmap contains no elements, false otherwise
     */
    bool IsEmpty() const;

    /**
     * Optimizes the bitmap by applying run-length encoding.
     * @return true if the bitmap was modified, false otherwise
     */
    bool RunOptimize();

    /**
     * Gets the cardinality of the bitmap as a 64-bit integer.
     * @return The number of elements in the bitmap
     */
    int64_t GetLongCardinality() const;

    /**
     * Gets the cardinality of the bitmap as a 32-bit integer.
     * @return The number of elements in the bitmap (truncated to 32 bits)
     */
    int32_t GetIntCardinality() const;

    /**
     * Clears all elements from the bitmap.
     */
    void Clear();

    /**
     * Serializes the bitmap to a byte array.
     * @return A vector containing the serialized bitmap
     */
    std::vector<uint8_t> Serialize() const;

    /**
     * Deserializes the bitmap from a byte array.
     * @param data The byte array containing the serialized bitmap
     */
    void Deserialize(const std::vector<uint8_t>& data);

    /**
     * Converts this bitmap to a list of contiguous ranges.
     * This is useful for interoperability with APIs that expect std::vector<Range>.
     * @return A vector of ranges representing the bitmap
     */
    std::vector<Range> ToRangeList() const;

    /**
     * Gets the internal RoaringBitmap64 without copying.
     * This is an optimization to avoid O(n) conversion when the navigable map
     * is no longer needed for modifications.
     * @return A const reference to the internal RoaringBitmap64
     */
    const RoaringBitmap64& GetBitmap() const;

    /**
     * Creates a new bitmap from a list of values.
     * @param values The values to include in the bitmap
     * @return A new RoaringNavigableMap64 containing the specified values
     */
    static RoaringNavigableMap64 BitmapOf(const std::vector<int64_t>& values);

    /**
     * Computes the intersection of two bitmaps.
     * @param x1 The first bitmap
     * @param x2 The second bitmap
     * @return A new bitmap containing the intersection of the two input bitmaps
     */
    static RoaringNavigableMap64 And(const RoaringNavigableMap64& x1,
                                     const RoaringNavigableMap64& x2);

    /**
     * Computes the union of two bitmaps.
     * @param x1 The first bitmap
     * @param x2 The second bitmap
     * @return A new bitmap containing the union of the two input bitmaps
     */
    static RoaringNavigableMap64 Or(const RoaringNavigableMap64& x1,
                                    const RoaringNavigableMap64& x2);

    /**
     * Equality operator.
     * @param other The other bitmap to compare with
     * @return true if the bitmaps are equal, false otherwise
     */
    bool operator==(const RoaringNavigableMap64& other) const;

    /**
     * Inequality operator.
     * @param other The other bitmap to compare with
     * @return true if the bitmaps are not equal, false otherwise
     */
    bool operator!=(const RoaringNavigableMap64& other) const;

    /**
     * Iterator class for iterating over the values in the bitmap.
     */
    class Iterator {
     public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = int64_t;
        using difference_type = std::ptrdiff_t;
        using pointer = const int64_t*;
        using reference = const int64_t&;

        explicit Iterator(const RoaringNavigableMap64& bitmap);
        Iterator(const Iterator& other);
        Iterator(Iterator&& other) noexcept;
        Iterator& operator=(const Iterator& other);
        Iterator& operator=(Iterator&& other) noexcept;
        ~Iterator();

        int64_t operator*() const;
        Iterator& operator++();
        Iterator operator++(int);
        bool operator==(const Iterator& other) const;
        bool operator!=(const Iterator& other) const;

     private:
        friend class RoaringNavigableMap64;

        class Impl;
        std::unique_ptr<Impl> impl_;

        // Private constructor for creating end iterator
        Iterator(const RoaringNavigableMap64& bitmap, bool is_end);
    };

    /**
     * Returns an iterator to the beginning of the bitmap.
     * @return Iterator pointing to the first element
     */
    Iterator begin() const;

    /**
     * Returns an iterator to the end of the bitmap.
     * @return Iterator pointing to the end
     */
    Iterator end() const;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon