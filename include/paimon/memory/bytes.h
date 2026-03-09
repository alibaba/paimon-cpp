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
#include <string>

#include "paimon/memory/memory_pool.h"
#include "paimon/visibility.h"

namespace paimon {

/// A memory-managed byte array class that provides efficient storage and manipulation of binary
/// data.
///
/// Bytes is a RAII wrapper around raw memory that integrates with Paimon's memory pool system
/// for efficient memory allocation and deallocation. It provides safe access to byte arrays
/// with automatic memory management and supports move semantics for efficient transfers.
class PAIMON_EXPORT Bytes {
 public:
    static const std::shared_ptr<Bytes>& EmptyBytes();
    /// Default constructor creating an empty Bytes object.
    ///
    /// Creates a Bytes object with no allocated memory. The object will have
    /// null data pointer and zero size.
    Bytes() = default;

    /// Constructor that allocates memory of specified size.
    ///
    /// Allocates a new byte array of the given size using the provided memory pool.
    /// The allocated memory is uninitialized.
    ///
    /// @param size Number of bytes to allocate.
    /// @param pool Memory pool to use for allocation.
    Bytes(size_t size, MemoryPool* pool);

    /// Constructor that creates a copy of a string.
    ///
    /// Allocates memory and copies the contents of the provided string into
    /// the new Bytes object.
    ///
    /// @param str String to copy data from.
    /// @param pool Memory pool to use for allocation.
    Bytes(const std::string& str, MemoryPool* pool);

    /// Create a copy of existing Bytes with specified size.
    ///
    /// Creates a new Bytes object by copying data from an existing Bytes object.
    /// The copy will have the specified size, which may be different from the
    /// source object's size (truncation or padding may occur).
    ///
    /// @param bytes Source Bytes object to copy from.
    /// @param size Size of the new Bytes object.
    /// @param pool Memory pool to use for allocation.
    /// @return Unique pointer to the newly created Bytes object.
    static PAIMON_UNIQUE_PTR<Bytes> CopyOf(const Bytes& bytes, size_t size, MemoryPool* pool);

    /// Allocate a new Bytes object with specified length.
    ///
    /// %Factory method that creates a new Bytes object with uninitialized memory
    /// of the specified length.
    ///
    /// @param length Number of bytes to allocate.
    /// @param pool Memory pool to use for allocation.
    /// @return Unique pointer to the newly allocated Bytes object.
    static PAIMON_UNIQUE_PTR<Bytes> AllocateBytes(int32_t length, MemoryPool* pool);

    /// Allocate a new Bytes object from string data.
    ///
    /// %Factory method that creates a new Bytes object and copies the contents
    /// of the provided string into it.
    ///
    /// @param str String to copy data from.
    /// @param pool Memory pool to use for allocation.
    /// @return Unique pointer to the newly allocated Bytes object.
    static PAIMON_UNIQUE_PTR<Bytes> AllocateBytes(const std::string& str, MemoryPool* pool);

    // No copying allowed.
    Bytes(const Bytes&) = delete;
    void operator=(const Bytes&) = delete;
    // move constructor
    Bytes(Bytes&&) noexcept;
    Bytes& operator=(Bytes&&) noexcept;

    ~Bytes();

    bool operator==(const Bytes& other) const;
    bool operator<(const Bytes& other) const;
    char& operator[](size_t idx) const;

    /// Compare this Bytes object with another.
    /// @return Negative value if this < other; 0 if equal; positive value if this > other.
    int32_t compare(const Bytes& other) const;

    /// Get pointer to the raw data.
    /// @return A pointer to the underlying byte array. The pointer remains valid
    /// as long as the Bytes object exists and is not moved from.
    inline char* data() const {
        return data_;
    }

    /// Get the size of the byte array.
    inline size_t size() const {
        return size_;
    }

 private:
    MemoryPool* pool_ = nullptr;
    char* data_ = nullptr;
    size_t size_ = 0;
};

}  // namespace paimon
