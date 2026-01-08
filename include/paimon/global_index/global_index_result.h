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

#include <memory>
#include <string>
#include <utility>

#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/utils/range.h"
#include "paimon/visibility.h"

namespace paimon {
/// Global index result to get selected global row ids.
class PAIMON_EXPORT GlobalIndexResult : public std::enable_shared_from_this<GlobalIndexResult> {
 public:
    virtual ~GlobalIndexResult() = default;

    /// Iterator interface for traversing selected global row ids.
    class Iterator {
     public:
        virtual ~Iterator() = default;

        /// Checks whether more row ids are available.
        virtual bool HasNext() const = 0;

        /// @return The next global row id and advances the iterator.
        virtual int64_t Next() = 0;
    };

    /// Checks whether the global index result contains no matching row ids.
    ///
    /// @return A `Result<bool>` where:
    ///         - `true` indicates the result is empty (no matching rows),
    ///         - `false` indicates at least one matching row exists,
    ///         - An error is returned only if internal state is corrupted or I/O fails
    ///           (e.g., during lazy loading of index data).
    virtual Result<bool> IsEmpty() const = 0;

    /// Creates a new iterator over the selected global row ids.
    virtual Result<std::unique_ptr<Iterator>> CreateIterator() const = 0;

    /// Returns non-overlapping, sorted ranges covering all row ids in `GlobalIndexResult`.
    Result<std::vector<Range>> ToRanges() const;

    /// Computes the logical AND (intersection) between current result and another.
    virtual Result<std::shared_ptr<GlobalIndexResult>> And(
        const std::shared_ptr<GlobalIndexResult>& other);

    /// Computes the logical OR (union) between this result and another.
    virtual Result<std::shared_ptr<GlobalIndexResult>> Or(
        const std::shared_ptr<GlobalIndexResult>& other);

    /// Adds the given offset to each row id in current result and returns the new global index
    /// result.
    virtual Result<std::shared_ptr<GlobalIndexResult>> AddOffset(int64_t offset) = 0;

    virtual std::string ToString() const = 0;

    /// Serializes a GlobalIndexResult object into a byte array.
    ///
    /// @note This method only supports the following concrete implementations:
    ///       - BitmapVectorSearchGlobalIndexResult
    ///       - BitmapGlobalIndexResult
    ///
    /// @param global_index_result The GlobalIndexResult instance to serialize (must not be null).
    /// @param pool Memory pool used to allocate the output byte buffer.
    /// @return A Result containing a unique pointer to the serialized Bytes on success,
    ///         or an error status on failure.
    static Result<PAIMON_UNIQUE_PTR<Bytes>> Serialize(
        const std::shared_ptr<GlobalIndexResult>& global_index_result,
        const std::shared_ptr<MemoryPool>& pool);

    /// Deserializes a GlobalIndexResult object from a raw byte buffer.
    ///
    /// @note The concrete type of the deserialized object is determined by metadata
    ///       embedded in the buffer. Currently, only the following types are supported:
    ///       - BitmapVectorSearchGlobalIndexResult
    ///       - BitmapGlobalIndexResult
    ///
    /// @param buffer Pointer to the serialized byte data (must not be null).
    /// @param length Size of the buffer in bytes.
    /// @param pool Memory pool used to allocate internal objects during deserialization.
    /// @return A Result containing a shared pointer to the reconstructed GlobalIndexResult
    ///         on success, or an error status on failure.
    static Result<std::shared_ptr<GlobalIndexResult>> Deserialize(
        const char* buffer, size_t length, const std::shared_ptr<MemoryPool>& pool);

 private:
    static constexpr int32_t VERSION = 1;
};

/// Represents the result of a vector search query against a global index.
/// This class encapsulates a set of search candidates (row id + score pairs) and provides
/// an iterator interface to traverse them.
class PAIMON_EXPORT VectorSearchGlobalIndexResult : public GlobalIndexResult {
 public:
    /// An iterator over the vector search results, returning (row_id, score) pairs.
    ///
    /// @note The results are **NOT sorted by score**. Instead, they are returned in **ascending
    ///       order of row_id**.
    class VectorSearchIterator {
     public:
        virtual ~VectorSearchIterator() = default;

        /// Checks whether more row ids are available.
        virtual bool HasNext() const = 0;

        /// Retrieves the next (row_id, score) pair and advances the iterator.
        ///
        /// @return A pair where:
        ///   - first: the global row id (returned in ascending order),
        ///   - second: the associated score computed by the index.
        ///
        /// @note The sequence is ordered by **row_id**, not by score.
        virtual std::pair<int64_t, float> NextWithScore() = 0;
    };

    /// Creates a new iterator for traversing the vector search results.
    virtual Result<std::unique_ptr<VectorSearchIterator>> CreateVectorSearchIterator() const = 0;
};
}  // namespace paimon
