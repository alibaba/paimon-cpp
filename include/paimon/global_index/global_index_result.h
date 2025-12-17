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

#include "paimon/result.h"
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

    /// Checks whether the global index result contains no matching row IDs.
    ///
    /// @return A `Result<bool>` where:
    ///         - `true` indicates the result is empty (no matching rows),
    ///         - `false` indicates at least one matching row exists,
    ///         - An error is returned only if internal state is corrupted or I/O fails
    ///           (e.g., during lazy loading of index data).
    virtual Result<bool> IsEmpty() const = 0;

    /// Creates a new iterator over the selected global row ids.
    virtual Result<std::unique_ptr<Iterator>> CreateIterator() const = 0;

    /// Computes the logical AND (intersection) between current result and another.
    virtual Result<std::shared_ptr<GlobalIndexResult>> And(
        const std::shared_ptr<GlobalIndexResult>& other);

    /// Computes the logical OR (union) between this result and another.
    virtual Result<std::shared_ptr<GlobalIndexResult>> Or(
        const std::shared_ptr<GlobalIndexResult>& other);

    virtual std::string ToString() const = 0;
};

/// Represents the result of a Top-K query against a global index.
/// This class encapsulates a set of top-K candidates (row ID + score pairs) and provides
/// an iterator interface to traverse them.
class PAIMON_EXPORT TopKGlobalIndexResult : public GlobalIndexResult {
 public:
    /// An iterator over the top-K results, returning (row_id, score) pairs.
    ///
    /// @note The results are **NOT sorted by score**. Instead, they are returned in **ascending
    ///       order of row_id**.
    class TopKIterator {
     public:
        virtual ~TopKIterator() = default;

        /// Checks whether more row IDs are available.
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

    /// Creates a new iterator for traversing the Top-K results.
    virtual Result<std::unique_ptr<TopKIterator>> CreateTopKIterator() const = 0;
};
}  // namespace paimon
