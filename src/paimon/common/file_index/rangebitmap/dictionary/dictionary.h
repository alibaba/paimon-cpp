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

#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace paimon {

class Dictionary {
 public:
    virtual ~Dictionary() = default;

    /// Finds the code for a given key using two-phase binary search.
    ///
    /// Firstly do a binary search on chunk representative key
    /// If found, return, otherwise, do a binary search inside the chunk
    ///
    /// @param key The key to search for
    /// @return Code (>= 0) if found, or -(insertion_point + 1) if not found.
    virtual Result<int32_t> Find(const Literal& key) = 0;

    /// Finds the key for a given code using two-phase binary search.
    ///
    /// Firstly do a binary search on chunk representative code
    /// If found, return, otherwise, do a binary search inside the chunk
    ///
    /// @param code The key to search for, must be valid
    /// @return Key literal corresponding to the code
    virtual Result<Literal> Find(int32_t code) = 0;

    class Appender {
     public:
        virtual ~Appender() = default;

        /// Appends a (key, code) pair to the dictionary in sorted order.
        ///
        /// This method enforces strict ordering constraints:
        /// - Keys must be in strictly ascending order (each key > previous key)
        /// - Codes must be incrementing integers with a step of one
        /// - Keys cannot be null
        ///
        /// The method automatically manages chunk creation and splitting:
        /// - Creates a new chunk if none exists, first key of the chunk will be put in chunk header
        /// - If current chunk is full or the size limit cannot fit a single key, flushes it and
        /// remove the chunk
        ///
        /// @param key The key to append, must not be null and must be > previous key
        /// @param code The code to associate with the key, must be previous code + 1
        /// @return Status::OK() if successful, or Status::Invalid() if constraints are violated
        virtual Status AppendSorted(const Literal& key, int32_t code) = 0;

        virtual Result<PAIMON_UNIQUE_PTR<Bytes>> Serialize() = 0;
    };
};

}  // namespace paimon
