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
#include <vector>

#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/table/source/split.h"
#include "paimon/visibility.h"

namespace paimon {
class MemoryPool;
class ReadContext;

/// Given a `Split` or a list of `Split`, generate a reader for batch reading.
class PAIMON_EXPORT TableRead {
 public:
    virtual ~TableRead() = default;

    /// Create an instance of `TableRead`.
    ///
    /// @param context A unique pointer to the `ReadContext` used for read operations.
    /// @return A Result containing a unique pointer to the `TableRead` instance.
    static Result<std::unique_ptr<TableRead>> Create(std::unique_ptr<ReadContext> context);

    /// Creates a `BatchReader` instance for reading data.
    ///
    /// This method creates a BatchReader that will be responsible for reading data from the
    /// provided splits.
    ///
    /// @param splits A vector of shared pointers to `Split` instances representing the
    ///                    data to be read.
    /// @return A Result containing a unique pointer to the `BatchReader` instance.
    /// @note `BatchReader`s created by the same `TableRead` are not thread-safe for
    /// concurrent reading.
    virtual Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::vector<std::shared_ptr<Split>>& splits);

    /// Creates a `BatchReader` instance for a single split.
    ///
    /// @param split A shared pointer to the `Split` instance that defines the data to be
    ///                   read.
    /// @return A Result containing a unique pointer to the `BatchReader` instance.
    virtual Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::shared_ptr<Split>& split) = 0;

 protected:
    explicit TableRead(const std::shared_ptr<MemoryPool>& memory_pool);

 private:
    std::shared_ptr<MemoryPool> pool_;
};
}  // namespace paimon
