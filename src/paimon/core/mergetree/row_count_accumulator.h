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

#include "paimon/core/mergetree/compact/sort_merge_reader.h"
#include "paimon/result.h"

namespace paimon {

/// Counts rows from a merged KeyValue stream after delete rows are dropped.
class RowCountAccumulator {
 public:
    /// @param merged_reader  The merged reader. Must be wrapped with DropDeleteReader
    ///                       so that only valid (non-deleted) KeyValue objects are output.
    explicit RowCountAccumulator(std::unique_ptr<SortMergeReader>&& merged_reader);

    ~RowCountAccumulator() = default;

    /// Count all valid rows from the merge reader.
    /// Iterates through all merged+deduplicated+non-deleted KeyValue objects.
    Result<int64_t> CountAll();

    /// Close underlying readers and release resources.
    void Close();

 private:
    std::unique_ptr<SortMergeReader> merged_reader_;
};

}  // namespace paimon
