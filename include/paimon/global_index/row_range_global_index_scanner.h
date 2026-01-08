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

#include "paimon/global_index/global_index_reader.h"
#include "paimon/visibility.h"

namespace paimon {
/// Interface for scanning global index data at the range level.
class PAIMON_EXPORT RowRangeGlobalIndexScanner {
 public:
    virtual ~RowRangeGlobalIndexScanner() = default;

    /// Creates a `GlobalIndexReader` for a specific field and index type within this range.
    ///
    /// This reader provides low-level access to the serialized index data
    /// for the given column (`field_name`) and index kind (`index_type`, such as "bitmap").
    ///
    /// @param field_name  Name of the indexed column.
    /// @param index_type  Type of the global index (e.g., "bitmap", "lumina").
    /// @return A `Result` that is:
    ///         - Successful with a non-null reader if the index exists and loads correctly;
    ///         - Successful with a null pointer if no index was built for the given field and type;
    ///         - An error only if loading fails (e.g., file corruption, I/O error, unsupported
    ///         format).
    /// @note All `GlobalIndexResult` objects returned by `GlobalIndexReader` use **local row
    /// ids** that start from 0 — not global row ids in the entire table.
    virtual Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        const std::string& field_name, const std::string& index_type) const = 0;

    /// Creates several `GlobalIndexReader`s for a specific field within this range.
    ///
    /// @param field_name  Name of the indexed column.
    /// @return A `Result` that is:
    ///         - Successful with several readers if the indexes exist and load correctly;
    ///         - Successful with an empty vector if no index was built for the given field;
    ///         - Error returns when loading fails (e.g., file corruption, I/O error, unsupported
    ///           format).
    virtual Result<std::vector<std::shared_ptr<GlobalIndexReader>>> CreateReaders(
        const std::string& field_name) const = 0;
};

}  // namespace paimon
