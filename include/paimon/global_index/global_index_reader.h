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

#include <functional>
#include <memory>
#include <vector>

#include "paimon/global_index/global_index_result.h"
#include "paimon/predicate/function_visitor.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/visibility.h"

namespace paimon {
/// Reads and evaluates filter predicates against a global file index.
///
/// Derived classes are expected to implement the visitor methods (e.g., `VisitEqual`,
/// `VisitIsNull`, etc.) to return index-based results that indicate which
/// row satisfy the given predicate.
///
/// @note All `GlobalIndexResult` objects returned by implementations of this class use **local row
/// ids** that start from 0 — not global row ids in the entire table.
/// The `GlobalIndexResult` can be converted to global row ids by calling `AddOffset()`.
class PAIMON_EXPORT GlobalIndexReader : public FunctionVisitor<std::shared_ptr<GlobalIndexResult>> {
 public:
    /// VisitVectorSearch performs approximate vector similarity search.
    /// @warning `VisitVectorSearch` may return error status when it is incorrectly invoked (e.g.,
    /// BitmapGlobalIndexReader call `VisitVectorSearch`).
    virtual Result<std::shared_ptr<VectorSearchGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) = 0;

    /// @return true if the reader is thread-safe; false otherwise.
    virtual bool IsThreadSafe() const = 0;

    /// @return An identifier representing the index type. (e.g., "bitmap", "lumina").
    virtual std::string GetIndexType() const = 0;
};

}  // namespace paimon
