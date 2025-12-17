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
#include "paimon/visibility.h"

namespace paimon {
/// Reads and evaluates filter predicates against a global file index.
/// `GlobalIndexReader` is an implementation of the `FunctionVisitor` interface
/// specialized to produce `std::shared_ptr<GlobalIndexResult>` objects.
///
/// Derived classes are expected to implement the visitor methods (e.g., `VisitEqual`,
/// `VisitIsNull`, etc.) to return index-based results that indicate which
/// row satisfy the given predicate.
class PAIMON_EXPORT GlobalIndexReader : public FunctionVisitor<std::shared_ptr<GlobalIndexResult>> {
 public:
    /// TopKPreFilter: A lightweight pre-filtering function applied **before** similarity scoring.
    /// It operates solely on row_id and is typically driven by other global index, such as bitmap,
    /// or range index. This filter enables early pruning of irrelevant candidates (e.g., "only
    /// consider rows with label X"), significantly reducing the search space. Returns true to
    /// include the row in Top-K computation; false to exclude it.
    ///
    /// @note Must be thread-safe.
    using TopKPreFilter = std::function<bool(int64_t)>;

    /// VisitTopK performs approximate top-k similarity search.
    ///
    /// @param k         Number of top results to return.
    /// @param query     The query vector (must match the dimensionality of the indexed vectors).
    /// @param filter    A pre-filter based on row_id, implemented by leveraging other global index
    ///                   structures (e.g., bitmap index) for efficient candidate pruning.
    /// @param predicate A runtime filtering condition that may involve graph traversal of
    ///                   structured attributes. **Using this parameter often yields better
    ///                   filtering accuracy** because during index construction, the underlying
    ///                   graph was built with explicit consideration of field connectivity (e.g.,
    ///                   relationships between attributes). As a result, predicates can leverage
    ///                   this pre-established semantic structure to perform more meaningful and
    ///                   context-aware filtering at query time.
    /// @note All fields referenced in the predicate must have been materialized
    ///       in the index during build to ensure availability.
    /// @note `VisitTopK` is thread-safe while other `VisitXXX` is not.
    virtual Result<std::shared_ptr<TopKGlobalIndexResult>> VisitTopK(
        int32_t k, const std::vector<float>& query, TopKPreFilter filter,
        const std::shared_ptr<Predicate>& predicate) = 0;
};

}  // namespace paimon
