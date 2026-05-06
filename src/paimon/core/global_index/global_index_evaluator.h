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
#include <optional>

#include "paimon/global_index/global_index_result.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/utils/row_range_index.h"
#include "paimon/visibility.h"

namespace paimon {
/// Abstract base class for evaluating predicates against a global index.
class PAIMON_EXPORT GlobalIndexEvaluator {
 public:
    virtual ~GlobalIndexEvaluator() = default;
    /// Evaluates a predicate against the global index.
    ///
    /// @param predicate       The filter predicate to evaluate.
    /// @param row_range_index Optional row range that limits evaluation to the given
    ///                        ranges of row ids. Index files whose row range does not
    ///                        intersect with `row_range_index` will be skipped. If a field has
    ///                        no usable index file in the requested range, the evaluator
    ///                        returns `nullptr` for that field.
    /// @return A `Result` containing:
    ///         - `nullptr` if the predicate cannot be evaluated by this index (e.g., field has
    ///         no index, or no index file intersects with `row_range_index`),
    ///         - A `std::shared_ptr<GlobalIndexResult>` if evaluation succeeds.
    ///         The `GlobalIndexResult` indicates the matching rows (e.g., via row ID bitmaps).
    virtual Result<std::shared_ptr<GlobalIndexResult>> Evaluate(
        const std::shared_ptr<Predicate>& predicate,
        const std::optional<RowRangeIndex>& row_range_index) = 0;
};

}  // namespace paimon
