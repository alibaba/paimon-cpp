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

#include "paimon/global_index/global_index_result.h"
#include "paimon/predicate/predicate.h"
#include "paimon/visibility.h"

namespace paimon {
/// Abstract base class for evaluating predicates against a global index.
class PAIMON_EXPORT GlobalIndexEvaluator {
 public:
    virtual ~GlobalIndexEvaluator() = default;
    /// Evaluates a predicate against the global index.
    ///
    /// @param predicate The filter predicate to evaluate.
    /// @return A `Result` containing:
    ///         - `std::nullopt` if the predicate cannot be evaluated by this index (e.g., field has
    ///         no index),
    ///         - A `std::shared_ptr<GlobalIndexResult>` if evaluation succeeds.
    ///         The `GlobalIndexResult` indicates the matching rows (e.g., via row ID bitmaps).
    ///
    /// @note Top-K predicates are **not handled** by this method. Use
    ///       `GlobalIndexReader::VisitTopK()` for Top-K specific index evaluation.
    virtual Result<std::optional<std::shared_ptr<GlobalIndexResult>>> Evaluate(
        const std::shared_ptr<Predicate>& predicate) = 0;
};

}  // namespace paimon
