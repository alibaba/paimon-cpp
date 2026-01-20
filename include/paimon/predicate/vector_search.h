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
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/predicate/predicate.h"
#include "paimon/visibility.h"

namespace paimon {
/// `VectorSearch` to perform vector similarity search.
struct PAIMON_EXPORT VectorSearch {
    /// `PreFilter`: A lightweight pre-filtering function applied **before** similarity
    /// scoring. It operates solely on **local row ids** and is typically driven by other global
    /// index, such as bitmap, or range index. This filter enables early pruning of irrelevant
    /// candidates (e.g., "only consider rows with label X"), significantly reducing the search
    /// space. Returns true to include the row in vector search process; false to exclude it.
    ///
    /// @note Must be thread-safe.
    using PreFilter = std::function<bool(int64_t)>;

    /// Enumeration of distance or similarity metrics for vector comparison.
    enum class DistanceType { EUCLIDEAN = 1, INNER_PRODUCT = 2, COSINE = 3, UNKNOWN = 128 };

    VectorSearch(const std::string& _field_name, int32_t _limit, const std::vector<float>& _query,
                 PreFilter _pre_filter, const std::shared_ptr<Predicate>& _predicate,
                 const std::optional<DistanceType>& _distance_type,
                 const std::map<std::string, std::string>& _options)
        : field_name(_field_name),
          limit(_limit),
          query(_query),
          pre_filter(_pre_filter),
          predicate(_predicate),
          distance_type(_distance_type),
          options(_options) {}

    std::shared_ptr<VectorSearch> ReplacePreFilter(PreFilter _pre_filter) const {
        return std::make_shared<VectorSearch>(field_name, limit, query, _pre_filter, predicate,
                                              distance_type, options);
    }

    /// Search field name.
    std::string field_name;
    /// Number of top results to return.
    int32_t limit;
    /// The query vector (must match the dimensionality of the indexed vectors).
    std::vector<float> query;
    /// A pre-filter based on **local row ids**, implemented by leveraging other global index
    std::function<bool(int64_t)> pre_filter;
    /// A runtime filtering condition that may involve graph traversal of
    /// structured attributes. **Using this parameter often yields better
    /// filtering accuracy** because during index construction, the underlying
    /// graph was built with explicit consideration of field connectivity (e.g.,
    /// relationships between attributes). As a result, predicates can leverage
    /// this pre-established semantic structure to perform more meaningful and
    /// context-aware filtering at query time.
    /// @note All fields referenced in the predicate must have been materialized
    ///       in the index during build to ensure availability.
    std::shared_ptr<Predicate> predicate;
    /// The distance metric to use for this query, if explicitly specified.
    /// If set, this value must match the distance type used by the index (e.g., EUCLIDEAN, COSINE).
    /// A mismatch will result in an error during query execution.
    /// If not set (std::nullopt), the query will use the distance type configured in the index.
    std::optional<DistanceType> distance_type;
    /// A key-value map of query-specific runtime options.
    /// Such as the size of candidate list in approximate search or parallelism for this query.
    std::map<std::string, std::string> options;
};
}  // namespace paimon
