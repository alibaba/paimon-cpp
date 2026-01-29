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
/// A configuration structure for full-text search operations.
struct PAIMON_EXPORT FullTextSearch {
    /// Enumeration of supported full-text search types.
    enum class SearchType {
        /// All terms in the query must be present (AND semantics).
        MATCH_ALL = 1,
        /// Any term in the query can match (OR semantics).
        MATCH_ANY = 2,
        /// Matches the exact sequence of words (with proximity).
        PHRASE = 3,
        /// Matches terms starting with the given string (e.g., "run*" → running, runner).
        PREFIX = 4,
        /// Supports wildcards * and ? (e.g., "ap*e", "app?e" -> "apple").
        WILDCARD = 5,
        /// Default/fallback type for unrecognized or invalid queries.
        UNKNOWN = 128
    };

    FullTextSearch(const std::string& _field_name, int32_t _limit, const std::string& _query,
                   const SearchType& _search_type)
        : field_name(_field_name), limit(_limit), query(_query), search_type(_search_type) {}

    /// Name of the field to search within (must be a full-text indexed field).
    std::string field_name;
    /// Maximum number of documents to return. Ordered by scores.
    int32_t limit;
    /// The query string to search for. The interpretation depends on search_type:
    ///
    /// - For MATCH_ALL/MATCH_ANY: keywords are split into terms using the **same analyzer as
    ///   indexing**.
    ///   Example: "Hello World" → terms ["hello", "world"] (after lowercasing and tokenization).
    ///
    /// - For PHRASE: matches the exact word sequence (with optional slop). Also be analyzed.
    ///
    /// - For PREFIX: matches terms starting with the given string (e.g., "run" → running, runner).
    ///   Only the prefix part is considered; analysis will not be applied.
    ///
    /// - For WILDCARD: supports wildcards * and ? (e.g., "ap*e", "app?e").
    ///   Not passed through analyzer — matched directly against indexed terms.
    ///
    /// @note Analyzer consistency between indexing and querying is critical for correctness.
    std::string query;
    /// Type of search to perform.
    SearchType search_type;
};
}  // namespace paimon
