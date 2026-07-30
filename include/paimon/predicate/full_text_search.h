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
#include "paimon/utils/roaring_bitmap64.h"
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

    FullTextSearch(const std::string& _field_name, std::optional<int32_t> _limit,
                   const std::string& _query, const SearchType& _search_type,
                   const std::optional<RoaringBitmap64>& _pre_filter, bool _with_score = false,
                   std::optional<float> _min_score = std::nullopt)
        : field_name(_field_name),
          limit(_limit),
          query(_query),
          search_type(_search_type),
          pre_filter(_pre_filter),
          with_score(_with_score),
          min_score(_min_score) {}

    std::shared_ptr<FullTextSearch> ReplacePreFilter(
        const std::optional<RoaringBitmap64>& _pre_filter) const {
        return std::make_shared<FullTextSearch>(field_name, limit, query, search_type, _pre_filter,
                                                with_score, min_score);
    }

    /// Name of the field to search within (must be a full-text indexed field).
    std::string field_name;
    /// Maximum number of documents to return. Purely a truncation switch,
    /// orthogonal to `with_score`: set `with_score = true` to get relevance
    /// scores; a non-empty `limit` does not by itself imply scoring.
    std::optional<int32_t> limit;
    /// The query string to search for. The interpretation depends on search_type:
    ///
    /// - For MATCH_ALL/MATCH_ANY: keywords are split into terms using the **same analyzer as
    ///   indexing**.
    ///   Example: "Hello World" → terms ["hello", "world"] (after lowercasing and tokenization).
    ///
    /// - For PHRASE: matches the exact word sequence (with optional slop). Also be analyzed.
    ///
    /// - For PREFIX: matches terms starting with the given string (e.g., "run" → running, runner).
    ///   The query is not tokenized or filtered for stop words. The original prefix is retained,
    ///   and a prefix consisting entirely of ASCII letters and digits is also matched using the
    ///   lowercase case-normalization applied to pure ASCII terms at indexing time.
    ///
    /// - For WILDCARD: supports wildcards * and ? (e.g., "ap*e", "app?e").
    ///   The query is not tokenized or filtered for stop words. The wildcard operators are
    ///   preserved. Both the original pattern and an alternative with each ASCII alphanumeric
    ///   fragment lowercased are matched, covering pure ASCII and mixed ASCII/non-ASCII terms.
    ///
    /// @note Analyzer consistency between indexing and querying is critical for correctness.
    std::string query;
    /// Type of search to perform.
    SearchType search_type;
    /// A pre-filter based on **global row IDs**, implemented by leveraging another global index.
    /// Only rows whose global row ID is present in `pre_filter` will be included during search.
    /// If not set, all rows will be included.
    std::optional<RoaringBitmap64> pre_filter;
    /// Whether to compute and return relevance scores (e.g. BM25). The 4-path matrix:
    /// - `with_score=false, limit=nullopt` → BitmapGlobalIndexResult (all rows, no score)
    /// - `with_score=false, limit=N`       → BitmapGlobalIndexResult (any N matches, unscored)
    /// - `with_score=true,  limit=nullopt` → BitmapScoredGlobalIndexResult (all rows + all scores)
    /// - `with_score=true,  limit=N`       → BitmapScoredGlobalIndexResult (top-N by score +
    /// scores)
    ///
    /// For plain `LIMIT N` without ORDER BY (the common case when an online
    /// engine, e.g. StarRocks, pushes down a predicate) set `with_score=false,
    /// limit=N` — the unscored fast path. For top-N by relevance use
    /// `with_score=true, limit=N` and drop the scores in the caller if unneeded.
    ///
    /// Default is `false` to avoid score computation overhead for callers that don't need it.
    bool with_score = false;
    /// Minimum relevance-score threshold (exclusive); results with score ≤ this value are
    /// excluded. The score is whatever the backend's similarity produces (e.g. BM25 for
    /// tantivy, classic TF-IDF for lucene), so a threshold is not directly comparable across
    /// backends. Only meaningful when scoring is active (`with_score = true` or `limit` set);
    /// applied before truncation so low-score documents never occupy limit slots.
    /// Default is nullopt (no threshold filtering).
    std::optional<float> min_score;
};
}  // namespace paimon
