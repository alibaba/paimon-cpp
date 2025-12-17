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
#include <optional>
#include <string>
#include <vector>

#include "paimon/visibility.h"

namespace paimon {
/// Range represents from (inclusive) and to (inclusive).
struct PAIMON_EXPORT Range {
    Range(int64_t _from, int64_t _to);

    /// Returns the number of integers in the range [from, to].
    int64_t Count() const;

    /// Computes the intersection of two ranges.
    static std::optional<Range> Intersection(const Range& left, const Range& right);

    /// Checks whether two ranges have any overlap.
    static bool HasIntersection(const Range& left, const Range& right);

    /// Sorts a list of ranges by `from`, then merges overlapping or adjacent ranges.
    /// @param ranges Input vector of ranges to merge.
    /// @param adjacent If true, also merges ranges that are adjacent (e.g., [1,3] and [4,5] →
    /// [1,5]).
    ///                 If false, only merges strictly overlapping ranges.
    /// @return A new vector of non-overlapping, sorted ranges.
    static std::vector<Range> SortAndMergeOverlap(const std::vector<Range>& ranges, bool adjacent);

    /// Computes the set intersection of two collections of disjoint, sorted ranges.
    static std::vector<Range> And(const std::vector<Range>& left, const std::vector<Range>& right);

    bool operator==(const Range& other) const;
    bool operator<(const Range& other) const;

    std::string ToString() const;

    int64_t from;
    int64_t to;
};

}  // namespace paimon
