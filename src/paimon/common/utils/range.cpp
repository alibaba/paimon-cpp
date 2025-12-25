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
#include "paimon/utils/range.h"

#include <algorithm>
#include <cassert>

#include "fmt/format.h"
namespace paimon {
Range::Range(int64_t _from, int64_t _to) : from(_from), to(_to) {
    assert(from <= to);
}

int64_t Range::Count() const {
    return to - from + 1;
}

std::vector<Range> Range::SortAndMergeOverlap(const std::vector<Range>& ranges, bool adjacent) {
    if (ranges.empty() || ranges.size() == 1) {
        return ranges;
    }
    // sort
    std::vector<Range> sorted_ranges = ranges;
    std::sort(sorted_ranges.begin(), sorted_ranges.end(),
              [](const Range& left, const Range& right) { return left.from < right.from; });

    std::vector<Range> results;
    Range current = sorted_ranges[0];

    for (size_t i = 1; i < sorted_ranges.size(); ++i) {
        Range next = sorted_ranges[i];
        // Check if current and next overlap (not just adjacent)
        if (current.to + (adjacent ? 1 : 0) >= next.from) {
            // Merge: extend current range
            current = Range(current.from, std::max(current.to, next.to));
        } else {
            // No overlap: add current to result and move to next
            results.push_back(current);
            current = next;
        }
    }
    // Add the last range
    results.push_back(current);
    return results;
}

std::vector<Range> Range::And(const std::vector<Range>& left, const std::vector<Range>& right) {
    if (left.empty() || right.empty()) {
        return {};
    }
    std::vector<Range> results;
    size_t i = 0;
    size_t j = 0;

    while (i < left.size() && j < right.size()) {
        const Range& lhs = left[i];
        const Range& rhs = right[j];

        // Compute intersection of current ranges
        std::optional<Range> intersect = Range::Intersection(lhs, rhs);
        if (intersect) {
            results.push_back(intersect.value());
        }

        // Advance the pointer of the range that ends earlier
        if (lhs.to <= rhs.to) {
            i++;
        } else {
            j++;
        }
    }

    return results;
}

std::optional<Range> Range::Intersection(const Range& left, const Range& right) {
    int64_t start = std::max(left.from, right.from);
    int64_t end = std::min(left.to, right.to);
    if (start > end) {
        return std::nullopt;
    }
    return Range(start, end);
}

bool Range::HasIntersection(const Range& left, const Range& right) {
    int64_t intersection_start = std::max(left.from, right.from);
    int64_t intersection_end = std::min(left.to, right.to);
    return intersection_start <= intersection_end;
}

std::vector<Range> Range::Exclude(const std::vector<Range>& ranges) const {
    if (ranges.empty()) {
        return {*this};
    }

    // Sort ranges by from
    std::vector<Range> sorted = ranges;
    std::sort(sorted.begin(), sorted.end(),
              [](const Range& left, const Range& right) { return left.from < right.from; });

    std::vector<Range> result;
    int64_t current = from;

    for (const auto& exclude : sorted) {
        // Compute intersection with the current range
        auto intersect = Range::Intersection(Range(current, to), exclude);
        if (!intersect) {
            continue;
        }
        // Add the part before the intersection (if any)
        if (current < intersect.value().from) {
            result.emplace_back(current, intersect.value().from - 1);
        }
        // Move current position past the intersection
        current = intersect.value().to + 1;
        if (current > to) {
            break;
        }
    }
    // Add the remaining part after all exclusions (if any)
    if (current <= to) {
        result.emplace_back(current, to);
    }

    return result;
}

bool Range::operator==(const Range& other) const {
    if (this == &other) {
        return true;
    }
    return from == other.from && to == other.to;
}

bool Range::operator<(const Range& other) const {
    if (from == other.from) {
        return to < other.to;
    }
    return from < other.from;
}

std::string Range::ToString() const {
    return fmt::format("[{}, {}]", from, to);
}

}  // namespace paimon
