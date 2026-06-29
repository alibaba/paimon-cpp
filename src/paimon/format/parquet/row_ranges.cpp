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

#include "paimon/format/parquet/row_ranges.h"

#include <algorithm>
#include <string>

namespace paimon::parquet {

namespace {

// Returns the union of the two ranges or nullopt if there are elements between them.
// Used by Add to splice an inserted range into the existing sorted-disjoint sequence.
std::optional<RowRanges::Range> UnionRanges(const RowRanges::Range& left,
                                            const RowRanges::Range& right) {
    if (left.from <= right.from) {
        if (left.to + 1 >= right.from) {
            return RowRanges::Range(left.from, std::max(left.to, right.to));
        }
    } else if (right.to + 1 >= left.from) {
        return RowRanges::Range(right.from, std::max(left.to, right.to));
    }
    return std::nullopt;
}

}  // namespace

RowRanges RowRanges::Union(const RowRanges& left, const RowRanges& right) {
    std::vector<Range> combined;
    combined.reserve(left.ranges_.size() + right.ranges_.size());
    combined.insert(combined.end(), left.ranges_.begin(), left.ranges_.end());
    combined.insert(combined.end(), right.ranges_.begin(), right.ranges_.end());
    return RowRanges(Range::SortAndMergeOverlap(combined, /*adjacent=*/true));
}

RowRanges RowRanges::Intersection(const RowRanges& left, const RowRanges& right) {
    return RowRanges(Range::And(left.ranges_, right.ranges_));
}

int64_t RowRanges::RowCount() const {
    int64_t count = 0;
    for (const auto& range : ranges_) {
        count += range.Count();
    }
    return count;
}

bool RowRanges::IsOverlapping(int64_t from, int64_t to) const {
    Range target(from, to);
    auto it = std::lower_bound(ranges_.begin(), ranges_.end(), target,
                               [](const Range& r, const Range& t) { return r.to < t.from; });
    return it != ranges_.end() && it->from <= target.to;
}

void RowRanges::Add(const Range& range) {
    if (ranges_.empty()) {
        ranges_.push_back(range);
        return;
    }

    // Find insertion point using binary search (sorted by 'from')
    auto pos =
        std::lower_bound(ranges_.begin(), ranges_.end(), range,
                         [](const Range& r, const Range& target) { return r.from < target.from; });

    // Scan backward and forward to find all ranges that overlap or are adjacent
    Range merged = range;
    auto merge_begin = pos;
    auto merge_end = pos;

    // Merge with preceding ranges
    while (merge_begin != ranges_.begin()) {
        auto prev = merge_begin - 1;
        auto u = UnionRanges(*prev, merged);
        if (!u.has_value()) break;
        merged = u.value();
        merge_begin = prev;
    }

    // Merge with following ranges
    while (merge_end != ranges_.end()) {
        auto u = UnionRanges(*merge_end, merged);
        if (!u.has_value()) break;
        merged = u.value();
        ++merge_end;
    }

    // Replace [merge_begin, merge_end) with the single merged range
    auto it = ranges_.erase(merge_begin, merge_end);
    ranges_.insert(it, merged);
}

std::optional<int64_t> RowRanges::MapFilteredIndexToOriginalRow(int64_t filtered_index) const {
    int64_t accumulated = 0;
    for (const auto& range : ranges_) {
        int64_t count = range.Count();
        if (filtered_index < accumulated + count) {
            return range.from + (filtered_index - accumulated);
        }
        accumulated += count;
    }
    return std::nullopt;
}

std::string RowRanges::ToString() const {
    if (ranges_.empty()) {
        return "[]";
    }
    std::string result = "[";
    for (size_t i = 0; i < ranges_.size(); ++i) {
        if (i > 0) {
            result += ", ";
        }
        result += ranges_[i].ToString();
    }
    result += "]";
    return result;
}

}  // namespace paimon::parquet
