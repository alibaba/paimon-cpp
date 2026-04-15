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

#include "paimon/format/parquet/row_ranges.h"

#include <algorithm>
#include <string>

namespace paimon::parquet {

namespace {

// Returns the union of the two ranges or nullopt if there are elements between them.
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

// Returns the intersection of the two ranges or nullopt if they don't overlap.
std::optional<RowRanges::Range> IntersectRanges(const RowRanges::Range& left,
                                                const RowRanges::Range& right) {
    if (left.from <= right.from) {
        if (left.to >= right.from) {
            return RowRanges::Range(right.from, std::min(left.to, right.to));
        }
    } else if (right.to >= left.from) {
        return RowRanges::Range(left.from, std::min(left.to, right.to));
    }
    return std::nullopt;
}

}  // namespace

RowRanges RowRanges::Union(const RowRanges& left, const RowRanges& right) {
    RowRanges result;

    auto it1 = left.ranges_.begin();
    auto it2 = right.ranges_.begin();

    while (it1 != left.ranges_.end() && it2 != right.ranges_.end()) {
        if (it1->from < it2->from) {
            result.Add(*it1);
            ++it1;
        } else {
            result.Add(*it2);
            ++it2;
        }
    }

    while (it1 != left.ranges_.end()) {
        result.Add(*it1);
        ++it1;
    }

    while (it2 != right.ranges_.end()) {
        result.Add(*it2);
        ++it2;
    }

    return result;
}

RowRanges RowRanges::Intersection(const RowRanges& left, const RowRanges& right) {
    RowRanges result;

    size_t right_index = 0;
    for (const auto& l : left.ranges_) {
        for (size_t i = right_index; i < right.ranges_.size(); ++i) {
            const auto& r = right.ranges_[i];
            if (l.IsBefore(r)) {
                break;
            } else if (l.IsAfter(r)) {
                right_index = i + 1;
                continue;
            }
            auto intersection = IntersectRanges(l, r);
            if (intersection.has_value()) {
                result.ranges_.push_back(intersection.value());
            }
        }
    }

    return result;
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
    if (it != ranges_.end() && !it->IsAfter(target)) {
        return true;
    }
    return false;
}

void RowRanges::Add(const Range& range) {
    if (ranges_.empty()) {
        ranges_.push_back(range);
        return;
    }

    Range range_to_add = range;
    for (int i = static_cast<int>(ranges_.size()) - 1; i >= 0; --i) {
        Range& last = ranges_[i];
        // The range to add should not be before the last range
        auto u = UnionRanges(last, range_to_add);
        if (!u.has_value()) {
            break;
        }
        range_to_add = u.value();
        ranges_.erase(ranges_.begin() + i);
    }
    ranges_.push_back(range_to_add);
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
