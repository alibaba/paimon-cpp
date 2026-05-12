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

#include "paimon/utils/row_range_index.h"

#include <algorithm>
#include <utility>

namespace paimon {

RowRangeIndex::RowRangeIndex(std::vector<Range> ranges) : ranges_(std::move(ranges)) {
    starts_.reserve(ranges_.size());
    ends_.reserve(ranges_.size());
    for (const auto& range : ranges_) {
        starts_.push_back(range.from);
        ends_.push_back(range.to);
    }
}

Result<RowRangeIndex> RowRangeIndex::Create(const std::vector<Range>& ranges) {
    if (ranges.empty()) {
        return Status::Invalid("Ranges cannot be empty in RowRangeIndex");
    }
    return RowRangeIndex(Range::SortAndMergeOverlap(ranges, /*adjacent=*/true));
}

const std::vector<Range>& RowRangeIndex::Ranges() const {
    return ranges_;
}

bool RowRangeIndex::Intersects(int64_t start, int64_t end) const {
    int32_t candidate = LowerBound(start);
    return candidate < static_cast<int32_t>(starts_.size()) && starts_[candidate] <= end;
}

std::vector<Range> RowRangeIndex::IntersectedRanges(int64_t start, int64_t end) const {
    int32_t left = LowerBound(start);
    if (left >= static_cast<int32_t>(ranges_.size())) {
        return {};
    }

    int32_t right = LowerBound(end);
    if (right >= static_cast<int32_t>(ranges_.size())) {
        right = static_cast<int32_t>(ranges_.size()) - 1;
    }

    if (starts_[left] > end) {
        return {};
    }

    std::vector<Range> expected;

    // Add the first intersecting range, clipped to [start, end].
    const Range& first_range = ranges_[left];
    expected.emplace_back(std::max(start, first_range.from), std::min(end, first_range.to));

    // Add all fully contained ranges between first and last.
    for (int32_t i = left + 1; i < right; ++i) {
        expected.push_back(ranges_[i]);
    }

    // Add the last intersecting range (if different from the first), clipped to [start, end].
    if (right != left) {
        const Range& last_range = ranges_[right];
        if (last_range.from <= end) {
            expected.emplace_back(std::max(start, last_range.from), std::min(end, last_range.to));
        }
    }

    return expected;
}

int32_t RowRangeIndex::LowerBound(int64_t target) const {
    int32_t left = 0;
    auto right = static_cast<int32_t>(ends_.size());
    while (left < right) {
        int32_t mid = left + (right - left) / 2;
        if (ends_[mid] < target) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

}  // namespace paimon
