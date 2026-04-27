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

#include <cstdint>
#include <vector>

#include "paimon/result.h"
#include "paimon/utils/range.h"
#include "paimon/visibility.h"

namespace paimon {

/// Index for row ranges. Provides efficient intersection queries over a sorted, non-overlapping
/// collection of ranges using binary search.
class PAIMON_EXPORT RowRangeIndex {
 public:
    /// Creates a RowRangeIndex from the given ranges. The ranges will be sorted and merged
    /// (overlapping and adjacent ranges are combined) before indexing.
    static Result<RowRangeIndex> Create(const std::vector<Range>& ranges);

    /// Returns the sorted, non-overlapping ranges held by this index.
    const std::vector<Range>& Ranges() const;

    /// Returns true if any range in this index intersects with the interval [start, end].
    bool Intersects(int64_t start, int64_t end) const;

    /// Returns the sub-ranges of this index that intersect with the interval [start, end].
    /// Each returned range is clipped to lie within [start, end].
    std::vector<Range> IntersectedRanges(int64_t start, int64_t end) const;

 private:
    explicit RowRangeIndex(std::vector<Range> ranges);

    /// Finds the first index in `ends_` whose value is >= target (lower bound).
    int32_t LowerBound(int64_t target) const;

 private:
    std::vector<Range> ranges_;
    std::vector<int64_t> starts_;
    std::vector<int64_t> ends_;
};

}  // namespace paimon
