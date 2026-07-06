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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/utils/range.h"

namespace paimon::parquet {

/// RowRanges represents a set of row ranges in a row group.
/// Each range is defined by [from, to] where both are inclusive.
/// This is used for page-level filtering to skip rows that don't match predicates.
class RowRanges {
 public:
    /// A single inclusive range. Aliased to paimon::Range so the parquet code shares the
    /// common range type and helpers (Intersection, And, SortAndMergeOverlap, ...).
    using Range = paimon::Range;

    /// Creates an empty RowRanges.
    RowRanges() = default;

    /// Creates a RowRanges with a single range [from, to].
    explicit RowRanges(const Range& range) : ranges_({range}) {}

    /// Creates a RowRanges from a list of ranges.
    explicit RowRanges(const std::vector<Range>& ranges) : ranges_(ranges) {}

    /// Creates a RowRanges from a list of ranges, taking ownership of the vector.
    explicit RowRanges(std::vector<Range>&& ranges) : ranges_(std::move(ranges)) {}

    /// Creates a RowRanges with a single range [0, row_count - 1].
    static RowRanges CreateSingle(int64_t row_count) {
        if (row_count <= 0) {
            return RowRanges();
        }
        return RowRanges(Range(0, row_count - 1));
    }

    /// Creates an empty RowRanges.
    static RowRanges CreateEmpty() {
        return RowRanges();
    }

    /// Calculates the union of two RowRanges.
    /// The union contains all row indexes that were contained in either of the inputs.
    static RowRanges Union(const RowRanges& left, const RowRanges& right);

    /// Calculates the intersection of two RowRanges.
    /// The intersection contains all row indexes that were contained in both inputs.
    static RowRanges Intersection(const RowRanges& left, const RowRanges& right);

    /// Returns the number of rows in the ranges.
    int64_t RowCount() const;

    /// Returns the ranges.
    const std::vector<Range>& GetRanges() const {
        return ranges_;
    }

    /// Returns true if there are no ranges.
    bool IsEmpty() const {
        return ranges_.empty();
    }

    /// Returns true if the specified range overlaps with any of the ranges.
    bool IsOverlapping(int64_t from, int64_t to) const;

    /// Returns true if the specified row is contained in any of the ranges.
    bool Contains(int64_t row) const {
        return IsOverlapping(row, row);
    }

    /// Adds a range to the end of the list, maintaining sorted disjoint ranges.
    void Add(const Range& range);

    /// Maps a filtered-result index to the original row index within the row group.
    /// For example, if RowRanges = {[10,19], [50,59]}, then:
    ///   MapFilteredIndexToOriginalRow(0)  = 10  (first row of first range)
    ///   MapFilteredIndexToOriginalRow(9)  = 19  (last row of first range)
    ///   MapFilteredIndexToOriginalRow(10) = 50  (first row of second range)
    /// Returns nullopt if filtered_index is out of bounds.
    std::optional<int64_t> MapFilteredIndexToOriginalRow(int64_t filtered_index) const;

    std::string ToString() const;

 private:
    std::vector<Range> ranges_;
};

struct TargetRowGroup {
    int32_t row_group_index{-1};
    bool is_partially_matched{false};
    // page-filtered row ranges, only valid if is_partially_matched is true.
    RowRanges row_ranges;
    // Whether this row group has been excluded by ApplyReadRanges.
    // When true, this row group is logically skipped during iteration
    // but retained so that a subsequent wider ApplyReadRanges can restore it.
    bool excluded_by_read_range{false};

    TargetRowGroup() = default;
    TargetRowGroup(int32_t rg_index, bool is_partially_matched, RowRanges ranges)
        : row_group_index(rg_index),
          is_partially_matched(is_partially_matched),
          row_ranges(std::move(ranges)) {}
};
}  // namespace paimon::parquet
