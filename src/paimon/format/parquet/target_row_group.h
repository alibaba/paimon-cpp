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

#include "paimon/format/parquet/row_ranges.h"

namespace paimon::parquet {
class TargetRowGroup;
using TargetRowGroups = std::vector<TargetRowGroup>;
class TargetRowGroup {
 public:
    TargetRowGroup(int32_t rg_index, bool is_partially_matched, RowRanges ranges)
        : row_group_index_(rg_index),
          is_partially_matched_(is_partially_matched),
          row_ranges_(std::move(ranges)) {}

    TargetRowGroup(const TargetRowGroup& other) = default;
    TargetRowGroup& operator=(const TargetRowGroup& other) = default;

    bool IsExcludedByReadRange() const {
        return excluded_by_read_range_;
    }

    void SetExcludedByReadRange(bool excluded) {
        excluded_by_read_range_ = excluded;
    }

    int32_t GetRowGroupIndex() const {
        return row_group_index_;
    }

    bool IsPartiallyMatched() const {
        return is_partially_matched_;
    }

    const RowRanges& GetRowRanges() const {
        return row_ranges_;
    }

    // Create a list of TargetRowGroups for serial (non-filtered) reading.
    //
    // Each element in 'ranges' is a (start, end) pair describing the row
    // range of a single row group. The vector index 'i' is used as the row
    // group index, so the caller must ensure that 'ranges' is ordered to
    // match the physical row-group order in the file.
    //
    // For each valid range (start < end), a TargetRowGroup is created with:
    //   - row_group_index = i
    //   - is_partially_matched = false  (the entire row group is read)
    //   - row_ranges = [0, end - start - 1]  (local row indices covering the
    //     full group; converted from absolute offsets to 0-based local offsets)
    //
    // Ranges where start >= end are treated as empty and skipped.
    static TargetRowGroups MakeForAllRowGroups(
        const std::vector<std::pair<uint64_t, uint64_t>>& ranges) {
        TargetRowGroups target_row_groups;
        target_row_groups.reserve(ranges.size());
        for (size_t i = 0; i < ranges.size(); ++i) {
            // Skip empty or invalid ranges.
            if (ranges[i].first >= ranges[i].second) {
                continue;
            }
            // Convert the absolute [start, end) pair into a 0-based local
            // row range [0, row_count - 1] for this row group.
            target_row_groups.emplace_back(
                static_cast<int32_t>(i), false,
                RowRanges(Range(0, ranges[i].second - ranges[i].first - 1)));
        }
        return target_row_groups;
    }

    static std::vector<int32_t> GetRowGroupIndices(const TargetRowGroups& target_row_groups) {
        std::vector<int32_t> indices;
        indices.reserve(target_row_groups.size());
        for (const auto& rg : target_row_groups) {
            indices.push_back(rg.GetRowGroupIndex());
        }
        return indices;
    }

 private:
    int32_t row_group_index_{-1};
    bool is_partially_matched_{false};
    // Local row ranges
    RowRanges row_ranges_;
    // Whether this row group has been excluded by ApplyReadRanges.
    // When true, this row group is logically skipped during iteration
    // but retained so that a subsequent wider ApplyReadRanges can restore it.
    bool excluded_by_read_range_{false};
};

}  // namespace paimon::parquet
