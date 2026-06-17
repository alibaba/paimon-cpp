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
    explicit TargetRowGroup(int32_t rg_index) : row_group_index(rg_index) {}
    TargetRowGroup(int32_t rg_index, bool is_partially_matched, RowRanges ranges)
        : row_group_index(rg_index),
          is_partially_matched(is_partially_matched),
          row_ranges(std::move(ranges)) {}

    TargetRowGroup(const TargetRowGroup& other) = default;

    bool IsExcludedByReadRange() const {
        return excluded_by_read_range;
    }

    void SetExcludedByReadRange(bool excluded) {
        excluded_by_read_range = excluded;
    }

    int32_t GetRowGroupIndex() const {
        return row_group_index;
    }

    bool IsPartiallyMatched() const {
        return is_partially_matched;
    }

    const RowRanges& GetRowRanges() const {
        return row_ranges;
    }

    static TargetRowGroups MakeSerialRowGroups(int32_t num_row_groups) {
        TargetRowGroups target_row_groups;
        target_row_groups.reserve(num_row_groups);
        for (int32_t i = 0; i < num_row_groups; ++i) {
            target_row_groups.emplace_back(i);
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
    int32_t row_group_index{-1};
    bool is_partially_matched{false};
    // page-filtered row ranges, only valid if is_partially_matched is true.
    RowRanges row_ranges;
    // Whether this row group has been excluded by ApplyReadRanges.
    // When true, this row group is logically skipped during iteration
    // but retained so that a subsequent wider ApplyReadRanges can restore it.
    bool excluded_by_read_range{false};
};

}  // namespace paimon::parquet
