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

#include <algorithm>
#include <cstdint>
#include <map>
#include <set>
#include <utility>
#include <vector>

namespace paimon {

/// Per-row allocation result produced by MapSharedShreddingColumnAllocator.
struct RowAllocation {
    /// Physical column assignments: col_to_field[col_index] = field_id.
    /// Length is always K. Unused columns have value -1.
    std::vector<int32_t> col_to_field;

    /// Field ids that overflowed (more fields than K physical columns).
    std::vector<int32_t> overflow_fields;
};

/// Allocates MAP field ids to K physical columns on a per-row basis,
/// and accumulates field-level metadata (field_to_columns, overflow_field_set, max_row_width).
///
/// This is a trivial implementation: each row simply assigns columns 0..min(N,K)-1
/// in order, with no LRU eviction.
/// TODO(jinli.zjw): support LRU
class MapSharedShreddingColumnAllocator {
 public:
    /// @param num_columns Number of physical columns K for this shared-shredding MAP column.
    explicit MapSharedShreddingColumnAllocator(int32_t num_columns);

    /// Allocates physical columns for one row's field ids.
    /// @param field_ids The field ids present in this row (order matters for fake impl).
    /// @return Allocation result with column assignments and overflow list.
    RowAllocation AllocateRow(const std::vector<int32_t>& field_ids);

    /// Returns accumulated field_id -> set of column indices (for MapSharedShreddingFileMeta).
    const std::map<int32_t, std::set<int32_t>>& GetFieldToColumns() const;

    /// Returns accumulated overflow field id set (for MapSharedShreddingFileMeta).
    const std::set<int32_t>& GetOverflowFieldSet() const;

    /// Returns the maximum row width observed so far.
    int32_t GetMaxRowWidth() const;

    /// Returns the number of physical columns K.
    int32_t GetNumColumns() const;

 private:
    int32_t num_columns_;

    // ---- Accumulated field-level metadata ----
    std::map<int32_t, std::set<int32_t>> field_to_columns_;
    std::set<int32_t> overflow_field_set_;
    int32_t max_row_width_ = 0;
};

}  // namespace paimon
