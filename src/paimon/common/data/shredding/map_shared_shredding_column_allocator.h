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
#include <map>
#include <set>
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

/// Allocates shared-shredding MAP field ids to K physical columns on a per-row basis,
/// and accumulates field-level metadata (field_to_columns, overflow_field_set, max_row_width).
class MapSharedShreddingColumnAllocator {
 public:
    virtual ~MapSharedShreddingColumnAllocator() = default;

    /// Allocates physical columns for one row's field ids.
    /// @param field_ids Field ids present in this row.
    /// @return Allocation result with column assignments and overflow list.
    virtual RowAllocation AllocateRow(const std::vector<int32_t>& field_ids) = 0;

    /// Returns accumulated field_id -> set of column indices (for MapSharedShreddingFileMeta).
    const std::map<int32_t, std::set<int32_t>>& GetFieldToColumns() const;

    /// Returns accumulated overflow field id set (for MapSharedShreddingFileMeta).
    const std::set<int32_t>& GetOverflowFieldSet() const;

    /// Returns the maximum row width observed so far.
    int32_t GetMaxRowWidth() const;

 protected:
    /// @param num_columns Number of physical columns K for this shared-shredding MAP column.
    explicit MapSharedShreddingColumnAllocator(int32_t num_columns);

    /// Commits a planned row allocation and updates accumulated metadata.
    /// @param allocation Allocation materialized for the current row.
    /// @param field_ids Field ids after allocator-specific preparation.
    void CommitRow(const RowAllocation& allocation, const std::vector<int32_t>& field_ids);

    int32_t num_columns_;

 private:
    // ---- Accumulated field-level metadata ----
    std::map<int32_t, std::set<int32_t>> field_to_columns_;
    std::set<int32_t> overflow_field_set_;
    int32_t max_row_width_ = 0;
};

}  // namespace paimon
