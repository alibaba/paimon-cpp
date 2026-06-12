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
#include <vector>

namespace paimon {

/// Cross-file shared context for shared-shredding MAP columns.
///
/// Lifetime: same as the owning writer (e.g. AppendOnlyWriter).
/// Holds per-column K_max and a sliding window of recent max_row_width
/// values to support adaptive K sizing across files.
///
/// - First file: K = K_max (no history).
/// - Subsequent files: K = min(max(recent_max_row_widths), K_max).
class MapSharedShreddingContext {
 public:
    /// @param column_to_k_max Map from logical column index to its K_max (from options).
    explicit MapSharedShreddingContext(const std::map<int32_t, int32_t>& column_to_k_max);

    /// Returns the K to use for each extend column in the next file.
    /// First file returns K_max for all columns; subsequent files adapt
    /// based on recent max_row_width observations.
    std::map<int32_t, int32_t> ComputeNextK() const;

    /// Reports the max row width observed in a completed file, for K adaptation.
    /// @param col_index Logical column index.
    /// @param max_row_width The maximum number of MAP keys in any single row of this file.
    void ReportFileStats(int32_t col_index, int32_t max_row_width);

    /// Returns the set of extend column indices.
    std::vector<int32_t> GetShreddingColumnIndices() const;

 private:
    static constexpr int32_t kWindowSize = 100;

    static int32_t ComputeWindowMax(const std::vector<int32_t>& values);

    /// K_max per extend column, from options.
    std::map<int32_t, int32_t> column_to_k_max_;
    /// Sliding window of recent max_row_width per column, for K adaptation.
    std::map<int32_t, std::vector<int32_t>> recent_max_row_widths_;
};

}  // namespace paimon
