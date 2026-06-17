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
#include <string>
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
    /// @param column_to_k_max Map from field name to its K_max (from options).
    explicit MapSharedShreddingContext(const std::map<std::string, int32_t>& column_to_k_max);

    /// Returns the K to use for each shared-shredding column in the next file.
    /// First file returns K_max for all columns; subsequent files adapt
    /// based on recent max_row_width observations.
    std::map<std::string, int32_t> ComputeNextK() const;

    /// Reports the max row width observed in a completed file, for K adaptation.
    /// @param field_name Field name of the shared-shredding MAP column.
    /// @param max_row_width The maximum number of MAP keys in any single row of this file.
    void ReportFileStats(const std::string& field_name, int32_t max_row_width);

    /// Returns the set of shared-shredding field names.
    std::vector<std::string> GetShreddingColumnNames() const;

 private:
    static constexpr int32_t kWindowSize = 100;

    static int32_t ComputeWindowMax(const std::vector<int32_t>& values);

    /// K_max per shared-shredding field, from options.
    std::map<std::string, int32_t> column_to_k_max_;
    /// Sliding window of recent max_row_width per field, for K adaptation.
    std::map<std::string, std::vector<int32_t>> recent_max_row_widths_;
};

}  // namespace paimon
