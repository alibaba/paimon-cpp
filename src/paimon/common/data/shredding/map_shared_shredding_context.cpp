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

#include "paimon/common/data/shredding/map_shared_shredding_context.h"

#include <algorithm>
#include <utility>

namespace paimon {

MapSharedShreddingContext::MapSharedShreddingContext(std::map<int32_t, int32_t> column_to_k_max)
    : column_to_k_max_(std::move(column_to_k_max)) {}

std::map<int32_t, int32_t> MapSharedShreddingContext::ComputeNextK() const {
    std::map<int32_t, int32_t> result;
    for (const auto& [col_index, k_max] : column_to_k_max_) {
        auto it = recent_max_row_widths_.find(col_index);
        if (it == recent_max_row_widths_.end() || it->second.empty()) {
            // First file — no history, use K_max.
            result[col_index] = k_max;
        } else {
            int32_t window_max = ComputeWindowMax(it->second);
            result[col_index] = std::min(window_max, k_max);
        }
    }
    return result;
}

void MapSharedShreddingContext::ReportFileStats(int32_t col_index, int32_t max_row_width) {
    auto& window = recent_max_row_widths_[col_index];
    window.push_back(max_row_width);
    if (static_cast<int32_t>(window.size()) > kWindowSize) {
        window.erase(window.begin());
    }
}

std::vector<int32_t> MapSharedShreddingContext::GetShreddingColumnIndices() const {
    std::vector<int32_t> indices;
    indices.reserve(column_to_k_max_.size());
    for (const auto& [col_index, _] : column_to_k_max_) {
        indices.push_back(col_index);
    }
    return indices;
}

int32_t MapSharedShreddingContext::ComputeWindowMax(const std::vector<int32_t>& values) {
    if (values.empty()) {
        return 0;
    }
    // TODO(xinyu.lxy): support P99
    return *std::max_element(values.begin(), values.end());
}

}  // namespace paimon
