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

#include <cstdint>
#include <map>
#include <vector>

#include "gtest/gtest.h"

namespace paimon::test {

class MapSharedShreddingContextTest : public ::testing::Test {};

TEST_F(MapSharedShreddingContextTest, FirstFileUsesKMax) {
    // No history — ComputeNextK should return K_max for every column.
    std::map<std::string, int32_t> field_to_k_max = {{"tags", 8}, {"metrics", 4}};
    MapSharedShreddingContext context(field_to_k_max);

    auto next_k = context.ComputeNextK();
    ASSERT_EQ(2, next_k.size());
    ASSERT_EQ(8, next_k.at("tags"));
    ASSERT_EQ(4, next_k.at("metrics"));
}

TEST_F(MapSharedShreddingContextTest, AdaptKAfterOneFile) {
    // After reporting stats from one file, K should adapt to
    // min(max_row_width, K_max).
    std::map<std::string, int32_t> field_to_k_max = {{"m", 10}};
    MapSharedShreddingContext context(field_to_k_max);

    // First file uses K_max=10.
    auto k1 = context.ComputeNextK();
    ASSERT_EQ(10, k1.at("m"));

    // Report: file had max_row_width=3 for field "m".
    context.ReportFileStats("m", 3);

    // Second file: K = min(3, 10) = 3.
    auto k2 = context.ComputeNextK();
    ASSERT_EQ(3, k2.at("m"));
}

TEST_F(MapSharedShreddingContextTest, AdaptKCappedByKMax) {
    // Even if max_row_width > K_max, K should be capped at K_max.
    std::map<std::string, int32_t> field_to_k_max = {{"m", 5}};
    MapSharedShreddingContext context(field_to_k_max);

    context.ReportFileStats("m", 100);

    auto next_k = context.ComputeNextK();
    ASSERT_EQ(5, next_k.at("m"));
}

TEST_F(MapSharedShreddingContextTest, WindowMaxTracksLargest) {
    // K should use the max of all recent max_row_widths within the window.
    std::map<std::string, int32_t> field_to_k_max = {{"m", 20}};
    MapSharedShreddingContext context(field_to_k_max);

    context.ReportFileStats("m", 3);
    context.ReportFileStats("m", 7);
    context.ReportFileStats("m", 5);

    // max of {3, 7, 5} = 7, capped by K_max=20 → K=7.
    auto next_k = context.ComputeNextK();
    ASSERT_EQ(7, next_k.at("m"));
}

TEST_F(MapSharedShreddingContextTest, MultipleColumnsIndependent) {
    // Each field adapts independently.
    std::map<std::string, int32_t> field_to_k_max = {{"tags", 10}, {"attrs", 6}};
    MapSharedShreddingContext context(field_to_k_max);

    // First file.
    auto k1 = context.ComputeNextK();
    ASSERT_EQ(10, k1.at("tags"));
    ASSERT_EQ(6, k1.at("attrs"));

    // Report: tags had width 4, attrs had width 2.
    context.ReportFileStats("tags", 4);
    context.ReportFileStats("attrs", 2);

    auto k2 = context.ComputeNextK();
    ASSERT_EQ(4, k2.at("tags"));
    ASSERT_EQ(2, k2.at("attrs"));

    // Report: tags had width 8, attrs had width 6.
    context.ReportFileStats("tags", 8);
    context.ReportFileStats("attrs", 6);

    auto k3 = context.ComputeNextK();
    // tags: max(4,8)=8, capped by 10 → 8
    // attrs: max(2,6)=6, capped by 6 → 6
    ASSERT_EQ(8, k3.at("tags"));
    ASSERT_EQ(6, k3.at("attrs"));
}

TEST_F(MapSharedShreddingContextTest, GetShreddingColumnIndices) {
    std::map<std::string, int32_t> field_to_k_max = {{"tags", 8}, {"metrics", 4}, {"props", 16}};
    MapSharedShreddingContext context(field_to_k_max);

    auto indices = context.GetShreddingColumnIndices();
    ASSERT_EQ(indices, std::vector<std::string>({"metrics", "props", "tags"}));
}

TEST_F(MapSharedShreddingContextTest, SlidingWindowEvictsOldEntries) {
    // The window size is 100. After filling 100 entries, adding one more
    // should evict the oldest. Verify that the evicted value no longer
    // affects ComputeNextK.
    std::map<std::string, int32_t> field_to_k_max = {{"m", 256}};
    MapSharedShreddingContext context(field_to_k_max);

    // Insert a large value as the first entry.
    context.ReportFileStats("m", 200);

    // Fill the remaining 99 slots with small values.
    for (int i = 0; i < 99; ++i) {
        context.ReportFileStats("m", 3);
    }

    // Window = [200, 3, 3, ..., 3] (100 entries). Max = 200.
    auto k_before = context.ComputeNextK();
    ASSERT_EQ(200, k_before.at("m"));

    // Push one more — evicts the 200.
    context.ReportFileStats("m", 5);

    // Window = [3, 3, ..., 3, 5] (100 entries). Max = 5.
    auto k_after = context.ComputeNextK();
    ASSERT_EQ(5, k_after.at("m"));
}

TEST_F(MapSharedShreddingContextTest, SingleColumnSingleEntry) {
    std::map<std::string, int32_t> field_to_k_max = {{"m", 4}};
    MapSharedShreddingContext context(field_to_k_max);

    auto indices = context.GetShreddingColumnIndices();
    ASSERT_EQ(indices, std::vector<std::string>({"m"}));
}

TEST_F(MapSharedShreddingContextTest, EmptyContext) {
    std::map<std::string, int32_t> field_to_k_max;
    MapSharedShreddingContext context(field_to_k_max);

    auto next_k = context.ComputeNextK();
    ASSERT_TRUE(next_k.empty());

    auto indices = context.GetShreddingColumnIndices();
    ASSERT_TRUE(indices.empty());
}

}  // namespace paimon::test
