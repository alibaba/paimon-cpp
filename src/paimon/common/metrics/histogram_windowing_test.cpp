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

#include "paimon/common/metrics/histogram_windowing.h"

#include <chrono>
#include <thread>

#include "gtest/gtest.h"

namespace paimon::test {

TEST(HistogramWindowingImplTest, TestAdvanceAndAggregateAcrossWindows) {
    // Use a relatively large span to avoid flakiness around boundary (aligned_now - start == span).
    HistogramWindowingImpl h(/*num_windows=*/4, /*micros_per_window=*/25000,
                             /*min_num_per_window=*/1);
    h.Add(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(26));
    h.Add(2);

    HistogramStats s = h.GetStats();
    EXPECT_EQ(s.count, 2);
    EXPECT_DOUBLE_EQ(s.min, 1);
    EXPECT_DOUBLE_EQ(s.max, 2);
    EXPECT_NEAR(s.average, 1.5, 1e-12);
}

TEST(HistogramWindowingImplTest, TestResetWhenBeyondMaxSpan) {
    HistogramWindowingImpl h(/*num_windows=*/2, /*micros_per_window=*/1000,
                             /*min_num_per_window=*/1);
    h.Add(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    h.Add(2);

    // Sleep long enough so that aligned_now - current_window_start >=
    // micros_per_window*num_windows.
    std::this_thread::sleep_for(std::chrono::milliseconds(3));
    h.Add(3);

    HistogramStats s = h.GetStats();
    EXPECT_EQ(s.count, 1);
    EXPECT_DOUBLE_EQ(s.min, 3);
    EXPECT_DOUBLE_EQ(s.max, 3);
    EXPECT_NEAR(s.average, 3.0, 1e-12);
}

TEST(HistogramWindowingImplTest, TestMergeSameParams) {
    // Use a large window to avoid relying on wall clock for this test.
    HistogramWindowingImpl h1(/*num_windows=*/4, /*micros_per_window=*/1000ULL * 1000ULL,
                              /*min_num_per_window=*/1);
    HistogramWindowingImpl h2(/*num_windows=*/4, /*micros_per_window=*/1000ULL * 1000ULL,
                              /*min_num_per_window=*/1);
    h1.Add(1);
    h1.Add(2);
    h2.Add(3);

    h1.Merge(h2);
    HistogramStats s = h1.GetStats();
    EXPECT_EQ(s.count, 3);
    EXPECT_DOUBLE_EQ(s.sum, 6);
    EXPECT_DOUBLE_EQ(s.min, 1);
    EXPECT_DOUBLE_EQ(s.max, 3);
}

TEST(HistogramWindowingImplTest, TestMergeDifferentParamsFallback) {
    HistogramWindowingImpl h1(/*num_windows=*/4, /*micros_per_window=*/1000ULL * 1000ULL,
                              /*min_num_per_window=*/1);
    HistogramWindowingImpl h2(/*num_windows=*/8, /*micros_per_window=*/1000ULL * 1000ULL,
                              /*min_num_per_window=*/1);
    h2.Add(10);
    h2.Add(20);

    h1.Merge(h2);
    HistogramStats s = h1.GetStats();
    EXPECT_EQ(s.count, 2);
    EXPECT_DOUBLE_EQ(s.min, 10);
    EXPECT_DOUBLE_EQ(s.max, 20);
}

TEST(HistogramWindowingImplTest, TestMinNumPerWindow100Case) {
    // Validate min_num_per_window behavior:
    // - window advancement is gated by sample count
    // - if the histogram doesn't advance in time, it may get reset once beyond max span
    // Use a larger window to reduce wall-clock sensitivity and avoid flakiness.
    HistogramWindowingImpl h(/*num_windows=*/3, /*micros_per_window=*/50000,
                             /*min_num_per_window=*/100);

    // Fill current window but keep it below min_num.
    for (int32_t i = 0; i < 99; ++i) {
        h.Add(1);
    }

    // Cross at least one window.
    std::this_thread::sleep_for(std::chrono::milliseconds(75));
    h.Add(2);  // 100th sample; advancement check happens before this add.

    HistogramStats s1 = h.GetStats();
    EXPECT_EQ(s1.count, 100);
    EXPECT_DOUBLE_EQ(s1.min, 1);
    EXPECT_DOUBLE_EQ(s1.max, 2);

    // Cross enough time so that aligned_now - current_window_start >= max_span
    // (max_span = num_windows * micros_per_window = 150ms here).
    std::this_thread::sleep_for(std::chrono::milliseconds(175));
    h.Add(1000);

    HistogramStats s2 = h.GetStats();
    EXPECT_EQ(s2.count, 1);
    EXPECT_DOUBLE_EQ(s2.min, 1000);
    EXPECT_DOUBLE_EQ(s2.max, 1000);
}

TEST(HistogramWindowingImplTest, TestLargeDatasetInSingleWindow) {
    // Use a large window to avoid relying on wall clock.
    HistogramWindowingImpl h(/*num_windows=*/4, /*micros_per_window=*/60ULL * 1000ULL * 1000ULL,
                             /*min_num_per_window=*/1);

    constexpr uint64_t n = 10000;
    for (uint64_t i = 1; i <= n; ++i) {
        h.Add(static_cast<double>(i));
    }

    HistogramStats s = h.GetStats();
    EXPECT_EQ(s.count, n);
    EXPECT_DOUBLE_EQ(s.min, 1);
    EXPECT_DOUBLE_EQ(s.max, static_cast<double>(n));
    EXPECT_DOUBLE_EQ(s.sum, static_cast<double>(n) * static_cast<double>(n + 1) / 2.0);
    EXPECT_NEAR(s.average, (static_cast<double>(n) + 1.0) / 2.0, 1e-12);
}

TEST(HistogramWindowingImplTest, TestCrossMaxSpanAfterAdvance) {
    // Build multiple windows, then cross max_span so that all previous windows are dropped.
    HistogramWindowingImpl h(/*num_windows=*/2, /*micros_per_window=*/2000,
                             /*min_num_per_window=*/1);

    h.Add(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(3));
    h.Add(2);  // should advance to next window.

    // Cross max_span (4ms) relative to the current window start.
    // max_span = 4ms here.
    std::this_thread::sleep_for(std::chrono::milliseconds(7));
    h.Add(3);  // should reset.

    HistogramStats s = h.GetStats();
    EXPECT_EQ(s.count, 1);
    EXPECT_DOUBLE_EQ(s.min, 3);
    EXPECT_DOUBLE_EQ(s.max, 3);
    EXPECT_DOUBLE_EQ(s.sum, 3);
}

TEST(HistogramWindowingImplTest, TestCloneConsistencyAndIndependence) {
    // Use a large window to avoid relying on wall clock and window advancement.
    HistogramWindowingImpl h(/*num_windows=*/4, /*micros_per_window=*/60ULL * 1000ULL * 1000ULL,
                             /*min_num_per_window=*/1);
    h.Add(1);
    h.Add(2);
    h.Add(3);
    h.Add(4);

    const HistogramStats before = h.GetStats();

    auto cloned_base = h.Clone();
    auto cloned = std::dynamic_pointer_cast<HistogramWindowingImpl>(cloned_base);
    ASSERT_TRUE(cloned != nullptr);

    const HistogramStats cloned_before = cloned->GetStats();
    EXPECT_EQ(cloned_before.count, before.count);
    EXPECT_DOUBLE_EQ(cloned_before.sum, before.sum);
    EXPECT_DOUBLE_EQ(cloned_before.min, before.min);
    EXPECT_DOUBLE_EQ(cloned_before.max, before.max);
    EXPECT_DOUBLE_EQ(cloned_before.average, before.average);
    EXPECT_DOUBLE_EQ(cloned_before.stddev, before.stddev);
    EXPECT_DOUBLE_EQ(cloned_before.p50, before.p50);
    EXPECT_DOUBLE_EQ(cloned_before.p90, before.p90);
    EXPECT_DOUBLE_EQ(cloned_before.p95, before.p95);
    EXPECT_DOUBLE_EQ(cloned_before.p99, before.p99);
    EXPECT_DOUBLE_EQ(cloned_before.p999, before.p999);

    // Mutating original should not affect cloned.
    h.Add(100);
    const HistogramStats after_original = h.GetStats();
    const HistogramStats after_clone = cloned->GetStats();
    EXPECT_EQ(after_clone.count, cloned_before.count);
    EXPECT_EQ(after_original.count, cloned_before.count + 1);

    // Mutating cloned should not affect original.
    cloned->Add(200);
    const HistogramStats after_clone2 = cloned->GetStats();
    const HistogramStats after_original2 = h.GetStats();
    EXPECT_EQ(after_original2.count, after_original.count);
    EXPECT_EQ(after_clone2.count, cloned_before.count + 1);
}

}  // namespace paimon::test
