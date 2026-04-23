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

#include "paimon/common/metrics/histogram.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <random>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/testing/utils/testharness.h"
#include "rapidjson/document.h"

namespace paimon::test {

TEST(HistogramImplTest, TestBasicStats) {
    HistogramImpl h;
    h.Add(1);
    h.Add(2);
    h.Add(3);

    HistogramStats s = h.GetStats();
    EXPECT_EQ(s.count, 3);
    EXPECT_DOUBLE_EQ(s.min, 1);
    EXPECT_DOUBLE_EQ(s.max, 3);
    EXPECT_NEAR(s.average, 2.0, 1e-12);
    EXPECT_NEAR(s.stddev, std::sqrt(2.0 / 3.0), 1e-12);
    EXPECT_DOUBLE_EQ(s.p50, 2);
    EXPECT_DOUBLE_EQ(s.p90, 3);
    EXPECT_DOUBLE_EQ(s.p95, 3);
    EXPECT_DOUBLE_EQ(s.p99, 3);
    EXPECT_DOUBLE_EQ(s.p999, 3);
}

TEST(HistogramImplTest, TestLargeDatasetExactMoments) {
    HistogramImpl h;

    std::mt19937 rng(12345);
    std::uniform_int_distribution<int> dist(-100, 5000);

    const int32_t n = 20000;
    double sum = 0;
    double sum_squares = 0;
    double minv = std::numeric_limits<double>::infinity();
    double maxv = -std::numeric_limits<double>::infinity();

    for (int32_t i = 0; i < n; ++i) {
        const auto v = static_cast<double>(dist(rng));
        h.Add(v);
        sum += v;
        sum_squares += v * v;
        minv = std::min(minv, v);
        maxv = std::max(maxv, v);
    }

    HistogramStats s = h.GetStats();
    EXPECT_EQ(s.count, static_cast<uint64_t>(n));
    EXPECT_DOUBLE_EQ(s.sum, sum);
    EXPECT_DOUBLE_EQ(s.min, minv);
    EXPECT_DOUBLE_EQ(s.max, maxv);

    const double mean = sum / static_cast<double>(n);
    const double ex2 = sum_squares / static_cast<double>(n);
    const double var = std::max(0.0, ex2 - mean * mean);
    const double stddev = std::sqrt(var);
    EXPECT_DOUBLE_EQ(s.average, mean);
    EXPECT_DOUBLE_EQ(s.stddev, stddev);

    // Percentiles should be inside [min, max] and monotonically increasing.
    EXPECT_GE(s.p50, s.min);
    EXPECT_LE(s.p50, s.max);
    EXPECT_GE(s.p90, s.p50);
    EXPECT_GE(s.p95, s.p90);
    EXPECT_GE(s.p99, s.p95);
    EXPECT_GE(s.p999, s.p99);
    EXPECT_LE(s.p999, s.max);
}

TEST(HistogramImplTest, TestMergeMatchesSingleHistogram) {
    std::mt19937 rng(7);
    std::uniform_int_distribution<int> dist(0, 100000);
    const int32_t n = 50000;

    std::vector<double> values;
    values.reserve(n);
    for (int32_t i = 0; i < n; ++i) {
        values.push_back(static_cast<double>(dist(rng)));
    }

    HistogramImpl left;
    HistogramImpl right;
    HistogramImpl all;

    for (int32_t i = 0; i < n; ++i) {
        all.Add(values[i]);
        if (i % 2 == 0) {
            left.Add(values[i]);
        } else {
            right.Add(values[i]);
        }
    }
    left.Merge(right);

    HistogramStats merged = left.GetStats();
    HistogramStats single = all.GetStats();

    EXPECT_EQ(merged.count, single.count);
    EXPECT_DOUBLE_EQ(merged.sum, single.sum);
    EXPECT_DOUBLE_EQ(merged.min, single.min);
    EXPECT_DOUBLE_EQ(merged.max, single.max);
    EXPECT_DOUBLE_EQ(merged.average, single.average);
    EXPECT_DOUBLE_EQ(merged.stddev, single.stddev);
    EXPECT_DOUBLE_EQ(merged.p50, single.p50);
    EXPECT_DOUBLE_EQ(merged.p90, single.p90);
    EXPECT_DOUBLE_EQ(merged.p95, single.p95);
    EXPECT_DOUBLE_EQ(merged.p99, single.p99);
    EXPECT_DOUBLE_EQ(merged.p999, single.p999);
}

TEST(HistogramImplTest, TestCloneConsistencyAndIndependence) {
    HistogramImpl h;
    // Include a mix of values across buckets; NaN should be ignored.
    const std::vector<double> values = {1,   2,   3,  10,
                                        100, 0.5, -5, std::numeric_limits<double>::quiet_NaN()};
    for (double v : values) {
        h.Add(v);
    }

    auto cloned_base = h.Clone();
    auto cloned = std::dynamic_pointer_cast<HistogramImpl>(cloned_base);
    ASSERT_TRUE(cloned != nullptr);

    // Capture original state after Clone() for easier post-mortem inspection.
    // Clone() should not mutate the source.
    const auto after_snapshot = h.GetSnapshot();
    const auto after_stats = h.GetStats();

    const auto cloned_snapshot = cloned->GetSnapshot();
    EXPECT_EQ(cloned_snapshot.count, after_snapshot.count);
    EXPECT_DOUBLE_EQ(cloned_snapshot.sum, after_snapshot.sum);
    EXPECT_DOUBLE_EQ(cloned_snapshot.sum_squares, after_snapshot.sum_squares);
    EXPECT_DOUBLE_EQ(cloned_snapshot.min, after_snapshot.min);
    EXPECT_DOUBLE_EQ(cloned_snapshot.max, after_snapshot.max);
    EXPECT_EQ(cloned_snapshot.bucket_counts, after_snapshot.bucket_counts);

    const auto cloned_stats = cloned->GetStats();
    EXPECT_EQ(cloned_stats.count, after_stats.count);
    EXPECT_DOUBLE_EQ(cloned_stats.sum, after_stats.sum);
    EXPECT_DOUBLE_EQ(cloned_stats.min, after_stats.min);
    EXPECT_DOUBLE_EQ(cloned_stats.max, after_stats.max);
    EXPECT_DOUBLE_EQ(cloned_stats.average, after_stats.average);
    EXPECT_DOUBLE_EQ(cloned_stats.stddev, after_stats.stddev);
    EXPECT_DOUBLE_EQ(cloned_stats.p50, after_stats.p50);
    EXPECT_DOUBLE_EQ(cloned_stats.p90, after_stats.p90);
    EXPECT_DOUBLE_EQ(cloned_stats.p95, after_stats.p95);
    EXPECT_DOUBLE_EQ(cloned_stats.p99, after_stats.p99);
    EXPECT_DOUBLE_EQ(cloned_stats.p999, after_stats.p999);

    // Mutating original should not affect cloned.
    h.Add(42);
    EXPECT_EQ(cloned->GetSnapshot().count, after_snapshot.count);
    EXPECT_EQ(h.GetSnapshot().count, after_snapshot.count + 1);

    // Mutating cloned should not affect original.
    cloned->Add(84);
    EXPECT_EQ(cloned->GetSnapshot().count, after_snapshot.count + 1);
    EXPECT_EQ(h.GetSnapshot().count, after_snapshot.count + 1);
}

TEST(MetricsImplHistogramTest, TestMergeAndOverwrite) {
    auto metrics1 = std::make_shared<MetricsImpl>();
    metrics1->ObserveHistogram("h", 1);
    metrics1->ObserveHistogram("h", 2);

    auto metrics2 = std::make_shared<MetricsImpl>();
    metrics2->ObserveHistogram("h", 3);
    metrics2->ObserveHistogram("h", 4);

    metrics1->Merge(metrics2);

    ASSERT_OK_AND_ASSIGN(HistogramStats s, metrics1->GetHistogramStats("h"));
    EXPECT_EQ(s.count, 4);
    EXPECT_DOUBLE_EQ(s.min, 1);
    EXPECT_DOUBLE_EQ(s.max, 4);
    EXPECT_NEAR(s.average, 2.5, 1e-12);
    EXPECT_DOUBLE_EQ(s.p50, 2);
    EXPECT_DOUBLE_EQ(s.p99, 4);

    auto metrics3 = std::make_shared<MetricsImpl>();
    metrics3->ObserveHistogram("h2", 100);
    metrics1->Overwrite(metrics3);
    ASSERT_NOK_WITH_MSG(metrics1->GetHistogramStats("h"), "Key error: histogram 'h' not found");
    ASSERT_OK_AND_ASSIGN(HistogramStats s2, metrics1->GetHistogramStats("h2"));
    EXPECT_EQ(s2.count, 1);
    EXPECT_DOUBLE_EQ(s2.min, 100);
    EXPECT_DOUBLE_EQ(s2.max, 100);
    EXPECT_DOUBLE_EQ(s2.stddev, 0);
}

TEST(MetricsImplHistogramTest, TestToStringWithHistogram) {
    auto metrics = std::make_shared<MetricsImpl>();
    metrics->SetCounter("k1", 1);
    metrics->ObserveHistogram("h", 1);
    metrics->ObserveHistogram("h", 2);

    rapidjson::Document doc;
    doc.Parse(metrics->ToString().c_str());
    ASSERT_TRUE(doc.IsObject());
    ASSERT_TRUE(doc.HasMember("k1"));
    EXPECT_EQ(doc["k1"].GetUint64(), 1);

    ASSERT_TRUE(doc.HasMember("h.count"));
    EXPECT_EQ(doc["h.count"].GetUint64(), 2);
    ASSERT_TRUE(doc.HasMember("h.min"));
    EXPECT_TRUE(doc["h.min"].IsNumber());
    ASSERT_TRUE(doc.HasMember("h.p99"));
    EXPECT_TRUE(doc["h.p99"].IsNumber());

    ASSERT_TRUE(doc.HasMember("h.p99.9"));
    EXPECT_TRUE(doc["h.p99.9"].IsNumber());
    ASSERT_TRUE(doc.HasMember("h.stddev"));
    EXPECT_TRUE(doc["h.stddev"].IsNumber());
}

}  // namespace paimon::test
