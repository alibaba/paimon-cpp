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

#include "paimon/core/operation/metrics/compaction_metrics.h"

#include <cstdint>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(CompactionMetricsTest, TestReporterAggregationAndCounters) {
    CompactionMetrics metrics;

    auto reporter1 = metrics.CreateReporter(BinaryRow::EmptyRow(), 0);
    auto reporter2 = metrics.CreateReporter(BinaryRow::EmptyRow(), 1);

    reporter1->ReportLevel0FileCount(10);
    reporter2->ReportLevel0FileCount(4);
    reporter1->ReportCompactionInputSize(200);
    reporter2->ReportCompactionInputSize(100);
    reporter1->ReportCompactionOutputSize(150);
    reporter2->ReportCompactionOutputSize(90);
    reporter1->ReportTotalFileSize(500);
    reporter2->ReportTotalFileSize(300);

    reporter1->ReportCompactionTime(50);
    reporter2->ReportCompactionTime(150);

    reporter1->IncreaseCompactionsCompletedCount();
    reporter1->IncreaseCompactionsTotalCount();
    reporter2->IncreaseCompactionsTotalCount();
    reporter1->IncreaseCompactionsQueuedCount();
    reporter2->IncreaseCompactionsQueuedCount();
    reporter2->DecreaseCompactionsQueuedCount();

    auto snapshot = metrics.GetMetrics();

    ASSERT_OK_AND_ASSIGN(auto completed,
                         snapshot->GetCounter(CompactionMetrics::COMPACTION_COMPLETED_COUNT));
    EXPECT_EQ(1, completed);
    ASSERT_OK_AND_ASSIGN(auto total,
                         snapshot->GetCounter(CompactionMetrics::COMPACTION_TOTAL_COUNT));
    EXPECT_EQ(2, total);
    ASSERT_OK_AND_ASSIGN(auto queued,
                         snapshot->GetCounter(CompactionMetrics::COMPACTION_QUEUED_COUNT));
    EXPECT_EQ(1, queued);

    ASSERT_OK_AND_ASSIGN(auto max_l0, snapshot->GetGauge(CompactionMetrics::MAX_LEVEL0_FILE_COUNT));
    EXPECT_DOUBLE_EQ(10.0, max_l0);
    ASSERT_OK_AND_ASSIGN(auto avg_l0, snapshot->GetGauge(CompactionMetrics::AVG_LEVEL0_FILE_COUNT));
    EXPECT_DOUBLE_EQ(7.0, avg_l0);

    ASSERT_OK_AND_ASSIGN(auto max_input,
                         snapshot->GetGauge(CompactionMetrics::MAX_COMPACTION_INPUT_SIZE));
    EXPECT_DOUBLE_EQ(200.0, max_input);
    ASSERT_OK_AND_ASSIGN(auto avg_input,
                         snapshot->GetGauge(CompactionMetrics::AVG_COMPACTION_INPUT_SIZE));
    EXPECT_DOUBLE_EQ(150.0, avg_input);

    ASSERT_OK_AND_ASSIGN(auto max_output,
                         snapshot->GetGauge(CompactionMetrics::MAX_COMPACTION_OUTPUT_SIZE));
    EXPECT_DOUBLE_EQ(150.0, max_output);
    ASSERT_OK_AND_ASSIGN(auto avg_output,
                         snapshot->GetGauge(CompactionMetrics::AVG_COMPACTION_OUTPUT_SIZE));
    EXPECT_DOUBLE_EQ(120.0, avg_output);

    ASSERT_OK_AND_ASSIGN(auto max_total,
                         snapshot->GetGauge(CompactionMetrics::MAX_TOTAL_FILE_SIZE));
    EXPECT_DOUBLE_EQ(500.0, max_total);
    ASSERT_OK_AND_ASSIGN(auto avg_total,
                         snapshot->GetGauge(CompactionMetrics::AVG_TOTAL_FILE_SIZE));
    EXPECT_DOUBLE_EQ(400.0, avg_total);

    ASSERT_OK_AND_ASSIGN(auto avg_time, snapshot->GetGauge(CompactionMetrics::AVG_COMPACTION_TIME));
    EXPECT_DOUBLE_EQ(100.0, avg_time);
}

TEST(CompactionMetricsTest, TestUnregisterAndEmptyDefaults) {
    CompactionMetrics metrics;

    auto reporter = metrics.CreateReporter(BinaryRow::EmptyRow(), 7);
    reporter->ReportLevel0FileCount(9);
    reporter->ReportCompactionInputSize(123);
    reporter->ReportCompactionOutputSize(45);
    reporter->ReportTotalFileSize(999);

    reporter->Unregister();

    auto snapshot = metrics.GetMetrics();

    ASSERT_OK_AND_ASSIGN(auto max_l0, snapshot->GetGauge(CompactionMetrics::MAX_LEVEL0_FILE_COUNT));
    EXPECT_DOUBLE_EQ(-1.0, max_l0);
    ASSERT_OK_AND_ASSIGN(auto avg_l0, snapshot->GetGauge(CompactionMetrics::AVG_LEVEL0_FILE_COUNT));
    EXPECT_DOUBLE_EQ(-1.0, avg_l0);

    ASSERT_OK_AND_ASSIGN(auto max_input,
                         snapshot->GetGauge(CompactionMetrics::MAX_COMPACTION_INPUT_SIZE));
    EXPECT_DOUBLE_EQ(-1.0, max_input);
    ASSERT_OK_AND_ASSIGN(auto avg_input,
                         snapshot->GetGauge(CompactionMetrics::AVG_COMPACTION_INPUT_SIZE));
    EXPECT_DOUBLE_EQ(-1.0, avg_input);

    ASSERT_OK_AND_ASSIGN(auto max_output,
                         snapshot->GetGauge(CompactionMetrics::MAX_COMPACTION_OUTPUT_SIZE));
    EXPECT_DOUBLE_EQ(-1.0, max_output);
    ASSERT_OK_AND_ASSIGN(auto avg_output,
                         snapshot->GetGauge(CompactionMetrics::AVG_COMPACTION_OUTPUT_SIZE));
    EXPECT_DOUBLE_EQ(-1.0, avg_output);

    ASSERT_OK_AND_ASSIGN(auto max_total,
                         snapshot->GetGauge(CompactionMetrics::MAX_TOTAL_FILE_SIZE));
    EXPECT_DOUBLE_EQ(-1.0, max_total);
    ASSERT_OK_AND_ASSIGN(auto avg_total,
                         snapshot->GetGauge(CompactionMetrics::AVG_TOTAL_FILE_SIZE));
    EXPECT_DOUBLE_EQ(-1.0, avg_total);

    ASSERT_OK_AND_ASSIGN(auto avg_time, snapshot->GetGauge(CompactionMetrics::AVG_COMPACTION_TIME));
    EXPECT_DOUBLE_EQ(0.0, avg_time);
}

TEST(CompactionMetricsTest, TestCompactionTimeWindow) {
    CompactionMetrics metrics;

    for (int64_t t = 1; t <= CompactionMetrics::kCompactionTimeWindow + 10; ++t) {
        metrics.ReportCompactionTime(t);
    }

    auto snapshot = metrics.GetMetrics();
    ASSERT_OK_AND_ASSIGN(auto avg_time, snapshot->GetGauge(CompactionMetrics::AVG_COMPACTION_TIME));

    // Only the last kCompactionTimeWindow values are kept.
    EXPECT_DOUBLE_EQ(60.5, avg_time);
}

}  // namespace paimon::test
