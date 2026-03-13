/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/common/metrics/metrics_impl.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(MetricsImplTest, TestSimple) {
    auto metrics = std::make_shared<MetricsImpl>();
    metrics->SetCounter("some_metric", 100);
    metrics->SetCounter("some_metric_2", 150);
    ASSERT_OK_AND_ASSIGN(uint64_t counter, metrics->GetCounter("some_metric"));
    ASSERT_EQ(100, counter);
    ASSERT_OK_AND_ASSIGN(counter, metrics->GetCounter("some_metric_2"));
    ASSERT_EQ(150, counter);
    auto other = std::make_shared<MetricsImpl>();
    other->SetCounter("some_metric_2", 200);
    other->SetCounter("some_metric_3", 300);
    metrics->Merge(other);
    ASSERT_OK_AND_ASSIGN(counter, metrics->GetCounter("some_metric"));
    ASSERT_EQ(100, counter);
    ASSERT_OK_AND_ASSIGN(counter, metrics->GetCounter("some_metric_2"));
    ASSERT_EQ(350, counter);
    ASSERT_OK_AND_ASSIGN(counter, metrics->GetCounter("some_metric_3"));
    ASSERT_EQ(300, counter);
    metrics->Overwrite(other);
    ASSERT_OK_AND_ASSIGN(counter, metrics->GetCounter("some_metric_2"));
    ASSERT_EQ(200, counter);
    ASSERT_OK_AND_ASSIGN(counter, metrics->GetCounter("some_metric_3"));
    ASSERT_EQ(300, counter);
    // check some_metric is not exist after overwrite
    ASSERT_NOK_WITH_MSG(metrics->GetCounter("some_metric"),
                        "Key error: metric 'some_metric' not found");
}

TEST(MetricsImplTest, TestGaugeMergeAndOverwrite) {
    auto metrics = std::make_shared<MetricsImpl>();
    metrics->SetGauge("g1", 1.5);
    metrics->SetGauge("g2", 2.0);

    auto other = std::make_shared<MetricsImpl>();
    other->SetGauge("g2", 3.25);
    other->SetGauge("g3", 4.75);

    metrics->Merge(other);

    ASSERT_OK_AND_ASSIGN(double gauge, metrics->GetGauge("g1"));
    EXPECT_DOUBLE_EQ(1.5, gauge);
    ASSERT_OK_AND_ASSIGN(gauge, metrics->GetGauge("g2"));
    EXPECT_DOUBLE_EQ(5.25, gauge);
    ASSERT_OK_AND_ASSIGN(gauge, metrics->GetGauge("g3"));
    EXPECT_DOUBLE_EQ(4.75, gauge);

    metrics->Overwrite(other);

    ASSERT_OK_AND_ASSIGN(gauge, metrics->GetGauge("g2"));
    EXPECT_DOUBLE_EQ(3.25, gauge);
    ASSERT_OK_AND_ASSIGN(gauge, metrics->GetGauge("g3"));
    EXPECT_DOUBLE_EQ(4.75, gauge);
    ASSERT_NOK_WITH_MSG(metrics->GetGauge("g1"), "Key error: metric 'g1' not found");
}

TEST(MetricsImplTest, TestToString) {
    std::shared_ptr<MetricsImpl> metrics1 = std::make_shared<MetricsImpl>();
    metrics1->SetCounter("k1", 1);
    metrics1->SetCounter("k2", 2);
    metrics1->SetGauge("g1", 1.25);
    std::shared_ptr<MetricsImpl> metrics2 = std::make_shared<MetricsImpl>();
    metrics2->SetCounter("m1", 3);
    metrics2->SetCounter("m2", 4);
    metrics2->SetCounter("k2", 5);
    metrics2->SetGauge("g2", 2.5);
    metrics1->Merge(metrics2);
    EXPECT_EQ(metrics1->ToString(), "{\"k1\":1,\"k2\":7,\"m1\":3,\"m2\":4,\"g1\":1.25,\"g2\":2.5}");
}

}  // namespace paimon::test
