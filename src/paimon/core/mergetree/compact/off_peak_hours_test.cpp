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

#include "paimon/core/mergetree/compact/off_peak_hours.h"

#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(OffPeakHoursTest, TestCreateFromOptions) {
    std::map<std::string, std::string> options = {{Options::COMPACT_OFFPEAK_START_HOUR, "22"},
                                                  {Options::COMPACT_OFFPEAK_END_HOUR, "6"},
                                                  {Options::COMPACTION_OFFPEAK_RATIO, "10"}};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    auto off_peak_hours = OffPeakHours::Create(core_options);
    ASSERT_TRUE(off_peak_hours);
    ASSERT_EQ(off_peak_hours->CurrentRatio(23), 10);
    ASSERT_EQ(off_peak_hours->CurrentRatio(7), 0);
}

TEST(OffPeakHoursTest, TestCreateFromOptionsWithDefault) {
    std::map<std::string, std::string> options = {};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    auto off_peak_hours = OffPeakHours::Create(core_options);
    ASSERT_FALSE(off_peak_hours);
}

TEST(OffPeakHoursTest, TestCreateFromOptionsWithSameHour) {
    std::map<std::string, std::string> options = {{Options::COMPACT_OFFPEAK_START_HOUR, "5"},
                                                  {Options::COMPACT_OFFPEAK_END_HOUR, "5"},
                                                  {Options::COMPACTION_OFFPEAK_RATIO, "10"}};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    auto off_peak_hours = OffPeakHours::Create(core_options);
    ASSERT_FALSE(off_peak_hours);
}

TEST(OffPeakHoursTest, TestCreateWithInvalidHours) {
    ASSERT_FALSE(
        OffPeakHours::Create(/*start_hour=*/-1, /*end_hour=*/-1, /*compact_off_peak_ratio=*/10));
    ASSERT_FALSE(
        OffPeakHours::Create(/*start_hour=*/5, /*end_hour=*/5, /*compact_off_peak_ratio=*/10));
    ASSERT_FALSE(
        OffPeakHours::Create(/*start_hour=*/2, /*end_hour=*/-1, /*compact_off_peak_ratio=*/10));
    ASSERT_FALSE(
        OffPeakHours::Create(/*start_hour=*/-1, /*end_hour=*/2, /*compact_off_peak_ratio=*/10));
}

TEST(OffPeakHoursTest, TestCurrentRatioNormalHours) {
    auto off_peak_hours =
        OffPeakHours::Create(/*start_hour=*/2, /*end_hour=*/8, /*compact_off_peak_ratio=*/10);
    ASSERT_TRUE(off_peak_hours);
    // Before start
    ASSERT_EQ(0, off_peak_hours->CurrentRatio(1));
    // At start
    ASSERT_EQ(10, off_peak_hours->CurrentRatio(2));
    // In between
    ASSERT_EQ(10, off_peak_hours->CurrentRatio(5));
    // Before end
    ASSERT_EQ(10, off_peak_hours->CurrentRatio(7));
    // At end (exclusive)
    ASSERT_EQ(0, off_peak_hours->CurrentRatio(8));
    // After end
    ASSERT_EQ(0, off_peak_hours->CurrentRatio(9));
}

TEST(OffPeakHoursTest, TestCurrentRatioOvernightHours) {
    auto off_peak_hours =
        OffPeakHours::Create(/*start_hour=*/22, /*end_hour=*/6, /*compact_off_peak_ratio=*/10);
    ASSERT_TRUE(off_peak_hours);
    // Before start
    ASSERT_EQ(0, off_peak_hours->CurrentRatio(21));
    // At start
    ASSERT_EQ(10, off_peak_hours->CurrentRatio(22));
    // After start
    ASSERT_EQ(10, off_peak_hours->CurrentRatio(23));
    // After midnight, before end
    ASSERT_EQ(10, off_peak_hours->CurrentRatio(0));
    // Before end
    ASSERT_EQ(10, off_peak_hours->CurrentRatio(5));
    // At end (exclusive)
    ASSERT_EQ(0, off_peak_hours->CurrentRatio(6));
    // After end, before next start"
    ASSERT_EQ(0, off_peak_hours->CurrentRatio(10));
}

}  // namespace paimon::test
