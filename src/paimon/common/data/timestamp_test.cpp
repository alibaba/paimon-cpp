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

#include "paimon/data/timestamp.h"

#include "gtest/gtest.h"

namespace paimon::test {

class TimestampTest : public ::testing::Test {
 protected:
    void SetUp() override {
        timestamp_ = Timestamp(1622547800000, 123456);  // 2021-06-01 11:43:20.000123456
    }

    Timestamp timestamp_;
};

TEST_F(TimestampTest, GetMillisecond) {
    ASSERT_EQ(timestamp_.GetMillisecond(), 1622547800000);
}

TEST_F(TimestampTest, GetNanoOfMillisecond) {
    ASSERT_EQ(timestamp_.GetNanoOfMillisecond(), 123456);
}

TEST_F(TimestampTest, ToNanosecond) {
    ASSERT_EQ(timestamp_.ToNanosecond(), 1622547800000123456ll);
}

TEST_F(TimestampTest, FromEpochMillis) {
    Timestamp ts1 = Timestamp::FromEpochMillis(1622547800000);
    ASSERT_EQ(ts1.GetMillisecond(), 1622547800000);
    ASSERT_EQ(ts1.GetNanoOfMillisecond(), 0);

    Timestamp ts2 = Timestamp::FromEpochMillis(1622547800000, 123456);
    ASSERT_EQ(ts2.GetMillisecond(), 1622547800000);
    ASSERT_EQ(ts2.GetNanoOfMillisecond(), 123456);
}

TEST_F(TimestampTest, ToMillisTimestamp) {
    Timestamp ts = timestamp_.ToMillisTimestamp();
    ASSERT_EQ(ts.GetMillisecond(), 1622547800000);
    ASSERT_EQ(ts.GetNanoOfMillisecond(), 0);
}

TEST_F(TimestampTest, IsCompact) {
    ASSERT_TRUE(Timestamp::IsCompact(3));
    ASSERT_FALSE(Timestamp::IsCompact(6));
}

TEST_F(TimestampTest, EqualityOperator) {
    Timestamp ts1(1622547800000, 123456);
    Timestamp ts2(1622547800000, 123456);
    Timestamp ts3(1622547800000, 654321);
    ASSERT_EQ(ts1, ts1);
    ASSERT_EQ(ts1, ts2);
    ASSERT_FALSE(ts1 == ts3);
}

TEST_F(TimestampTest, LessThanOperator) {
    Timestamp ts1(1622547800000, 123456);
    Timestamp ts2(1622547800000, 654321);
    Timestamp ts3(1622547800001, 123456);
    ASSERT_LT(ts1, ts2);
    ASSERT_LT(ts1, ts3);
    ASSERT_FALSE(ts2 < ts1);
    ASSERT_FALSE(ts3 < ts1);
}

TEST_F(TimestampTest, ToString) {
    ASSERT_EQ(timestamp_.ToString(), "2021-06-01 11:43:20.000123456");
    ASSERT_EQ(Timestamp(-1, 0).ToString(), "1969-12-31 23:59:59.999000000");
    ASSERT_EQ(Timestamp(-62109569749000l, 0).ToString(), "0001-10-29 05:44:11.000000000");
}

TEST_F(TimestampTest, TestToMicrosecond) {
    {
        Timestamp ts(1622547800000, 123456);
        ASSERT_EQ(1622547800000123l, ts.ToMicrosecond());
    }
    {
        Timestamp ts(-16225478, 123456);
        ASSERT_EQ(-16225477877, ts.ToMicrosecond());
    }
}
}  // namespace paimon::test
