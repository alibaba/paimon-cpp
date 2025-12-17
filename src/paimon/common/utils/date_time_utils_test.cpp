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

#include "paimon/common/utils/date_time_utils.h"

#include <memory>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"

namespace paimon::test {

TEST(DateTimeUtilsTest, TestTimestampConverter) {
    {
        auto ret = DateTimeUtils::TimestampConverter(
            10L, DateTimeUtils::SECOND, DateTimeUtils::MILLISECOND, DateTimeUtils::NANOSECOND);
        ASSERT_EQ(ret, std::make_pair(10000L, 0L));
    }
    {
        auto ret = DateTimeUtils::TimestampConverter(
            10L, DateTimeUtils::SECOND, DateTimeUtils::NANOSECOND, DateTimeUtils::NANOSECOND);
        ASSERT_EQ(ret, std::make_pair(10000000000L, 0L));
    }
    {
        auto ret = DateTimeUtils::TimestampConverter(
            10L, DateTimeUtils::SECOND, DateTimeUtils::NANOSECOND, DateTimeUtils::SECOND);
        ASSERT_EQ(ret, std::make_pair(10000000000L, 0L));
    }

    {
        auto ret = DateTimeUtils::TimestampConverter(
            2567L, DateTimeUtils::MILLISECOND, DateTimeUtils::SECOND, DateTimeUtils::NANOSECOND);
        ASSERT_EQ(ret, std::make_pair(2L, 567000000L));
    }
    {
        auto ret = DateTimeUtils::TimestampConverter(2567L, DateTimeUtils::MILLISECOND,
                                                     DateTimeUtils::SECOND, DateTimeUtils::SECOND);
        ASSERT_EQ(ret, std::make_pair(2L, 0L));
    }
    {
        auto ret = DateTimeUtils::TimestampConverter(
            2567L, DateTimeUtils::MILLISECOND, DateTimeUtils::MICROSECOND, DateTimeUtils::SECOND);
        ASSERT_EQ(ret, std::make_pair(2567000L, 0L));
    }
    {
        auto ret = DateTimeUtils::TimestampConverter(2567L, DateTimeUtils::MILLISECOND,
                                                     DateTimeUtils::MICROSECOND,
                                                     DateTimeUtils::NANOSECOND);
        ASSERT_EQ(ret, std::make_pair(2567000L, 0L));
    }

    {
        auto ret = DateTimeUtils::TimestampConverter(12345678L, DateTimeUtils::NANOSECOND,
                                                     DateTimeUtils::MILLISECOND,
                                                     DateTimeUtils::NANOSECOND);
        ASSERT_EQ(ret, std::make_pair(12L, 345678L));
    }
    {
        auto ret = DateTimeUtils::TimestampConverter(12345678L, DateTimeUtils::NANOSECOND,
                                                     DateTimeUtils::MILLISECOND,
                                                     DateTimeUtils::MICROSECOND);
        ASSERT_EQ(ret, std::make_pair(12L, 345L));
    }
    {
        auto ret =
            DateTimeUtils::TimestampConverter(12345678L, DateTimeUtils::NANOSECOND,
                                              DateTimeUtils::MILLISECOND, DateTimeUtils::SECOND);
        ASSERT_EQ(ret, std::make_pair(12L, 0L));
    }
    {
        auto ret = DateTimeUtils::TimestampConverter(
            -2240521239998998999L, DateTimeUtils::NANOSECOND, DateTimeUtils::MILLISECOND,
            DateTimeUtils::NANOSECOND);
        ASSERT_EQ(ret, std::make_pair(-2240521239999L, 1001L));
    }
    {
        // 9999-12-31
        auto ret = DateTimeUtils::TimestampConverter(253402252995L, DateTimeUtils::SECOND,
                                                     DateTimeUtils::MILLISECOND,
                                                     DateTimeUtils::NANOSECOND);
        ASSERT_EQ(ret, std::make_pair(253402252995000L, 0L));
    }
}

TEST(DateTimeUtilsTest, TestTimestampToInteger) {
    {
        ASSERT_EQ(DateTimeUtils::TimestampToInteger(Timestamp(1758173447000l, 0),
                                                    /*dst_type=*/DateTimeUtils::TimeType::SECOND),
                  1758173447l);
        ASSERT_EQ(
            DateTimeUtils::TimestampToInteger(Timestamp(1758173447001l, 0),
                                              /*dst_type=*/DateTimeUtils::TimeType::MILLISECOND),
            1758173447001l);
        ASSERT_EQ(
            DateTimeUtils::TimestampToInteger(Timestamp(1758173447001l, 1000),
                                              /*dst_type=*/DateTimeUtils::TimeType::MICROSECOND),
            1758173447001001l);
        ASSERT_EQ(
            DateTimeUtils::TimestampToInteger(Timestamp(1758173447001l, 1001),
                                              /*dst_type=*/DateTimeUtils::TimeType::NANOSECOND),
            1758173447001001001l);
    }
    {
        ASSERT_EQ(DateTimeUtils::TimestampToInteger(Timestamp(-2493033748000l, 0),
                                                    /*dst_type=*/DateTimeUtils::TimeType::SECOND),
                  -2493033748l);
        ASSERT_EQ(
            DateTimeUtils::TimestampToInteger(Timestamp(-2493033748001l, 0),
                                              /*dst_type=*/DateTimeUtils::TimeType::MILLISECOND),
            -2493033748001l);
        ASSERT_EQ(
            DateTimeUtils::TimestampToInteger(Timestamp(-2493033748001l, 1000),
                                              /*dst_type=*/DateTimeUtils::TimeType::MICROSECOND),
            -2493033748000999l);
        ASSERT_EQ(
            DateTimeUtils::TimestampToInteger(Timestamp(-2493033748001l, 1001),
                                              /*dst_type=*/DateTimeUtils::TimeType::NANOSECOND),
            -2493033748000998999l);
    }

    {
        // 9999-12-31, cannot convert to nano second, which is overflow for int64
        ASSERT_EQ(DateTimeUtils::TimestampToInteger(Timestamp(253402252995000l, 0),
                                                    /*dst_type=*/DateTimeUtils::TimeType::SECOND),
                  253402252995l);
        ASSERT_EQ(
            DateTimeUtils::TimestampToInteger(Timestamp(253402252995001l, 0),
                                              /*dst_type=*/DateTimeUtils::TimeType::MILLISECOND),
            253402252995001l);
        ASSERT_EQ(
            DateTimeUtils::TimestampToInteger(Timestamp(253402252995001l, 1000),
                                              /*dst_type=*/DateTimeUtils::TimeType::MICROSECOND),
            253402252995001001l);
    }
    {
        // 0000-01-01, cannot convert to nano second, which is overflow for int64
        ASSERT_EQ(DateTimeUtils::TimestampToInteger(Timestamp(-62167219200000l, 0),
                                                    /*dst_type=*/DateTimeUtils::TimeType::SECOND),
                  -62167219200l);
        ASSERT_EQ(
            DateTimeUtils::TimestampToInteger(Timestamp(-62167219200000l, 0),
                                              /*dst_type=*/DateTimeUtils::TimeType::MILLISECOND),
            -62167219200000l);
        ASSERT_EQ(
            DateTimeUtils::TimestampToInteger(Timestamp(-62167219200000l, 1000),
                                              /*dst_type=*/DateTimeUtils::TimeType::MICROSECOND),
            -62167219199999999l);
    }

    {
        // test precision loss
        ASSERT_EQ(
            DateTimeUtils::TimestampToInteger(Timestamp(1758173447001l, 1001),
                                              /*dst_type=*/DateTimeUtils::TimeType::MICROSECOND),
            1758173447001001l);
        ASSERT_EQ(
            DateTimeUtils::TimestampToInteger(Timestamp(1758173447001l, 1001),
                                              /*dst_type=*/DateTimeUtils::TimeType::MILLISECOND),
            1758173447001l);
        ASSERT_EQ(DateTimeUtils::TimestampToInteger(Timestamp(1758173447001l, 1001),
                                                    /*dst_type=*/DateTimeUtils::TimeType::SECOND),
                  1758173447l);
    }
}

TEST(DateTimeUtilsTest, TestGetPrecisionFromType) {
    auto ts_sec_type = arrow::timestamp(arrow::TimeUnit::type::SECOND);
    auto ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(ts_sec_type);
    ASSERT_EQ(DateTimeUtils::GetPrecisionFromType(ts_type), 0);

    auto ts_milli_type = arrow::timestamp(arrow::TimeUnit::type::MILLI);
    ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(ts_milli_type);
    ASSERT_EQ(DateTimeUtils::GetPrecisionFromType(ts_type), 3);

    auto ts_micro_type = arrow::timestamp(arrow::TimeUnit::type::MICRO);
    ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(ts_micro_type);
    ASSERT_EQ(DateTimeUtils::GetPrecisionFromType(ts_type), 6);

    auto ts_nano_type = arrow::timestamp(arrow::TimeUnit::type::NANO);
    ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(ts_nano_type);
    ASSERT_EQ(DateTimeUtils::GetPrecisionFromType(ts_type), 9);
}

TEST(DateTimeUtilsTest, TestGetTimeTypeFromArrowType) {
    auto ts_sec_type = arrow::timestamp(arrow::TimeUnit::type::SECOND);
    auto ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(ts_sec_type);
    ASSERT_EQ(DateTimeUtils::GetTimeTypeFromArrowType(ts_type), DateTimeUtils::TimeType::SECOND);

    auto ts_milli_type = arrow::timestamp(arrow::TimeUnit::type::MILLI);
    ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(ts_milli_type);
    ASSERT_EQ(DateTimeUtils::GetTimeTypeFromArrowType(ts_type),
              DateTimeUtils::TimeType::MILLISECOND);

    auto ts_micro_type = arrow::timestamp(arrow::TimeUnit::type::MICRO);
    ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(ts_micro_type);
    ASSERT_EQ(DateTimeUtils::GetTimeTypeFromArrowType(ts_type),
              DateTimeUtils::TimeType::MICROSECOND);

    auto ts_nano_type = arrow::timestamp(arrow::TimeUnit::type::NANO);
    ts_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(ts_nano_type);
    ASSERT_EQ(DateTimeUtils::GetTimeTypeFromArrowType(ts_type),
              DateTimeUtils::TimeType::NANOSECOND);
}

TEST(DateTimeUtilsTest, TestGetTypeFromPrecision) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> ts_type,
                             DateTimeUtils::GetTypeFromPrecision(0, /*with_timezone=*/false));
        ASSERT_TRUE(ts_type->Equals(arrow::timestamp(arrow::TimeUnit::type::SECOND)));
    }
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> ts_type,
                             DateTimeUtils::GetTypeFromPrecision(0, /*with_timezone=*/true));
        ASSERT_TRUE(ts_type->Equals(arrow::timestamp(arrow::TimeUnit::type::SECOND, timezone)));
    }
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> ts_type,
                             DateTimeUtils::GetTypeFromPrecision(3, /*with_timezone=*/false));
        ASSERT_TRUE(ts_type->Equals(arrow::timestamp(arrow::TimeUnit::type::MILLI)));
    }
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> ts_type,
                             DateTimeUtils::GetTypeFromPrecision(3, /*with_timezone=*/true));
        ASSERT_TRUE(ts_type->Equals(arrow::timestamp(arrow::TimeUnit::type::MILLI, timezone)));
    }
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> ts_type,
                             DateTimeUtils::GetTypeFromPrecision(6, /*with_timezone=*/false));
        ASSERT_TRUE(ts_type->Equals(arrow::timestamp(arrow::TimeUnit::type::MICRO)));
    }
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> ts_type,
                             DateTimeUtils::GetTypeFromPrecision(6, /*with_timezone=*/true));
        ASSERT_TRUE(ts_type->Equals(arrow::timestamp(arrow::TimeUnit::type::MICRO, timezone)));
    }
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> ts_type,
                             DateTimeUtils::GetTypeFromPrecision(9, /*with_timezone=*/false));
        ASSERT_TRUE(ts_type->Equals(arrow::timestamp(arrow::TimeUnit::type::NANO)));
    }
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> ts_type,
                             DateTimeUtils::GetTypeFromPrecision(9, /*with_timezone=*/true));
        ASSERT_TRUE(ts_type->Equals(arrow::timestamp(arrow::TimeUnit::type::NANO, timezone)));
    }
    {
        ASSERT_NOK_WITH_MSG(DateTimeUtils::GetTypeFromPrecision(4, /*with_timezone=*/true),
                            "only support precision 0/3/6/9 in timestamp type");
    }
}

TEST(DateTimeUtilsTest, TestGetLocalTimezoneName) {
    std::string timezone = DateTimeUtils::GetLocalTimezoneName();
    {
        TimezoneGuard guard("US/Hawaii");
        ASSERT_EQ(DateTimeUtils::GetLocalTimezoneName(), "US/Hawaii");
    }
    ASSERT_EQ(DateTimeUtils::GetLocalTimezoneName(), timezone);
}

TEST(DateTimeUtilsTest, TestGetCurrentTime) {
    TimezoneGuard guard("Asia/Shanghai");
    uint64_t utc_ts = DateTimeUtils::GetCurrentUTCTimeUs();
    uint64_t local_ts = DateTimeUtils::GetCurrentLocalTimeUs().value();
    ASSERT_GT(local_ts, utc_ts);
    ASSERT_GE(local_ts - utc_ts, 28800000000l);
}

TEST(DateTimeUtilsTest, TestToUTCTimestamp) {
    TimezoneGuard guard("Asia/Shanghai");
    {
        Timestamp ts(0, 0);
        ASSERT_OK_AND_ASSIGN(Timestamp utc_ts, DateTimeUtils::ToUTCTimestamp(ts));
        ASSERT_EQ(utc_ts, Timestamp(-28800000l, 0));
    }
    {
        // test precision loss for nano
        Timestamp ts(0, 500);
        ASSERT_OK_AND_ASSIGN(Timestamp utc_ts, DateTimeUtils::ToUTCTimestamp(ts));
        ASSERT_EQ(utc_ts, Timestamp(-28800000l, 0));
    }
}
}  // namespace paimon::test
