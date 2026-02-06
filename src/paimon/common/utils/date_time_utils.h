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

#pragma once

#include <sys/time.h>

#include <cassert>
#include <cstdint>
#include <ctime>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/vendored/datetime.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"
namespace paimon {
/// Utils for date time.
class DateTimeUtils {
 public:
    DateTimeUtils() = delete;
    ~DateTimeUtils() = delete;

    /// The number of milliseconds in a day.
    ///
    /// This is the modulo 'mask' used when converting TIMESTAMP values to DATE and TIME values.
    static constexpr int64_t MILLIS_PER_DAY = 86400000l;  // = 24 * 60 * 60 * 1000
    static constexpr int64_t SECONDS_PER_DAY = 86400l;    // = 24 * 60 * 60
    static constexpr int64_t NANOS_PER_MILLIS = 1000000l;
    enum TimeType {
        SECOND = 0,
        MILLISECOND = 1,
        MICROSECOND = 2,
        NANOSECOND = 3,
    };
    constexpr static int64_t CONVERSION_FACTORS[] = {1L, 1000L, 1000000L, 1000000000L};

    // convert a timestamp of a certain type into a combination of two specified types
    // e.g., src_timestamp = 12345678, src_type = ns, dst_first_type = ms, dst_second_type = ns
    // return: {12, 345678}
    static std::pair<int64_t, int64_t> TimestampConverter(int64_t src_timestamp,
                                                          const TimeType& src_type,
                                                          const TimeType& dst_first_type,
                                                          const TimeType& dst_second_type) {
        if (src_type <= dst_first_type) {
            // e.g., ms -> {us, ns} or {ms, ns} or {us, us} or {ns, ms}
            int64_t conversion_factor_to_first_type =
                CONVERSION_FACTORS[dst_first_type] / CONVERSION_FACTORS[src_type];
            // TODO(jinli.zjw): maybe overflow int64
            assert(src_timestamp * conversion_factor_to_first_type <
                   std::numeric_limits<int64_t>::max());
            return std::make_pair(src_timestamp * conversion_factor_to_first_type, 0L);
        } else {
            // e.g., ns -> {ms, ns} or {ms, s} or {ms, us}
            int64_t conversion_factor_to_first_type =
                CONVERSION_FACTORS[src_type] / CONVERSION_FACTORS[dst_first_type];
            double conversion_factor_to_second_type =
                static_cast<double>(CONVERSION_FACTORS[dst_second_type]) /
                CONVERSION_FACTORS[src_type];

            int64_t first_value = src_timestamp / conversion_factor_to_first_type;
            int64_t second_value = src_timestamp % conversion_factor_to_first_type;
            if (second_value < 0) {
                second_value += conversion_factor_to_first_type;
                first_value--;
            }
            second_value = conversion_factor_to_second_type * second_value;
            return std::make_pair(first_value, second_value);
        }
    }

    static int64_t TimestampToInteger(const Timestamp& timestamp, const TimeType& dst_type) {
        if (dst_type == TimeType::SECOND) {
            return timestamp.GetMillisecond() / CONVERSION_FACTORS[MILLISECOND];
        } else if (dst_type == TimeType::MILLISECOND) {
            return timestamp.GetMillisecond();
        } else if (dst_type == TimeType::MICROSECOND) {
            return timestamp.ToMicrosecond();
        }
        return timestamp.ToNanosecond();
    }

    static inline uint64_t GetCurrentUTCTimeUs() {
        struct timeval ts;
        gettimeofday(&ts, nullptr);
        return static_cast<uint64_t>(ts.tv_sec) * 1000000ULL + static_cast<uint64_t>(ts.tv_usec);
    }

    static inline Result<uint64_t> GetCurrentLocalTimeUs() {
        uint64_t utc_micro = GetCurrentUTCTimeUs();
        auto utc_ts_scalar = std::make_shared<arrow::TimestampScalar>(
            static_cast<int64_t>(utc_micro), arrow::TimeUnit::MICRO, GetLocalTimezoneName());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            arrow::Datum local_micro, arrow::compute::LocalTimestamp(arrow::Datum(utc_ts_scalar)));
        auto local_ts_scalar =
            std::dynamic_pointer_cast<arrow::TimestampScalar>(local_micro.scalar());
        return *(static_cast<const int64_t*>(local_ts_scalar->data()));
    }

    static inline int32_t GetPrecisionFromType(
        const std::shared_ptr<arrow::TimestampType>& timestamp_type) {
        int32_t precision = Timestamp::MAX_PRECISION;
        if (timestamp_type->unit() == arrow::TimeUnit::type::SECOND) {
            precision = Timestamp::MIN_PRECISION;
        } else if (timestamp_type->unit() == arrow::TimeUnit::type::MILLI) {
            precision = Timestamp::MILLIS_PRECISION;
        } else if (timestamp_type->unit() == arrow::TimeUnit::type::MICRO) {
            precision = Timestamp::DEFAULT_PRECISION;
        }
        return precision;
    }

    static inline TimeType GetTimeTypeFromArrowType(
        const std::shared_ptr<arrow::TimestampType>& timestamp_type) {
        if (timestamp_type->unit() == arrow::TimeUnit::type::SECOND) {
            return TimeType::SECOND;
        } else if (timestamp_type->unit() == arrow::TimeUnit::type::MILLI) {
            return TimeType::MILLISECOND;
        } else if (timestamp_type->unit() == arrow::TimeUnit::type::MICRO) {
            return TimeType::MICROSECOND;
        }
        return TimeType::NANOSECOND;
    }

    static inline Result<std::shared_ptr<arrow::DataType>> GetTypeFromPrecision(
        int32_t precision, bool with_timezone) {
        std::string timezone = with_timezone ? GetLocalTimezoneName() : "";
        if (precision == Timestamp::MIN_PRECISION) {
            return arrow::timestamp(arrow::TimeUnit::type::SECOND, timezone);
        } else if (precision == Timestamp::MILLIS_PRECISION) {
            return arrow::timestamp(arrow::TimeUnit::type::MILLI, timezone);
        } else if (precision == Timestamp::DEFAULT_PRECISION) {
            return arrow::timestamp(arrow::TimeUnit::type::MICRO, timezone);
        } else if (precision == Timestamp::MAX_PRECISION) {
            return arrow::timestamp(arrow::TimeUnit::type::NANO, timezone);
        }
        return Status::Invalid("only support precision 0/3/6/9 in timestamp type");
    }

    static std::string GetLocalTimezoneName() {
        // find local tz in env
        const char* timezone = std::getenv("TZ");
        if (timezone != nullptr && *timezone != '\0') {
            return std::string(timezone);
        }
        // find local tz in file
        auto* tz = arrow_vendored::date::current_zone();
        return tz ? tz->name() : "UTC";
    }

    static std::string GetArrowTimeUnitStr(arrow::TimeUnit::type unit) {
        switch (unit) {
            case arrow::TimeUnit::SECOND:
                return "SECOND";
            case arrow::TimeUnit::MILLI:
                return "MILLISECOND";
            case arrow::TimeUnit::MICRO:
                return "MICROSECOND";
            case arrow::TimeUnit::NANO:
                return "NANOSECOND";
            default:
                break;
        }
        return "UNKNOWN";
    }

    // there may be a precision loss for nano
    static Result<Timestamp> ToUTCTimestamp(const Timestamp& timestamp) {
        int64_t micro_second = timestamp.ToMicrosecond();
        auto local_ts_scalar =
            std::make_shared<arrow::TimestampScalar>(micro_second, arrow::TimeUnit::MICRO);
        arrow::compute::AssumeTimezoneOptions options(DateTimeUtils::GetLocalTimezoneName());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            arrow::Datum target_scalar,
            arrow::compute::AssumeTimezone(arrow::Datum(local_ts_scalar), options));
        auto utc_ts_scalar =
            std::dynamic_pointer_cast<arrow::TimestampScalar>(target_scalar.scalar());
        auto [milli, nano] = DateTimeUtils::TimestampConverter(
            *(static_cast<const int64_t*>(utc_ts_scalar->data())),
            DateTimeUtils::TimeType::MICROSECOND, DateTimeUtils::TimeType::MILLISECOND,
            DateTimeUtils::TimeType::NANOSECOND);
        return Timestamp(milli, nano);
    }
};
}  // namespace paimon
