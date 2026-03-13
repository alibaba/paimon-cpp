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

#include <cassert>
#include <chrono>
#include <cstdint>
#include <string>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
/// A data structure representing data of Timestamp without timezone.
///
/// This data structure is immutable and consists of a milliseconds and nanos-of-millisecond since
/// `1970-01-01 00:00:00`. It might be stored in a compact representation (as a long value) if
/// values are small enough. Timestamp range from 0000-01-01 00:00:00.000000000 to 9999-12-31
/// 23:59:59.999999999.
///
class PAIMON_EXPORT Timestamp {
 public:
    Timestamp() : Timestamp(0, 0) {}
    Timestamp(int64_t millisecond, int32_t nano_of_millisecond)
        : millisecond_(millisecond), nano_of_millisecond_(nano_of_millisecond) {
        assert(nano_of_millisecond >= 0 && nano_of_millisecond <= 999999ll);
    }

    /// Get the number of milliseconds since `1970-01-01 00:00:00`.
    int64_t GetMillisecond() const {
        return millisecond_;
    }
    /// Get the number of nanoseconds (the nanoseconds within the milliseconds).
    ///
    /// The value range is from 0 to 999,999.
    int32_t GetNanoOfMillisecond() const {
        return nano_of_millisecond_;
    }

    /// Creates an instance of `Timestamp` from milliseconds.
    ///
    /// The nanos-of-millisecond field will be set to zero.
    ///
    /// @param milliseconds The number of milliseconds since `1970-01-01 00:00:00`; a
    /// negative number is the number of milliseconds before `1970-01-01 00:00:00`.
    static Timestamp FromEpochMillis(int64_t milliseconds) {
        return {milliseconds, 0};
    }

    /// Creates an instance of `Timestamp` from milliseconds and a nanos-of-millisecond.
    ///
    /// @param milliseconds The number of milliseconds since `1970-01-01 00:00:00`; a
    /// negative number is the number of milliseconds before `1970-01-01 00:00:00`.
    /// @param nanos_of_millisecond The nanoseconds within the millisecond, from 0 to 999,999.
    static Timestamp FromEpochMillis(int64_t milliseconds, int32_t nanos_of_millisecond) {
        return {milliseconds, nanos_of_millisecond};
    }

    /// Converts this `Timestamp` object to millis `Timestamp` object (ignore
    /// `nanos_of_millisecond`).
    Timestamp ToMillisTimestamp() const {
        return FromEpochMillis(millisecond_);
    }

    /// Converts this `Timestamp` object to microsecond.
    int64_t ToMicrosecond() const;

    /// Converts this `Timestamp` object to nanoseconds.
    int64_t ToNanosecond() const {
        std::chrono::nanoseconds nanosecond = std::chrono::nanoseconds(nano_of_millisecond_) +
                                              std::chrono::milliseconds(millisecond_);
        return nanosecond.count();
    }

    /// @return Whether the timestamp data is small enough to be stored in a long of
    /// milliseconds.
    static bool IsCompact(int32_t precision) {
        return precision <= MAX_COMPACT_PRECISION;
    }

    bool operator==(const Timestamp& other) const {
        if (this == &other) {
            return true;
        }
        return millisecond_ == other.millisecond_ &&
               nano_of_millisecond_ == other.nano_of_millisecond_;
    }

    bool operator!=(const Timestamp& other) const {
        return !(*this == other);
    }

    bool operator<(const Timestamp& other) const {
        if (millisecond_ == other.millisecond_) {
            return nano_of_millisecond_ < other.nano_of_millisecond_;
        }
        return millisecond_ < other.millisecond_;
    }

    /// Converts the Timestamp object to a string representation in UTC (GMT).
    ///
    /// The format of the returned string is "YYYY-MM-DD HH:MM:SS.nnnnnnnnn",
    /// where the date and time are in UTC (GMT), and the nanoseconds are derived
    /// from the millisecond and nanosecond parts of the timestamp.
    ///
    /// @note This method uses UTC (GMT) time zone when formatting the time.
    /// This is different from the Java Paimon implementation, which may convert
    /// the timestamp to the local time zone of the machine running the Java process.
    std::string ToString() const;

    static const int32_t DEFAULT_PRECISION;
    static const int32_t MILLIS_PRECISION;
    static const int32_t MIN_PRECISION;
    static const int32_t MAX_PRECISION;
    static const int32_t MAX_COMPACT_PRECISION;

 private:
    int64_t millisecond_ = 0;
    int32_t nano_of_millisecond_ = 0;
};

}  // namespace paimon
