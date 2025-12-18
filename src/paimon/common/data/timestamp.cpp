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

#include <ctime>
#include <iomanip>
#include <sstream>

namespace paimon {
const int32_t Timestamp::DEFAULT_PRECISION = 6;
const int32_t Timestamp::MILLIS_PRECISION = 3;
const int32_t Timestamp::MAX_PRECISION = 9;
const int32_t Timestamp::MIN_PRECISION = 0;
const int32_t Timestamp::MAX_COMPACT_PRECISION = 3;

int64_t Timestamp::ToMicrosecond() const {
    int64_t MICROS_PER_MILLIS = 1000l;
    int64_t NANOS_PER_MICROS = 1000l;
    int64_t micro = millisecond_ * MICROS_PER_MILLIS;
    return micro + nano_of_millisecond_ / NANOS_PER_MICROS;
}

std::string Timestamp::ToString() const {
    time_t seconds = millisecond_ / 1000;
    std::stringstream out;
    int64_t ns = (millisecond_ % 1000) * 1000000l + nano_of_millisecond_;
    if (ns < 0) {
        seconds -= 1;
        ns += 1000000000l;
    }

    std::tm tm_info;
    ::gmtime_r(&seconds, &tm_info);
    out << std::put_time(&tm_info, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0') << std::setw(9)
        << ns;
    // for year with less than 4 digits, year str is not 4 digits in put_time(), e.g., year 1 is "1"
    // rather than "0001"
    static const int32_t MAX_STR_LENGTH = 29;
    std::string ret = out.str();
    if (ret.length() < MAX_STR_LENGTH) {
        ret.insert(0, MAX_STR_LENGTH - ret.length(), '0');
    }
    return ret;
}

}  // namespace paimon
