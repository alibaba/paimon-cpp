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

#include "paimon/common/data/variant/variant_json_utils.h"

#include <charconv>
#include <cmath>
#include <cstdio>
#include <cstdlib>

#include "arrow/vendored/datetime.h"
#include "fmt/format.h"

namespace paimon {

namespace {

constexpr int64_t kMicrosPerSecond = 1000000LL;
constexpr int64_t kMicrosPerDay = 86400LL * kMicrosPerSecond;

// Formats the shortest round-trip decimal digits + decimal exponent into the textual form of
// `java.lang.Double.toString` / `java.lang.Float.toString`: plain notation when
// `1e-3 <= |value| < 1e7`, computerized scientific notation otherwise.
std::string JavaFloatingToString(std::string_view digits, int32_t exp, bool negative) {
    std::string result;
    if (negative) {
        result.push_back('-');
    }
    if (exp >= -3 && exp < 7) {
        if (exp >= 0) {
            size_t int_digits = static_cast<size_t>(exp) + 1;
            if (digits.size() > int_digits) {
                result.append(digits, 0, int_digits);
                result.push_back('.');
                result.append(digits.substr(int_digits));
            } else {
                result.append(digits);
                result.append(int_digits - digits.size(), '0');
                result.append(".0");
            }
        } else {
            result.append("0.");
            result.append(static_cast<size_t>(-exp) - 1, '0');
            result.append(digits);
        }
    } else {
        result.push_back(digits[0]);
        result.push_back('.');
        if (digits.size() > 1) {
            result.append(digits.substr(1));
        } else {
            result.push_back('0');
        }
        result.push_back('E');
        result.append(std::to_string(exp));
    }
    return result;
}

template <typename T>
std::string FloatingToJavaString(T value) {
    if (std::isnan(value)) {
        return "NaN";
    }
    if (std::isinf(value)) {
        return value > 0 ? "Infinity" : "-Infinity";
    }
    if (value == 0) {
        return std::signbit(value) ? "-0.0" : "0.0";
    }
    // The shortest round-trip representation in scientific form, e.g. `1.234e+05`. gcc 8's
    // <charconv> lacks floating-point to_chars, so probe increasing precision until the value
    // round-trips; dropping a trailing zero digit is an exact rounding, so the first precision
    // that round-trips carries no trailing zeros.
    constexpr int kMaxFractionDigits = std::is_same_v<T, float> ? 8 : 16;
    char buf[64];
    int len = 0;
    for (int precision = 0; precision <= kMaxFractionDigits; ++precision) {
        len = std::snprintf(buf, sizeof(buf), "%.*e", precision, static_cast<double>(value));
        T parsed;
        if constexpr (std::is_same_v<T, float>) {
            parsed = std::strtof(buf, nullptr);
        } else {
            parsed = std::strtod(buf, nullptr);
        }
        if (parsed == value) {
            break;
        }
    }
    std::string_view repr(buf, static_cast<size_t>(len));
    bool negative = repr[0] == '-';
    if (negative) {
        repr.remove_prefix(1);
    }
    size_t e_pos = repr.find('e');
    std::string_view mantissa = repr.substr(0, e_pos);
    size_t exp_start = e_pos + 1;
    // `std::from_chars` does not accept a leading '+'.
    if (repr[exp_start] == '+') {
        ++exp_start;
    }
    int32_t exp = 0;
    std::from_chars(repr.data() + exp_start, repr.data() + repr.size(), exp);
    std::string digits;
    digits.push_back(mantissa[0]);
    if (mantissa.size() > 2) {
        digits.append(mantissa.substr(2));
    }
    return JavaFloatingToString(digits, exp, negative);
}

// Appends a year like `java.time.LocalDate.toString`: absolute value padded to at least 4
// digits; years above 9999 get a `+` prefix.
void AppendJavaYear(int64_t year, std::string* out) {
    if (year < 0) {
        out->push_back('-');
        year = -year;
        out->append(fmt::format("{:04}", year));
    } else {
        if (year > 9999) {
            out->push_back('+');
        }
        out->append(fmt::format("{:04}", year));
    }
}

void AppendDate(int64_t days_since_epoch, std::string* out) {
    using arrow_vendored::date::days;
    using arrow_vendored::date::sys_days;
    using arrow_vendored::date::year_month_day;
    year_month_day ymd{sys_days{days{days_since_epoch}}};
    AppendJavaYear(static_cast<int32_t>(ymd.year()), out);
    out->append(fmt::format("-{:02}-{:02}", static_cast<uint32_t>(ymd.month()),
                            static_cast<uint32_t>(ymd.day())));
}

int64_t FloorDiv(int64_t x, int64_t y) {
    int64_t quotient = x / y;
    if ((x % y != 0) && ((x < 0) != (y < 0))) {
        --quotient;
    }
    return quotient;
}

}  // namespace

void VariantJsonUtils::AppendEscapedJson(std::string_view str, std::string* out) {
    out->push_back('"');
    for (char c : str) {
        switch (c) {
            case '"':
                out->append("\\\"");
                break;
            case '\\':
                out->append("\\\\");
                break;
            case '\b':
                out->append("\\b");
                break;
            case '\f':
                out->append("\\f");
                break;
            case '\n':
                out->append("\\n");
                break;
            case '\r':
                out->append("\\r");
                break;
            case '\t':
                out->append("\\t");
                break;
            default:
                if (static_cast<uint8_t>(c) < 0x20) {
                    out->append(fmt::format("\\u{:04x}", static_cast<uint8_t>(c)));
                } else {
                    out->push_back(c);
                }
        }
    }
    out->push_back('"');
}

std::string VariantJsonUtils::JavaDoubleToString(double value) {
    return FloatingToJavaString(value);
}

std::string VariantJsonUtils::JavaFloatToString(float value) {
    return FloatingToJavaString(value);
}

std::string VariantJsonUtils::DateToString(int32_t days_since_epoch) {
    std::string result;
    AppendDate(days_since_epoch, &result);
    return result;
}

std::string VariantJsonUtils::TimestampToString(int64_t micros_since_epoch, int32_t offset_seconds,
                                                bool with_offset) {
    int64_t local_micros = micros_since_epoch + static_cast<int64_t>(offset_seconds) * 1000000;
    int64_t days = FloorDiv(local_micros, kMicrosPerDay);
    int64_t micros_of_day = local_micros - days * kMicrosPerDay;
    std::string result;
    AppendDate(days, &result);
    int64_t seconds_of_day = micros_of_day / kMicrosPerSecond;
    int64_t micros_of_second = micros_of_day % kMicrosPerSecond;
    result.append(fmt::format(" {:02}:{:02}:{:02}", seconds_of_day / 3600,
                              (seconds_of_day / 60) % 60, seconds_of_day % 60));
    if (micros_of_second != 0) {
        std::string fraction = fmt::format("{:06}", micros_of_second);
        while (fraction.back() == '0') {
            fraction.pop_back();
        }
        result.push_back('.');
        result.append(fraction);
    }
    if (with_offset) {
        int32_t abs_offset = offset_seconds >= 0 ? offset_seconds : -offset_seconds;
        result.append(fmt::format("{}{:02}:{:02}", offset_seconds >= 0 ? '+' : '-',
                                  abs_offset / 3600, (abs_offset / 60) % 60));
    }
    return result;
}

Result<int32_t> VariantJsonUtils::GetZoneOffsetSeconds(const std::string& zone_id,
                                                       int64_t micros_since_epoch) {
    std::string_view id = zone_id;
    if (id == "Z" || id == "UTC" || id == "GMT" || id == "UT") {
        return 0;
    }
    // `UTC+08:00` style ids: strip the prefix and parse the remaining fixed offset.
    if (id.size() > 3 && (id.substr(0, 3) == "UTC" || id.substr(0, 3) == "GMT")) {
        id.remove_prefix(3);
    } else if (id.size() > 2 && id.substr(0, 2) == "UT" && (id[2] == '+' || id[2] == '-')) {
        id.remove_prefix(2);
    }
    if (!id.empty() && (id[0] == '+' || id[0] == '-')) {
        bool negative = id[0] == '-';
        id.remove_prefix(1);
        // Accepted forms: H, HH, HH:MM, HHMM, HH:MM:SS, HHMMSS.
        std::string digits;
        for (char c : id) {
            if (c >= '0' && c <= '9') {
                digits.push_back(c);
            } else if (c != ':') {
                return Status::Invalid(fmt::format("Invalid zone offset: {}", zone_id));
            }
        }
        int32_t hours = 0;
        int32_t minutes = 0;
        int32_t seconds = 0;
        if (digits.size() == 1 || digits.size() == 2) {
            hours = std::stoi(digits);
        } else if (digits.size() == 4) {
            hours = std::stoi(digits.substr(0, 2));
            minutes = std::stoi(digits.substr(2, 2));
        } else if (digits.size() == 6) {
            hours = std::stoi(digits.substr(0, 2));
            minutes = std::stoi(digits.substr(2, 2));
            seconds = std::stoi(digits.substr(4, 2));
        } else {
            return Status::Invalid(fmt::format("Invalid zone offset: {}", zone_id));
        }
        if (hours > 18 || minutes > 59 || seconds > 59) {
            return Status::Invalid(fmt::format("Invalid zone offset: {}", zone_id));
        }
        int32_t total = hours * 3600 + minutes * 60 + seconds;
        return negative ? -total : total;
    }
    // IANA region id, resolved at the given instant (honoring DST).
    try {
        const auto* zone = arrow_vendored::date::locate_zone(zone_id);
        std::chrono::time_point<std::chrono::system_clock, std::chrono::microseconds> tp{
            std::chrono::microseconds(micros_since_epoch)};
        auto info = zone->get_info(tp);
        return static_cast<int32_t>(info.offset.count());
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("Invalid zone id: {}, {}", zone_id, e.what()));
    }
}

}  // namespace paimon
