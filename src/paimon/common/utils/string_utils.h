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

#include <algorithm>
#include <cassert>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

#include "fmt/core.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/data/timestamp.h"
#include "paimon/result.h"
#include "paimon/traits.h"
#include "paimon/visibility.h"

namespace paimon {

/// Utils for string.
class PAIMON_EXPORT StringUtils {
 public:
    /// Replaces all occurrences of a string within another string.
    ///
    /// A `null` reference passed to this method is a no-op.
    ///
    /// <pre>
    /// StringUtils::Replace(null, *, *)        = null
    /// StringUtils::Replace("", *, *)          = ""
    /// StringUtils::Replace("any", null, *)    = "any"
    /// StringUtils::Replace("any", *, null)    = "any"
    /// StringUtils::Replace("any", "", *)      = "any"
    /// StringUtils::Replace("aba", "a", null)  = "aba"
    /// StringUtils::Replace("aba", "a", "")    = "b"
    /// StringUtils::Replace("aba", "a", "z")   = "zbz"
    /// </pre>
    ///
    /// @see #replace(string text, string search_string, string replacement, int max)
    /// @param text text to search and replace in, may be null
    /// @param search_string the String to search for, may be null
    /// @param replacement the String to replace it with, may be null
    /// @return the text with any replacements processed, `null` if null string input
    static std::string Replace(const std::string& text, const std::string& search_string,
                               const std::string& replacement) {
        return Replace(text, search_string, replacement, -1);
    }

    /// Replaces a String with another String inside a larger String, for the first `max` values of
    /// the search String.
    ///
    /// A `null` reference passed to this method is a no-op.
    ///
    /// <pre>
    /// StringUtils::Replace(null, *, *, *)         = null
    /// StringUtils::Replace("", *, *, *)           = ""
    /// StringUtils::Replace("any", null, *, *)     = "any"
    /// StringUtils::Replace("any", *, null, *)     = "any"
    /// StringUtils::Replace("any", "", *, *)       = "any"
    /// StringUtils::Replace("any", *, *, 0)        = "any"
    /// StringUtils::Replace("abaa", "a", null, -1) = "abaa"
    /// StringUtils::Replace("abaa", "a", "", -1)   = "b"
    /// StringUtils::Replace("abaa", "a", "z", 0)   = "abaa"
    /// StringUtils::Replace("abaa", "a", "z", 1)   = "zbaa"
    /// StringUtils::Replace("abaa", "a", "z", 2)   = "zbza"
    /// StringUtils::Replace("abaa", "a", "z", -1)  = "zbzz"
    /// </pre>
    ///
    /// @param text text to search and replace in, may be null
    /// @param search_string the String to search for, may be null
    /// @param replacement the String to replace it with, may be null
    /// @param max maximum number of values to replace, or `-1` if no maximum
    /// @return the text with any replacements processed, `null` if null string input
    static std::string Replace(const std::string& text, const std::string& search_string,
                               const std::string& replacement, int32_t max);

    static std::string ReplaceLast(const std::string& text, const std::string& old_str,
                                   const std::string& new_str);

    static bool StartsWith(const std::string& str, const std::string& prefix, size_t start_pos = 0);

    static bool EndsWith(const std::string& str, const std::string& suffix);

    static bool IsNullOrWhitespaceOnly(const std::string& str);

    static void Trim(std::string* str);

    static std::string ToLowerCase(const std::string& str);
    static std::string ToUpperCase(const std::string& str);

    template <typename T>
    static std::string VectorToString(const std::vector<T>& vec) {
        std::vector<std::string> strs;
        strs.reserve(vec.size());
        for (const auto& value : vec) {
            if constexpr (is_optional<T>::value) {
                if (value == std::nullopt) {
                    strs.emplace_back("null");
                } else {
                    strs.emplace_back(value.value().ToString());
                }
            } else if constexpr (is_pointer<T>::value) {
                strs.emplace_back(value->ToString());
            } else {
                strs.emplace_back(value.ToString());
            }
        }
        return fmt::format("[{}]", fmt::join(strs, ", "));
    }

    static std::vector<std::string> Split(const std::string& text, const std::string& sep_str,
                                          bool ignore_empty = true);

    static std::vector<std::vector<std::string>> Split(const std::string& text,
                                                       const std::string& delim1,
                                                       const std::string& delim2);

    static Result<int32_t> StringToDate(const std::string& str);

    template <typename T>
    static std::optional<T> StringToValue(const std::string& str);
};

template <typename T>
std::optional<T> StringUtils::StringToValue(const std::string& str) {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    if (str.length() == 0) {
        return std::nullopt;
    }

    if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                  std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                  std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> ||
                  std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>) {
        T value{};
        int32_t base = 10;
        auto str_data = str.data();
        auto str_size = str.size();
        if constexpr (std::is_unsigned_v<T>) {
            if (str_data[0] == '-') {
                return std::nullopt;
            }
        }
        auto result = std::from_chars(str_data, str_data + str_size, value, base);
        if (result.ec != std::errc() || result.ptr != str_data + str_size) {
            return std::nullopt;
        } else {
            return value;
        }
    } else if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
        T value;
        std::istringstream iss(str);
        iss >> value;
        if (iss && iss.eof()) {
            return value;
        }
        return std::nullopt;
    } else if constexpr (std::is_same_v<T, bool>) {
        static const std::set<std::string> TRUE_STRINGS = {"t", "true", "y", "yes", "1"};
        static const std::set<std::string> FALSE_STRINGS = {"f", "false", "n", "no", "0"};
        std::string lower_case = ToLowerCase(str);
        if (TRUE_STRINGS.find(lower_case) != TRUE_STRINGS.end()) {
            return true;
        } else if (FALSE_STRINGS.find(lower_case) != FALSE_STRINGS.end()) {
            return false;
        } else {
            return std::nullopt;
        }
    } else {
        assert(false);
        return std::nullopt;
    }
}

template <>
inline std::optional<std::string> StringUtils::StringToValue<std::string>(const std::string& str) {
    return str;
}

}  // namespace paimon
