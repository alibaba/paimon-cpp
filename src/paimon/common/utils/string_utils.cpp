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

#include "paimon/common/utils/string_utils.h"

#include <algorithm>
#include <cctype>
#include <ctime>
#include <iomanip>
#include <iterator>
#include <utility>

#include "fmt/format.h"
#include "paimon/status.h"

namespace paimon {
std::string StringUtils::Replace(const std::string& text, const std::string& search_string,
                                 const std::string& replacement, int32_t max) {
    std::string str = text;
    size_t pos = str.find(search_string);
    int32_t count = 0;
    while (pos != std::string::npos && (count < max || max == -1)) {
        str.replace(pos, search_string.size(), replacement);
        pos = str.find(search_string, pos + replacement.size());
        count++;
    }
    return str;
}

std::string StringUtils::ReplaceLast(const std::string& text, const std::string& old_str,
                                     const std::string& new_str) {
    std::string str = text;
    size_t pos = str.rfind(old_str);
    if (pos != std::string::npos) {
        str.replace(pos, old_str.size(), new_str);
    }
    return str;
}

bool StringUtils::StartsWith(const std::string& str, const std::string& prefix, size_t start_pos) {
    return (str.size() >= prefix.size()) && (str.compare(start_pos, prefix.size(), prefix) == 0);
}
bool StringUtils::EndsWith(const std::string& str, const std::string& suffix) {
    size_t s1 = str.size();
    size_t s2 = suffix.size();
    return (s1 >= s2) && (str.compare(s1 - s2, s2, suffix) == 0);
}
bool StringUtils::IsNullOrWhitespaceOnly(const std::string& str) {
    if (str.empty()) {
        return true;
    }
    for (char c : str) {
        if (!std::isspace(static_cast<unsigned char>(c))) {
            return false;
        }
    }
    return true;
}

void StringUtils::Trim(std::string* str) {
    str->erase(str->find_last_not_of(' ') + 1);
    str->erase(0, str->find_first_not_of(' '));
}

std::string StringUtils::ToLowerCase(const std::string& str) {
    std::string result;
    result.reserve(str.length());
    std::transform(str.begin(), str.end(), std::back_inserter(result),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
}

std::string StringUtils::ToUpperCase(const std::string& str) {
    std::string result;
    result.reserve(str.length());
    std::transform(str.begin(), str.end(), std::back_inserter(result),
                   [](unsigned char c) { return std::toupper(c); });
    return result;
}

std::vector<std::string> StringUtils::Split(const std::string& text, const std::string& sep_str,
                                            bool ignore_empty) {
    std::vector<std::string> vec;
    if (sep_str.empty()) {
        // invalid case, do not split.
        vec.emplace_back(text);
        return vec;
    }
    size_t n = 0, old = 0;
    while (n != std::string::npos) {
        n = text.find(sep_str, n);
        if (n != std::string::npos) {
            if (!ignore_empty || n != old) {
                vec.emplace_back(text.substr(old, n - old));
            }
            n += sep_str.length();
            old = n;
        }
    }

    if (!ignore_empty || old < text.length()) {
        vec.emplace_back(text.substr(old, text.length() - old));
    }
    return vec;
}

std::vector<std::vector<std::string>> StringUtils::Split(const std::string& text,
                                                         const std::string& delim1,
                                                         const std::string& delim2) {
    std::vector<std::vector<std::string>> result;
    std::vector<std::string> split_parts = Split(text, delim1);
    result.reserve(split_parts.size());
    for (auto& part : split_parts) {
        result.emplace_back(Split(part, delim2));
    }
    return result;
}

Result<int32_t> StringUtils::StringToDate(const std::string& str) {
    auto int_value = StringToValue<int32_t>(str);
    if (int_value) {
        return int_value.value();
    }
    std::tm timeinfo = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, nullptr};
    std::istringstream ss(str);
    ss >> std::get_time(&timeinfo, "%Y-%m-%d");
    if (ss.fail()) {
        return Status::Invalid(fmt::format("failed to convert string '{}' to date", str));
    }
    std::time_t time = timegm(&timeinfo);
    if (time == -1) {
        return Status::Invalid(fmt::format("failed to convert string '{}' to date", str));
    }
    static const int64_t SECONDS_PER_DAY = 86400l;  // = 24 * 60 * 60
    return time / SECONDS_PER_DAY;
}

}  // namespace paimon
