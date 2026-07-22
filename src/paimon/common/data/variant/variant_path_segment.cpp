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

#include "paimon/common/data/variant/variant_path_segment.h"

#include <limits>
#include <string_view>

#include "fmt/format.h"

namespace paimon {

namespace {

// Matches `[123]` at the beginning of `remaining` and consumes it.
bool TryParseIndex(std::string_view* remaining, int32_t* index) {
    std::string_view s = *remaining;
    if (s.empty() || s[0] != '[') {
        return false;
    }
    size_t i = 1;
    int64_t value = 0;
    size_t digits = 0;
    while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
        value = value * 10 + (s[i] - '0');
        if (value > std::numeric_limits<int32_t>::max()) {
            return false;
        }
        ++i;
        ++digits;
    }
    if (digits == 0 || i >= s.size() || s[i] != ']') {
        return false;
    }
    *index = static_cast<int32_t>(value);
    remaining->remove_prefix(i + 1);
    return true;
}

// Matches `['key']` or `["key"]` at the beginning of `remaining` and consumes it.
bool TryParseQuotedKey(std::string_view* remaining, std::string* key) {
    std::string_view s = *remaining;
    if (s.size() < 4 || s[0] != '[' || (s[1] != '\'' && s[1] != '"')) {
        return false;
    }
    char quote = s[1];
    size_t end = s.find(quote, 2);
    if (end == std::string_view::npos || end == 2 || end + 1 >= s.size() || s[end + 1] != ']') {
        return false;
    }
    key->assign(s.substr(2, end - 2));
    remaining->remove_prefix(end + 2);
    return true;
}

// Matches `.key` (one or more characters that are neither `.` nor `[`) at the beginning of
// `remaining` and consumes it.
bool TryParseDotKey(std::string_view* remaining, std::string* key) {
    std::string_view s = *remaining;
    if (s.size() < 2 || s[0] != '.') {
        return false;
    }
    size_t end = 1;
    while (end < s.size() && s[end] != '.' && s[end] != '[') {
        ++end;
    }
    if (end == 1) {
        return false;
    }
    key->assign(s.substr(1, end - 1));
    remaining->remove_prefix(end);
    return true;
}

}  // namespace

Result<std::vector<VariantPathSegment>> VariantPathSegment::Parse(const std::string& path) {
    // Mirrors the Java parser: the root `$` is located with a find, so segments are parsed
    // after the FIRST `$` and any prefix before it is ignored.
    size_t root = path.find('$');
    if (path.empty() || root == std::string::npos) {
        return Status::Invalid(fmt::format("Invalid path: {}", path));
    }
    std::string_view remaining = std::string_view(path).substr(root + 1);
    std::vector<VariantPathSegment> segments;
    while (!remaining.empty()) {
        int32_t index = 0;
        if (TryParseIndex(&remaining, &index)) {
            segments.push_back(ArrayExtraction(index));
            continue;
        }
        std::string key;
        if (TryParseDotKey(&remaining, &key) || TryParseQuotedKey(&remaining, &key)) {
            segments.push_back(ObjectExtraction(std::move(key)));
            continue;
        }
        return Status::Invalid(fmt::format("Invalid path: {}", path));
    }
    return segments;
}

}  // namespace paimon
