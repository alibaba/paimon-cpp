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

#include "paimon/common/predicate/like.h"

#include <string>
#include <vector>

#include "fmt/format.h"
namespace paimon {

namespace {

/// Returns the byte length of a UTF-8 leading byte's code point.
/// Returns 1 for ASCII, 2-4 for multi-byte sequences, 1 for invalid bytes.
inline size_t Utf8CodePointLength(unsigned char leading_byte) {
    if (leading_byte < 0x80) {
        return 1;
    }
    if ((leading_byte & 0xE0) == 0xC0) {
        return 2;
    }
    if ((leading_byte & 0xF0) == 0xE0) {
        return 3;
    }
    if ((leading_byte & 0xF8) == 0xF0) {
        return 4;
    }
    return 1;  // invalid continuation byte, treat as single byte
}

inline bool IsJavaRegexLineTerminator(const std::string& code_point) {
    return code_point == "\n" || code_point == "\r" || code_point == "\xC2\x85" ||
           code_point == "\xE2\x80\xA8" || code_point == "\xE2\x80\xA9";
}

}  // namespace

Result<bool> Like::TestString(const std::string& field, const std::string& pattern) const {
    if (pattern.empty()) {
        return field.empty();
    }

    // Phase 1: Parse pattern with escape handling (Java-compatible).
    // Only \_, \%, \\ are valid escape sequences.
    std::vector<std::string> pat_chars;  // each element is a literal string segment or wildcard
    std::vector<bool> is_wild;

    for (size_t i = 0; i < pattern.size();) {
        if (pattern[i] == '\\') {
            if (i + 1 >= pattern.size()) {
                return Status::Invalid(fmt::format("Invalid escape sequence '{}', index={}",
                                                   pattern, std::to_string(i)));
            }
            char next_char = pattern[i + 1];
            if (next_char != '_' && next_char != '%' && next_char != '\\') {
                return Status::Invalid(fmt::format("Invalid escape sequence '{}', index={}",
                                                   pattern, std::to_string(i)));
            }
            pat_chars.emplace_back(1, next_char);
            is_wild.push_back(false);
            i += 2;
        } else if (pattern[i] == '_' || pattern[i] == '%') {
            pat_chars.emplace_back(1, pattern[i]);
            is_wild.push_back(true);
            ++i;
        } else {
            // Read one UTF-8 code point from pattern as a literal element.
            size_t cp_len = Utf8CodePointLength(static_cast<unsigned char>(pattern[i]));
            if (i + cp_len > pattern.size()) {
                cp_len = 1;
            }
            pat_chars.push_back(pattern.substr(i, cp_len));
            is_wild.push_back(false);
            i += cp_len;
        }
    }

    // Phase 2: Merge consecutive '%' wildcards.
    std::vector<std::string> simp_pat;
    std::vector<bool> simp_wild;
    for (size_t i = 0; i < pat_chars.size(); ++i) {
        if (is_wild[i] && pat_chars[i] == "%" && !simp_pat.empty() && simp_wild.back() &&
            simp_pat.back() == "%") {
            continue;
        }
        simp_pat.push_back(pat_chars[i]);
        simp_wild.push_back(is_wild[i]);
    }

    // Phase 3: Decompose field into UTF-8 code points for character-level matching.
    std::vector<std::string> field_chars;
    for (size_t i = 0; i < field.size();) {
        size_t cp_len = Utf8CodePointLength(static_cast<unsigned char>(field[i]));
        if (i + cp_len > field.size()) {
            cp_len = 1;  // truncated sequence, treat byte as single char
        }
        field_chars.push_back(field.substr(i, cp_len));
        i += cp_len;
    }

    const size_t m = field_chars.size();
    const size_t n = simp_pat.size();

    if (m == 0) {
        return n == 1 && simp_wild[0] && simp_pat[0] == "%";
    }

    // Quick reject: count minimum required characters (non-wildcard pattern elements).
    size_t min_len = 0;
    for (size_t i = 0; i < n; ++i) {
        if (!simp_wild[i]) {
            min_len++;
        } else if (simp_pat[i] == "_") {
            min_len++;
        }
    }
    if (min_len > m) {
        return false;
    }

    // Phase 4: DP matching at character (code point) level.
    std::vector<bool> dp(n + 1, false);
    dp[0] = true;
    for (size_t j = 1; j <= n && simp_wild[j - 1] && simp_pat[j - 1] == "%"; ++j) {
        dp[j] = true;
    }

    for (size_t i = 0; i < m; ++i) {
        const std::string& field_char = field_chars[i];
        bool prev = dp[0];
        dp[0] = false;
        bool has_match = false;
        for (size_t j = 1; j <= n; ++j) {
            const bool temp = dp[j];
            const std::string& pc = simp_pat[j - 1];
            const bool wild = simp_wild[j - 1];
            if (wild && pc == "%") {
                dp[j] = dp[j - 1] || dp[j];
            } else if (wild && pc == "_") {
                dp[j] = prev && !IsJavaRegexLineTerminator(field_char);
            } else {
                dp[j] = (pc == field_char) ? prev : false;
            }
            has_match |= dp[j];
            prev = temp;
        }
        if (!has_match) {
            return false;
        }
    }
    return static_cast<bool>(dp[n]);
}
}  // namespace paimon
