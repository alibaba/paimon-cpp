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

namespace paimon {

Result<bool> Like::TestString(const std::string& field, const std::string& pattern) const {
    if (pattern.empty()) {
        return field.empty();
    }
    std::vector<char> pat;
    std::vector<bool> is_wild;
    for (size_t i = 0; i < pattern.size(); ++i) {
        if (pattern[i] == '\\' && i + 1 < pattern.size()) {
            pat.push_back(pattern[i + 1]);
            is_wild.push_back(false);
            ++i;
        } else {
            char c = pattern[i];
            pat.push_back(c);
            is_wild.push_back(c == '_' || c == '%');
        }
    }
    std::vector<char> simp_pat;
    std::vector<bool> simp_wild;
    for (size_t i = 0; i < pat.size(); ++i) {
        if (is_wild[i] && pat[i] == '%' && !simp_pat.empty() && simp_wild.back() &&
            simp_pat.back() == '%') {
            continue;
        }
        simp_pat.push_back(pat[i]);
        simp_wild.push_back(is_wild[i]);
    }
    const size_t m = field.size();
    const size_t n = simp_pat.size();
    if (field.empty()) {
        return n == 1 && simp_wild[0] && simp_pat[0] == '%';
    }
    size_t min_len = 0;
    for (size_t i = 0; i < n; ++i) {
        if (!simp_wild[i]) {
            min_len++;
        }
    }
    if (min_len > m) {
        return false;
    }
    constexpr size_t STACK_LIMIT = 128;
    std::unique_ptr<bool[]> dp_storage;
    bool* dp;
    if (n <= STACK_LIMIT) {
        dp = static_cast<bool*>(alloca((n + 1) * sizeof(bool)));
    } else {
        dp_storage = std::make_unique<bool[]>(n + 1);
        dp = dp_storage.get();
    }
    std::fill_n(dp, n + 1, false);
    dp[0] = true;
    for (size_t j = 1; j <= n && simp_wild[j - 1] && simp_pat[j - 1] == '%'; ++j) {
        dp[j] = true;
    }
    const char* f = field.data();
    for (size_t i = 0; i < m; ++i) {
        const char sc = f[i];
        bool prev = dp[0];
        dp[0] = false;
        bool has_match = false;
        for (size_t j = 1; j <= n; ++j) {
            const bool temp = dp[j];
            const char pc = simp_pat[j - 1];
            const bool wild = simp_wild[j - 1];
            if (wild && pc == '%') {
                dp[j] = dp[j - 1] || dp[j];
            } else if (wild && pc == '_') {
                dp[j] = prev;
            } else {
                dp[j] = (pc == sc) ? prev : false;
            }
            has_match |= dp[j];
            prev = temp;
        }
        if (!has_match) {
            return false;
        }
    }
    return dp[n];
}
}  // namespace paimon
