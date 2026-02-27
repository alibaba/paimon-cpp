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

#include "paimon/common/predicate/starts_with.h"

#include "paimon/common/utils/string_utils.h"

namespace paimon {

Result<bool> StartsWith::TestString(const std::string& field, const std::string& pattern) const {
    return StringUtils::StartsWith(field, pattern);
}

Result<bool> StartsWith::Test(int64_t row_count, const Literal& min_value, const Literal& max_value,
                              const std::optional<int64_t>& null_count,
                              const Literal& pattern_literal) const {
    const auto min_str = min_value.GetValue<std::string>();
    const auto max_str = max_value.GetValue<std::string>();
    const auto pattern_str = pattern_literal.GetValue<std::string>();
    PAIMON_ASSIGN_OR_RAISE(const auto min_test, TestString(min_str, pattern_str));
    PAIMON_ASSIGN_OR_RAISE(const auto max_test, TestString(max_str, pattern_str));
    return (min_test || min_str.compare(pattern_str) <= 0) &&
           (max_test || max_str.compare(pattern_str) >= 0);
}
}  // namespace paimon
