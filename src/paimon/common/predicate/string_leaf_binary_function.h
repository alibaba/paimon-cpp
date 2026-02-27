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

#pragma once

#include "paimon/common/predicate/null_false_leaf_binary_function.h"

namespace paimon {
class LeafFunction;

class StringLeafBinaryFunction : public NullFalseLeafBinaryFunction {
 public:
    virtual Result<bool> TestString(const std::string& field, const std::string& pattern) const = 0;

    Result<bool> Test(const Literal& field, const Literal& pattern_literal) const override {
        return TestString(field.GetValue<std::string>(), pattern_literal.GetValue<std::string>());
    }

    Result<bool> Test(int64_t row_count, const Literal& min_value, const Literal& max_value,
                      const std::optional<int64_t>& null_count,
                      const Literal& literal) const override {
        return true;
    }

    const LeafFunction* Negate() const override {
        return nullptr;
    }
};
}  // namespace paimon
