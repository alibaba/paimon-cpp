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
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "paimon/common/predicate/null_false_leaf_binary_function.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace paimon {
class LeafFunction;

/// A `NullFalseLeafBinaryFunction` to eval less or equal.
class LessOrEqual : public NullFalseLeafBinaryFunction {
 public:
    static const LessOrEqual& Instance() {
        static const LessOrEqual instance = LessOrEqual();
        return instance;
    }

    Result<bool> Test(const Literal& field, const Literal& literal) const override {
        PAIMON_ASSIGN_OR_RAISE(int32_t compare_res, field.CompareTo(literal));
        return compare_res <= 0;
    }
    Result<bool> Test(int64_t row_count, const Literal& min_value, const Literal& max_value,
                      const std::optional<int64_t>& null_count,
                      const Literal& literal) const override {
        PAIMON_ASSIGN_OR_RAISE(int32_t res, literal.CompareTo(min_value));
        return res >= 0;
    }
    Type GetType() const override {
        return Type::LESS_OR_EQUAL;
    }
    const LeafFunction* Negate() const override;

    std::string ToString() const override {
        return "LessOrEqual";
    }

 private:
    LessOrEqual() = default;
};
}  // namespace paimon
