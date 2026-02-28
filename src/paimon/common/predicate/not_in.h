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
#include <vector>

#include "paimon/common/predicate/multi_literals_leaf_function.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace paimon {
class LeafFunction;

/// A `LeafFunction` to eval not in.
class NotIn : public MultiLiteralsLeafFunction {
 public:
    static const NotIn& Instance() {
        static const NotIn instance = NotIn();
        return instance;
    }

    Result<bool> InnerTest(const Literal& field,
                           const std::vector<Literal>& literals) const override {
        for (const auto& literal : literals) {
            if (literal.IsNull()) {
                return false;
            }
            PAIMON_ASSIGN_OR_RAISE(int32_t compare_res, field.CompareTo(literal));
            if (compare_res == 0) {
                return false;
            }
        }
        return true;
    }

    Result<bool> InnerTest(int64_t row_count, const Literal& min_value, const Literal& max_value,
                           const std::optional<int64_t>& null_count,
                           const std::vector<Literal>& literals) const override {
        for (const auto& literal : literals) {
            if (literal.IsNull()) {
                return false;
            }
            PAIMON_ASSIGN_OR_RAISE(int32_t min_ret, literal.CompareTo(min_value));
            PAIMON_ASSIGN_OR_RAISE(int32_t max_ret, literal.CompareTo(max_value));
            if (min_ret == 0 && max_ret == 0) {
                return false;
            }
        }
        return true;
    }

    Type GetType() const override {
        return Type::NOT_IN;
    }
    const LeafFunction* Negate() const override;
    std::string ToString() const override {
        return "NotIn";
    }

 private:
    NotIn() = default;
};
}  // namespace paimon
