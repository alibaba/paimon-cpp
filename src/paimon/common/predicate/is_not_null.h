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

#include "paimon/common/predicate/leaf_unary_function.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace paimon {
class LeafFunction;

/// A `LeafUnaryFunction` to eval is not null.
class IsNotNull : public LeafUnaryFunction {
 public:
    static const IsNotNull& Instance() {
        static const IsNotNull instance = IsNotNull();
        return instance;
    }

    Result<bool> Test(const Literal& field) const override {
        return !field.IsNull();
    }
    Result<bool> Test(int64_t row_count, const Literal& min_value, const Literal& max_value,
                      const std::optional<int64_t>& null_count) const override {
        return null_count == std::nullopt || null_count.value() < row_count;
    }

    Type GetType() const override {
        return Type::IS_NOT_NULL;
    }
    const LeafFunction* Negate() const override;
    std::string ToString() const override {
        return "IsNotNull";
    }

 private:
    IsNotNull() = default;
};
}  // namespace paimon
