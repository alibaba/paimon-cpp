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

#include <vector>

#include "arrow/array/array_nested.h"
#include "arrow/c/bridge.h"
#include "arrow/util/checked_cast.h"
#include "paimon/common/predicate/leaf_function.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/status.h"

namespace paimon {
class LeafUnaryFunction : public LeafFunction {
 public:
    Result<std::vector<char>> Test(const arrow::Array& array,
                                   const std::vector<Literal>& literals) const override {
        std::vector<char> is_valid(array.length(), false);
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<Literal> array_values,
            LiteralConverter::ConvertLiteralsFromArray(array, /*own_data=*/false));
        for (int64_t i = 0; i < array.length(); i++) {
            PAIMON_ASSIGN_OR_RAISE(is_valid[i], Test(array_values[i]));
        }
        return is_valid;
    }

    Result<bool> Test(const Literal& value, const std::vector<Literal>& literals) const override {
        return Test(value);
    }

    Result<bool> Test(int64_t row_count, const Literal& min_value, const Literal& max_value,
                      const std::optional<int64_t>& null_count,
                      const std::vector<Literal>& literals) const override {
        return Test(row_count, min_value, max_value, null_count);
    }

    virtual Result<bool> Test(const Literal& field) const = 0;
    virtual Result<bool> Test(int64_t row_count, const Literal& min_value, const Literal& max_value,
                              const std::optional<int64_t>& null_count) const = 0;
};
}  // namespace paimon
