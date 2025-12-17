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

#include <memory>
#include <vector>

#include "arrow/array/array_base.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"

namespace paimon {
class CompoundFunction : public Function {
 public:
    // input array is the struct array of all fields
    virtual Result<std::vector<char>> Test(
        const arrow::Array& array,
        const std::vector<std::shared_ptr<Predicate>>& children) const = 0;

    virtual Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema, const InternalRow& row,
                              const std::vector<std::shared_ptr<Predicate>>& children) const = 0;

    virtual Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema, int64_t row_count,
                              const InternalRow& min_values, const InternalRow& max_values,
                              const InternalArray& null_counts,
                              const std::vector<std::shared_ptr<Predicate>>& children) const = 0;

    virtual const CompoundFunction& Negate() const = 0;
};
}  // namespace paimon
