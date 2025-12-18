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

#include "paimon/common/predicate/compound_function.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/predicate/compound_predicate.h"

namespace paimon {
class CompoundPredicateImpl : public CompoundPredicate, public PredicateFilter {
 public:
    CompoundPredicateImpl(const CompoundFunction& compound_function,
                          const std::vector<std::shared_ptr<Predicate>>& children)
        : CompoundPredicate(compound_function, children) {}

    Result<std::vector<char>> Test(const arrow::Array& array) const override {
        return compound_function_.Test(array, children_);
    }

    Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema,
                      const InternalRow& row) const override {
        return compound_function_.Test(schema, row, children_);
    }

    Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema, int64_t row_count,
                      const InternalRow& min_values, const InternalRow& max_values,
                      const InternalArray& null_counts) const override {
        return compound_function_.Test(schema, row_count, min_values, max_values, null_counts,
                                       children_);
    }

    std::shared_ptr<CompoundPredicateImpl> NewCompoundPredicate(
        const std::vector<std::shared_ptr<Predicate>>& new_children) const {
        return std::make_shared<CompoundPredicateImpl>(compound_function_, new_children);
    }
};
}  // namespace paimon
