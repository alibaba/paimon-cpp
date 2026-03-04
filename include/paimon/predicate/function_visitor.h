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

#include <string>
#include <vector>

#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
/// A visitor interface for evaluating filter predicates on indexed columns.
/// @tparam T The result type produced by each visit method (e.g., a file index result or global
/// index result).
template <typename T>
class PAIMON_EXPORT FunctionVisitor {
 public:
    virtual ~FunctionVisitor() = default;

    /// Evaluates the IS NOT NULL predicate on the indexed column.
    virtual Result<T> VisitIsNotNull() = 0;

    /// Evaluates the IS NULL predicate on the indexed column.
    virtual Result<T> VisitIsNull() = 0;

    /// Evaluates the equality (==) predicate against the given literal.
    virtual Result<T> VisitEqual(const Literal& literal) = 0;

    /// Evaluates the inequality (!=) predicate against the given literal.
    virtual Result<T> VisitNotEqual(const Literal& literal) = 0;

    /// Evaluates the less-than (<) predicate against the given literal.
    virtual Result<T> VisitLessThan(const Literal& literal) = 0;

    /// Evaluates the less-than-or-equal (<=) predicate against the given literal.
    virtual Result<T> VisitLessOrEqual(const Literal& literal) = 0;

    /// Evaluates the greater-than (>) predicate against the given literal.
    virtual Result<T> VisitGreaterThan(const Literal& literal) = 0;

    /// Evaluates the greater-than-or-equal (>=) predicate against the given literal.
    virtual Result<T> VisitGreaterOrEqual(const Literal& literal) = 0;

    /// Evaluates the IN predicate against a list of literals.
    virtual Result<T> VisitIn(const std::vector<Literal>& literals) = 0;

    /// Evaluates the NOT IN predicate against a list of literals.
    virtual Result<T> VisitNotIn(const std::vector<Literal>& literals) = 0;

    /// Evaluates whether string values start with the given prefix.
    virtual Result<T> VisitStartsWith(const Literal& prefix) = 0;

    /// Evaluates whether string values end with the given prefix.
    virtual Result<T> VisitEndsWith(const Literal& suffix) = 0;

    /// Evaluates whether string values contain the given substring.
    virtual Result<T> VisitContains(const Literal& literal) = 0;

    /// Evaluates whether string values like the given string.
    virtual Result<T> VisitLike(const Literal& literal) = 0;

    /// Evaluates the BETWEEN predicate with the given lower and upper bounds.
    virtual Result<T> VisitBetween(const Literal& from, const Literal& to) {
        // Default implementation: BETWEEN is equivalent to >= AND <=
        auto lower_result = VisitGreaterOrEqual(from);
        if (!lower_result.ok()) {
            return lower_result.status();
        }
        auto upper_result = VisitLessOrEqual(to);
        if (!upper_result.ok()) {
            return upper_result.status();
        }
        return VisitAnd({std::move(lower_result).value(), std::move(upper_result).value()});
    }

    /// Evaluates the NOT BETWEEN predicate with the given lower and upper bounds.
    virtual Result<T> VisitNotBetween(const Literal& from, const Literal& to) {
        // Default implementation: NOT BETWEEN is equivalent to < OR >
        auto lower_result = VisitLessThan(from);
        if (!lower_result.ok()) {
            return lower_result.status();
        }
        auto upper_result = VisitGreaterThan(to);
        if (!upper_result.ok()) {
            return upper_result.status();
        }
        return VisitOr({std::move(lower_result).value(), std::move(upper_result).value()});
    }

    // ----------------- Compound functions ------------------------

    /// Evaluates the AND predicate across multiple child results.
    virtual Result<T> VisitAnd(const std::vector<Result<T>>& children) = 0;

    /// Evaluates the OR predicate across multiple child results.
    virtual Result<T> VisitOr(const std::vector<Result<T>>& children) = 0;
};
}  // namespace paimon
