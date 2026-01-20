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
#include <memory>
#include <string>
#include <vector>

#include "paimon/predicate/predicate.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
class Literal;
enum class FieldType;

/// A utility class to create `Predicate` object for common filter conditions.
///
/// PredicateBuilder provides static factory methods to create various types of predicates
/// that can be used for filtering data in Paimon tables.
class PAIMON_EXPORT PredicateBuilder {
 public:
    PredicateBuilder() = delete;
    ~PredicateBuilder() = delete;

    /// Create an equality predicate (field == literal).
    ///
    /// @param field_index The index of the field in read schema (0-based).
    /// @param field_name The name of the field.
    /// @param field_type The data type of the field.
    /// @param literal The literal value to compare against.
    /// @return A shared pointer to the created Predicate object.
    static std::shared_ptr<Predicate> Equal(int32_t field_index, const std::string& field_name,
                                            const FieldType& field_type, const Literal& literal);

    /// Create a not-equal predicate (field != literal).
    static std::shared_ptr<Predicate> NotEqual(int32_t field_index, const std::string& field_name,
                                               const FieldType& field_type, const Literal& literal);

    /// Create a less-than predicate (field < literal).
    static std::shared_ptr<Predicate> LessThan(int32_t field_index, const std::string& field_name,
                                               const FieldType& field_type, const Literal& literal);

    /// Create a less-than-or-equal predicate (field <= literal).
    static std::shared_ptr<Predicate> LessOrEqual(int32_t field_index,
                                                  const std::string& field_name,
                                                  const FieldType& field_type,
                                                  const Literal& literal);

    /// Create a greater-than predicate (field > literal).
    static std::shared_ptr<Predicate> GreaterThan(int32_t field_index,
                                                  const std::string& field_name,
                                                  const FieldType& field_type,
                                                  const Literal& literal);

    /// Create a greater-than-or-equal predicate (field >= literal).
    static std::shared_ptr<Predicate> GreaterOrEqual(int32_t field_index,
                                                     const std::string& field_name,
                                                     const FieldType& field_type,
                                                     const Literal& literal);

    /// Create an IS NULL predicate (field IS NULL).
    static std::shared_ptr<Predicate> IsNull(int32_t field_index, const std::string& field_name,
                                             const FieldType& field_type);

    /// Create an IS NOT NULL predicate (field IS NOT NULL).
    static std::shared_ptr<Predicate> IsNotNull(int32_t field_index, const std::string& field_name,
                                                const FieldType& field_type);

    /// Create an IN predicate (field IN (literal1, literal2, ...)).
    ///
    /// Tests whether the field value matches any of the provided literal values.
    static std::shared_ptr<Predicate> In(int32_t field_index, const std::string& field_name,
                                         const FieldType& field_type,
                                         const std::vector<Literal>& literals);

    /// Create a NOT IN predicate (field NOT IN (literal1, literal2, ...)).
    ///
    /// Tests whether the field value does not match any of the provided literal values.
    static std::shared_ptr<Predicate> NotIn(int32_t field_index, const std::string& field_name,
                                            const FieldType& field_type,
                                            const std::vector<Literal>& literals);

    /// Create a BETWEEN predicate (field BETWEEN lower_bound AND upper_bound).
    ///
    /// Tests whether the field value falls within the specified range (inclusive on both ends).
    ///
    /// @param field_index The index of the field in read schema (0-based).
    /// @param field_name The name of the field.
    /// @param field_type The data type of the field.
    /// @param included_lower_bound The lower bound of the range (inclusive).
    /// @param included_upper_bound The upper bound of the range (inclusive).
    static std::shared_ptr<Predicate> Between(int32_t field_index, const std::string& field_name,
                                              const FieldType& field_type,
                                              const Literal& included_lower_bound,
                                              const Literal& included_upper_bound);

    /// Create an AND predicate combining multiple predicates.
    ///
    /// Creates a logical AND operation that evaluates to true only when all input predicates
    /// evaluate to true.
    ///
    /// @param predicates A vector of shared pointers to the predicates, which must not be empty.
    static Result<std::shared_ptr<Predicate>> And(
        const std::vector<std::shared_ptr<Predicate>>& predicates);

    /// Create an OR predicate combining multiple predicates.
    ///
    /// Creates a logical OR operation that evaluates to true when at least one of the input
    /// predicates evaluates to true.
    ///
    /// @param predicates A vector of shared pointers to the predicates, which must not be empty.
    static Result<std::shared_ptr<Predicate>> Or(
        const std::vector<std::shared_ptr<Predicate>>& predicates);

    /// Create a NOT predicate negating the result of another predicate.
    ///
    /// Creates a logical NOT operation that inverts the truth value of the input predicate.
    ///
    /// @param predicate A shared pointer to the predicate to be negated, which must not be nullptr.
    static Result<std::shared_ptr<Predicate>> Not(const std::shared_ptr<Predicate>& predicate);
};
}  // namespace paimon
