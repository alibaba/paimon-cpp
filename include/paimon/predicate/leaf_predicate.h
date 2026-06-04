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

#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/visibility.h"

namespace paimon {
class LeafFunction;
class Function;
enum class FieldType;

/// Leaf node of a `Predicate` tree. Compares a field with literals.
class PAIMON_EXPORT LeafPredicate : virtual public Predicate {
 public:
    /// The field's position in the schema this predicate is currently bound to.
    /// At construction the value reflects the schema the caller supplied to
    /// `PredicateBuilder`; predicates obtained from `InternalReadContext::GetPredicate()`
    /// have already been projected onto the read schema (see
    /// `InternalReadContext::GetPredicate()` for the projection semantics).
    int32_t FieldIndex() const {
        return field_index_;
    }
    const std::string& FieldName() const {
        return field_name_;
    }
    FieldType GetFieldType() const {
        return field_type_;
    }
    const std::vector<Literal>& Literals() const {
        return literals_;
    }
    const Function& GetFunction() const override;

    std::string ToString() const override;

    std::shared_ptr<Predicate> Negate() const override;

    bool operator==(const Predicate& other) const override;

 protected:
    LeafPredicate(const LeafFunction& leaf_function, int32_t field_index,
                  const std::string& field_name, const FieldType& field_type,
                  const std::vector<Literal>& literals);

    const LeafFunction& leaf_function_;
    int32_t field_index_;
    std::string field_name_;
    FieldType field_type_;
    std::vector<Literal> literals_;
};

}  // namespace paimon
