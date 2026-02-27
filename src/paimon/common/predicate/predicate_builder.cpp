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

#include "paimon/predicate/predicate_builder.h"

#include <utility>

#include "paimon/common/predicate/and.h"
#include "paimon/common/predicate/compound_predicate_impl.h"
#include "paimon/common/predicate/contains.h"
#include "paimon/common/predicate/ends_with.h"
#include "paimon/common/predicate/equal.h"
#include "paimon/common/predicate/greater_or_equal.h"
#include "paimon/common/predicate/greater_than.h"
#include "paimon/common/predicate/in.h"
#include "paimon/common/predicate/is_not_null.h"
#include "paimon/common/predicate/is_null.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/common/predicate/less_or_equal.h"
#include "paimon/common/predicate/less_than.h"
#include "paimon/common/predicate/like.h"
#include "paimon/common/predicate/not_equal.h"
#include "paimon/common/predicate/not_in.h"
#include "paimon/common/predicate/or.h"
#include "paimon/common/predicate/starts_with.h"
#include "paimon/predicate/literal.h"
#include "paimon/status.h"

namespace paimon {
enum class FieldType;

// TODO(xinyu.lxy): predicate field_index use index in read schema now, but java paimon use index
// in file schema
std::shared_ptr<Predicate> PredicateBuilder::Equal(int32_t field_index,
                                                   const std::string& field_name,
                                                   const FieldType& field_type,
                                                   const Literal& literal) {
    return std::make_shared<LeafPredicateImpl>(Equal::Instance(), field_index, field_name,
                                               field_type, std::vector<Literal>({literal}));
}

std::shared_ptr<Predicate> PredicateBuilder::NotEqual(int32_t field_index,
                                                      const std::string& field_name,
                                                      const FieldType& field_type,
                                                      const Literal& literal) {
    return std::make_shared<LeafPredicateImpl>(NotEqual::Instance(), field_index, field_name,
                                               field_type, std::vector<Literal>({literal}));
}

std::shared_ptr<Predicate> PredicateBuilder::LessThan(int32_t field_index,
                                                      const std::string& field_name,
                                                      const FieldType& field_type,
                                                      const Literal& literal) {
    return std::make_shared<LeafPredicateImpl>(LessThan::Instance(), field_index, field_name,
                                               field_type, std::vector<Literal>({literal}));
}

std::shared_ptr<Predicate> PredicateBuilder::LessOrEqual(int32_t field_index,
                                                         const std::string& field_name,
                                                         const FieldType& field_type,
                                                         const Literal& literal) {
    return std::make_shared<LeafPredicateImpl>(LessOrEqual::Instance(), field_index, field_name,
                                               field_type, std::vector<Literal>({literal}));
}

std::shared_ptr<Predicate> PredicateBuilder::GreaterThan(int32_t field_index,
                                                         const std::string& field_name,
                                                         const FieldType& field_type,
                                                         const Literal& literal) {
    return std::make_shared<LeafPredicateImpl>(GreaterThan::Instance(), field_index, field_name,
                                               field_type, std::vector<Literal>({literal}));
}

std::shared_ptr<Predicate> PredicateBuilder::GreaterOrEqual(int32_t field_index,
                                                            const std::string& field_name,
                                                            const FieldType& field_type,
                                                            const Literal& literal) {
    return std::make_shared<LeafPredicateImpl>(GreaterOrEqual::Instance(), field_index, field_name,
                                               field_type, std::vector<Literal>({literal}));
}

std::shared_ptr<Predicate> PredicateBuilder::IsNull(int32_t field_index,
                                                    const std::string& field_name,
                                                    const FieldType& field_type) {
    return std::make_shared<LeafPredicateImpl>(IsNull::Instance(), field_index, field_name,
                                               field_type, std::vector<Literal>());
}

std::shared_ptr<Predicate> PredicateBuilder::IsNotNull(int32_t field_index,
                                                       const std::string& field_name,
                                                       const FieldType& field_type) {
    return std::make_shared<LeafPredicateImpl>(IsNotNull::Instance(), field_index, field_name,
                                               field_type, std::vector<Literal>());
}

std::shared_ptr<Predicate> PredicateBuilder::In(int32_t field_index, const std::string& field_name,
                                                const FieldType& field_type,
                                                const std::vector<Literal>& literals) {
    return std::make_shared<LeafPredicateImpl>(In::Instance(), field_index, field_name, field_type,
                                               literals);
}

std::shared_ptr<Predicate> PredicateBuilder::NotIn(int32_t field_index,
                                                   const std::string& field_name,
                                                   const FieldType& field_type,
                                                   const std::vector<Literal>& literals) {
    return std::make_shared<LeafPredicateImpl>(NotIn::Instance(), field_index, field_name,
                                               field_type, literals);
}

std::shared_ptr<Predicate> PredicateBuilder::Between(int32_t field_index,
                                                     const std::string& field_name,
                                                     const FieldType& field_type,
                                                     const Literal& included_lower_bound,
                                                     const Literal& included_upper_bound) {
    std::vector<std::shared_ptr<Predicate>> predicates;
    predicates.reserve(2);
    predicates.push_back(GreaterOrEqual(field_index, field_name, field_type, included_lower_bound));
    predicates.push_back(LessOrEqual(field_index, field_name, field_type, included_upper_bound));
    return std::make_shared<CompoundPredicateImpl>(And::Instance(), predicates);
}

Result<std::shared_ptr<Predicate>> PredicateBuilder::And(
    const std::vector<std::shared_ptr<Predicate>>& predicates) {
    if (predicates.empty()) {
        return Status::Invalid(
            "There must be at least 1 inner predicate to construct an AND predicate");
    }
    if (predicates.size() == 1) {
        return predicates[0];
    }
    return std::make_shared<CompoundPredicateImpl>(And::Instance(), predicates);
}

Result<std::shared_ptr<Predicate>> PredicateBuilder::Or(
    const std::vector<std::shared_ptr<Predicate>>& predicates) {
    if (predicates.empty()) {
        return Status::Invalid(
            "There must be at least 1 inner predicate to construct an OR predicate");
    }
    if (predicates.size() == 1) {
        return predicates[0];
    }
    return std::make_shared<CompoundPredicateImpl>(Or::Instance(), predicates);
}

Result<std::shared_ptr<Predicate>> PredicateBuilder::Not(
    const std::shared_ptr<Predicate>& predicate) {
    if (!predicate) {
        return Status::Invalid("There must not be nullptr to construct a NOT predicate");
    }
    auto negate_predicate = predicate->Negate();
    if (!negate_predicate) {
        return Status::Invalid("Could not construct A NOT predicate from " + predicate->ToString());
    }
    return negate_predicate;
}

Result<std::shared_ptr<Predicate>> PredicateBuilder::StartsWith(int32_t field_index,
                                                                const std::string& field_name,
                                                                const FieldType& field_type,
                                                                const Literal& literal) {
    if (field_type != FieldType::STRING || literal.GetType() != FieldType::STRING) {
        return Status::Invalid(
            "There must be STRING type field and literal to construct a StartsWith predicate");
    }
    return std::make_shared<LeafPredicateImpl>(StartsWith::Instance(), field_index, field_name,
                                               field_type, std::vector<Literal>({literal}));
}

Result<std::shared_ptr<Predicate>> PredicateBuilder::EndsWith(int32_t field_index,
                                                              const std::string& field_name,
                                                              const FieldType& field_type,
                                                              const Literal& literal) {
    if (field_type != FieldType::STRING || literal.GetType() != FieldType::STRING) {
        return Status::Invalid(
            "There must be STRING type field and literal to construct an EndsWith predicate");
    }
    return std::make_shared<LeafPredicateImpl>(EndsWith::Instance(), field_index, field_name,
                                               field_type, std::vector<Literal>({literal}));
}

Result<std::shared_ptr<Predicate>> PredicateBuilder::Contains(int32_t field_index,
                                                              const std::string& field_name,
                                                              const FieldType& field_type,
                                                              const Literal& literal) {
    if (field_type != FieldType::STRING || literal.GetType() != FieldType::STRING) {
        return Status::Invalid(
            "There must be STRING type field and literal to construct a Contains predicate");
    }
    return std::make_shared<LeafPredicateImpl>(Contains::Instance(), field_index, field_name,
                                               field_type, std::vector<Literal>({literal}));
}

Result<std::shared_ptr<Predicate>> PredicateBuilder::Like(int32_t field_index,
                                                          const std::string& field_name,
                                                          const FieldType& field_type,
                                                          const Literal& literal) {
    if (field_type != FieldType::STRING || literal.GetType() != FieldType::STRING) {
        return Status::Invalid(
            "There must be STRING type field and literal to construct a Like predicate");
    }
    return std::make_shared<LeafPredicateImpl>(Like::Instance(), field_index, field_name,
                                               field_type, std::vector<Literal>({literal}));
}
}  // namespace paimon
