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
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "fmt/format.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/function_visitor.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {

class PAIMON_EXPORT PredicateUtils {
 public:
    PredicateUtils() = delete;
    ~PredicateUtils() = delete;
    static Result<bool> ContainAnyField(const std::shared_ptr<Predicate>& predicate,
                                        const std::set<std::string>& field_names);

    static Result<std::shared_ptr<Predicate>> ExcludePredicateWithFields(
        const std::shared_ptr<Predicate>& predicates, const std::set<std::string>& field_names);

    static Result<std::vector<std::shared_ptr<Predicate>>> ExcludePredicateWithFields(
        const std::vector<std::shared_ptr<Predicate>>& predicates,
        const std::set<std::string>& field_names);

    static std::vector<std::shared_ptr<Predicate>> SplitAnd(
        const std::shared_ptr<Predicate>& predicate);

    // picked_field_name_to_idx: [field_name, idx in target schema]
    static Result<std::shared_ptr<Predicate>> CreatePickedFieldFilter(
        const std::shared_ptr<Predicate>& predicate,
        const std::map<std::string, int32_t>& picked_field_name_to_idx);

    static Status GetAllNames(const std::shared_ptr<Predicate>& predicate,
                              std::set<std::string>* field_names);

    template <typename T>
    static Result<T> VisitPredicate(const std::shared_ptr<LeafPredicate>& predicate,
                                    const std::shared_ptr<FunctionVisitor<T>>& visitor) {
        auto type = predicate->GetFunction().GetType();
        switch (type) {
            case Function::Type::IS_NULL:
                return visitor->VisitIsNull();
            case Function::Type::IS_NOT_NULL:
                return visitor->VisitIsNotNull();
            case Function::Type::EQUAL: {
                assert(predicate->Literals().size() == 1);
                return visitor->VisitEqual(predicate->Literals()[0]);
            }
            case Function::Type::NOT_EQUAL: {
                assert(predicate->Literals().size() == 1);
                return visitor->VisitNotEqual(predicate->Literals()[0]);
            }
            case Function::Type::GREATER_THAN: {
                assert(predicate->Literals().size() == 1);
                return visitor->VisitGreaterThan(predicate->Literals()[0]);
            }
            case Function::Type::GREATER_OR_EQUAL: {
                assert(predicate->Literals().size() == 1);
                return visitor->VisitGreaterOrEqual(predicate->Literals()[0]);
            }
            case Function::Type::LESS_THAN: {
                assert(predicate->Literals().size() == 1);
                return visitor->VisitLessThan(predicate->Literals()[0]);
            }
            case Function::Type::LESS_OR_EQUAL: {
                assert(predicate->Literals().size() == 1);
                return visitor->VisitLessOrEqual(predicate->Literals()[0]);
            }
            case Function::Type::IN:
                return visitor->VisitIn(predicate->Literals());
            case Function::Type::NOT_IN:
                return visitor->VisitNotIn(predicate->Literals());
            case Function::Type::STARTS_WITH: {
                assert(predicate->Literals().size() == 1);
                return visitor->VisitStartsWith(predicate->Literals()[0]);
            }
            case Function::Type::ENDS_WITH: {
                assert(predicate->Literals().size() == 1);
                return visitor->VisitEndsWith(predicate->Literals()[0]);
            }
            case Function::Type::CONTAINS: {
                assert(predicate->Literals().size() == 1);
                return visitor->VisitContains(predicate->Literals()[0]);
            }
            case Function::Type::LIKE: {
                assert(predicate->Literals().size() == 1);
                return visitor->VisitLike(predicate->Literals()[0]);
            }
            default:
                return Status::Invalid(fmt::format("invalid {} function in leaf predicate",
                                                   predicate->GetFunction().ToString()));
        }
    }

 private:
    static Result<std::optional<std::shared_ptr<Predicate>>> ReconstructPredicateWithPickedFields(
        const std::shared_ptr<Predicate>& predicate,
        const std::map<std::string, int32_t>& picked_field_name_to_idx);

    static void SplitCompound(const Function::Type& type,
                              const std::shared_ptr<Predicate>& predicate,
                              std::vector<std::shared_ptr<Predicate>>* result);
};

}  // namespace paimon
