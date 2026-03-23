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

#include "paimon/predicate/predicate_utils.h"

#include <utility>

#include "paimon/common/predicate/compound_predicate_impl.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate_builder.h"

namespace paimon {
Result<bool> PredicateUtils::ContainAnyField(const std::shared_ptr<Predicate>& predicate,
                                             const std::set<std::string>& field_names) {
    if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        return field_names.find(leaf_predicate->FieldName()) != field_names.end();
    } else if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        for (const auto& child : compound_predicate->Children()) {
            PAIMON_ASSIGN_OR_RAISE(bool contains, ContainAnyField(child, field_names));
            if (contains) {
                return true;
            }
        }
        return false;
    }
    return Status::Invalid("must be LeafPredicate or CompoundPredicate");
}

Status PredicateUtils::GetAllNames(const std::shared_ptr<Predicate>& predicate,
                                   std::set<std::string>* field_names) {
    if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        field_names->insert(leaf_predicate->FieldName());
        return Status::OK();
    } else if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        for (const auto& child : compound_predicate->Children()) {
            PAIMON_RETURN_NOT_OK(GetAllNames(child, field_names));
        }
        return Status::OK();
    }
    return Status::Invalid("must be LeafPredicate or CompoundPredicate");
}

Result<std::shared_ptr<Predicate>> PredicateUtils::ExcludePredicateWithFields(
    const std::shared_ptr<Predicate>& predicates, const std::set<std::string>& field_names) {
    auto sub_predicates = PredicateUtils::SplitAnd(predicates);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Predicate>> new_predicates,
                           PredicateUtils::ExcludePredicateWithFields(sub_predicates, field_names));
    if (new_predicates.empty()) {
        return std::shared_ptr<Predicate>();
    }
    return PredicateBuilder::And(new_predicates);
}

Result<std::vector<std::shared_ptr<Predicate>>> PredicateUtils::ExcludePredicateWithFields(
    const std::vector<std::shared_ptr<Predicate>>& predicates,
    const std::set<std::string>& field_names) {
    if (predicates.empty() || field_names.empty()) {
        return predicates;
    }
    std::vector<std::shared_ptr<Predicate>> remain_predicates;
    for (const auto& predicate : predicates) {
        PAIMON_ASSIGN_OR_RAISE(bool contain, ContainAnyField(predicate, field_names));
        if (!contain) {
            remain_predicates.push_back(predicate);
        }
    }
    return remain_predicates;
}

std::vector<std::shared_ptr<Predicate>> PredicateUtils::SplitAnd(
    const std::shared_ptr<Predicate>& predicate) {
    std::vector<std::shared_ptr<Predicate>> result;
    if (predicate == nullptr) {
        return result;
    }
    SplitCompound(Function::Type::AND, predicate, &result);
    return result;
}

Result<std::shared_ptr<Predicate>> PredicateUtils::CreatePickedFieldFilter(
    const std::shared_ptr<Predicate>& predicate,
    const std::map<std::string, int32_t>& picked_field_name_to_idx) {
    if (!predicate) {
        return predicate;
    }
    auto split_predicates = PredicateUtils::SplitAnd(predicate);
    std::vector<std::shared_ptr<Predicate>> converted_predicates;
    converted_predicates.reserve(split_predicates.size());
    for (const auto& predicate : split_predicates) {
        PAIMON_ASSIGN_OR_RAISE(
            std::optional<std::shared_ptr<Predicate>> converted,
            ReconstructPredicateWithPickedFields(predicate, picked_field_name_to_idx));
        if (converted != std::nullopt) {
            converted_predicates.push_back(converted.value());
        }
    }
    if (converted_predicates.empty()) {
        return std::shared_ptr<Predicate>();
    }
    return PredicateBuilder::And(converted_predicates);
}

Result<std::optional<std::shared_ptr<Predicate>>>
PredicateUtils::ReconstructPredicateWithPickedFields(
    const std::shared_ptr<Predicate>& predicate,
    const std::map<std::string, int32_t>& picked_field_name_to_idx) {
    if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicateImpl>(predicate)) {
        std::vector<std::shared_ptr<Predicate>> mapped_children;
        for (const auto& child : compound_predicate->Children()) {
            PAIMON_ASSIGN_OR_RAISE(
                std::optional<std::shared_ptr<Predicate>> mapped,
                ReconstructPredicateWithPickedFields(child, picked_field_name_to_idx));
            if (mapped != std::nullopt) {
                mapped_children.push_back(mapped.value());
            } else {
                return std::optional<std::shared_ptr<Predicate>>();
            }
        }
        return std::optional<std::shared_ptr<Predicate>>(
            compound_predicate->NewCompoundPredicate(mapped_children));
    } else if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicateImpl>(predicate)) {
        auto iter = picked_field_name_to_idx.find(leaf_predicate->FieldName());
        if (iter == picked_field_name_to_idx.end()) {
            return std::optional<std::shared_ptr<Predicate>>();
        } else {
            return std::optional<std::shared_ptr<Predicate>>(
                leaf_predicate->NewLeafPredicate(/*new_field_index=*/iter->second));
        }
    }
    return Status::Invalid(fmt::format(
        "cannot cast predicate {} to CompoundPredicate or LeafPredicate", predicate->ToString()));
}

void PredicateUtils::SplitCompound(const Function::Type& type,
                                   const std::shared_ptr<Predicate>& predicate,
                                   std::vector<std::shared_ptr<Predicate>>* result) {
    if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        if (compound_predicate->GetFunction().GetType() == type) {
            for (const auto& child : compound_predicate->Children()) {
                SplitCompound(type, child, result);
            }
            return;
        }
    }
    result->push_back(predicate);
}

}  // namespace paimon
