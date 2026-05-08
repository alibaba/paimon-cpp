/*
 * Copyright 2026-present Alibaba Inc.
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

#include "paimon/core/global_index/global_index_evaluator_impl.h"

#include "fmt/format.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate_utils.h"

namespace paimon {
Result<std::shared_ptr<GlobalIndexResult>> GlobalIndexEvaluatorImpl::Evaluate(
    const std::shared_ptr<Predicate>& predicate) {
    std::shared_ptr<GlobalIndexResult> compound_result;
    if (predicate) {
        PAIMON_ASSIGN_OR_RAISE(compound_result, EvaluatePredicate(predicate));
    }
    return compound_result;
}

Result<std::vector<std::shared_ptr<GlobalIndexReader>>> GlobalIndexEvaluatorImpl::GetIndexReaders(
    const std::string& field_name) {
    PAIMON_ASSIGN_OR_RAISE(DataField data_field, table_schema_->GetField(field_name));
    int32_t field_id = data_field.Id();
    // get or create global index readers for current field
    std::vector<std::shared_ptr<GlobalIndexReader>> readers;
    auto iter = index_readers_cache_.find(field_id);
    if (iter != index_readers_cache_.end()) {
        readers = iter->second;
    } else {
        PAIMON_ASSIGN_OR_RAISE(readers, create_index_readers_(field_id));
        index_readers_cache_.insert({field_id, readers});
    }
    return readers;
}

Result<std::shared_ptr<GlobalIndexResult>> GlobalIndexEvaluatorImpl::EvaluatePredicate(
    const std::shared_ptr<Predicate>& predicate) {
    if (predicate == nullptr) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }

    if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        return EvaluateCompoundPredicate(compound_predicate);
    } else if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        const std::string& field_name = leaf_predicate->FieldName();
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<GlobalIndexReader>> readers,
                               GetIndexReaders(field_name));
        if (readers.empty()) {
            // No usable index for this field within the requested range. Treat as "no
            // pushdown available" so the upstream falls back to a full scan instead of
            // wrongly producing an empty result.
            return std::shared_ptr<GlobalIndexResult>(nullptr);
        }
        // calculate compound result as field may has multiple indexes
        std::shared_ptr<GlobalIndexResult> compound_result;
        for (const auto& index_reader : readers) {
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<GlobalIndexResult> sub_result,
                PredicateUtils::VisitPredicate<std::shared_ptr<GlobalIndexResult>>(leaf_predicate,
                                                                                   index_reader));
            if (sub_result) {
                if (!compound_result) {
                    compound_result = sub_result;
                } else {
                    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> and_result,
                                           compound_result->And(sub_result));
                    compound_result = and_result;
                }
            }
            if (compound_result) {
                PAIMON_ASSIGN_OR_RAISE(bool is_empty, compound_result->IsEmpty());
                if (is_empty) {
                    return compound_result;
                }
            }
        }
        return compound_result;
    }
    return Status::Invalid(fmt::format(
        "cannot cast predicate {} to CompoundPredicate or LeafPredicate", predicate->ToString()));
}

Result<std::shared_ptr<GlobalIndexResult>> GlobalIndexEvaluatorImpl::EvaluateCompoundPredicate(
    const std::shared_ptr<CompoundPredicate>& compound_predicate) {
    if (compound_predicate->GetFunction().GetType() == Function::Type::OR) {
        std::shared_ptr<GlobalIndexResult> compound_result;
        for (const auto& child : compound_predicate->Children()) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> sub_result,
                                   EvaluatePredicate(child));
            if (!sub_result) {
                return std::shared_ptr<GlobalIndexResult>(nullptr);
            }
            if (!compound_result) {
                compound_result = sub_result;
            } else {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> or_result,
                                       compound_result->Or(sub_result));
                compound_result = or_result;
            }
        }
        return compound_result;
    } else if (compound_predicate->GetFunction().GetType() == Function::Type::AND) {
        std::shared_ptr<GlobalIndexResult> compound_result;
        for (const auto& child : compound_predicate->Children()) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> sub_result,
                                   EvaluatePredicate(child));
            if (sub_result) {
                if (!compound_result) {
                    compound_result = sub_result;
                } else {
                    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> and_result,
                                           compound_result->And(sub_result));
                    compound_result = and_result;
                }
            }

            if (compound_result) {
                PAIMON_ASSIGN_OR_RAISE(bool is_empty, compound_result->IsEmpty());
                if (is_empty) {
                    return compound_result;
                }
            }
        }
        return compound_result;
    }
    return Status::Invalid("CompoundPredicate only support And/Or function");
}

}  // namespace paimon
