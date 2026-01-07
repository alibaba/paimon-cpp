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

#include "paimon/core/global_index/global_index_evaluator_impl.h"

#include "fmt/format.h"
#include "paimon/common/predicate/predicate_utils.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/predicate/leaf_predicate.h"

namespace paimon {
Result<std::optional<std::shared_ptr<GlobalIndexResult>>> GlobalIndexEvaluatorImpl::Evaluate(
    const std::shared_ptr<Predicate>& predicate,
    const std::shared_ptr<VectorSearch>& vector_search) {
    std::optional<std::shared_ptr<GlobalIndexResult>> compound_result;
    if (predicate) {
        PAIMON_ASSIGN_OR_RAISE(compound_result, EvaluatePredicate(predicate));
    }
    if (vector_search) {
        PAIMON_ASSIGN_OR_RAISE(
            compound_result,
            EvaluateVectorSearch(vector_search, /*predicate_result=*/compound_result));
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

Result<std::optional<std::shared_ptr<GlobalIndexResult>>>
GlobalIndexEvaluatorImpl::EvaluateVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search,
    const std::optional<std::shared_ptr<GlobalIndexResult>>& predicate_result) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<GlobalIndexReader>> readers,
                           GetIndexReaders(vector_search->field_name));
    if (readers.empty()) {
        return predicate_result;
    }
    if (readers.size() > 1) {
        return Status::Invalid("Vector search cannot have multiple global indexes");
    }
    const auto& vector_search_reader = readers[0];
    if (predicate_result && vector_search->pre_filter != nullptr) {
        return Status::Invalid("Predicate result and pre_filter in VectorSearch conflict");
    }
    auto final_vector_search = vector_search;
    if (predicate_result) {
        auto bitmap_global_index_result =
            std::dynamic_pointer_cast<BitmapGlobalIndexResult>(predicate_result.value());
        if (!bitmap_global_index_result) {
            return Status::Invalid(
                "The pre_filter of vector search only supports BitmapGlobalIndexResult");
        }
        PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* bitmap,
                               bitmap_global_index_result->GetBitmap());
        assert(bitmap);
        final_vector_search = vector_search->ReplacePreFilter(
            [bitmap_global_index_result, bitmap](int64_t row_id) -> bool {
                return bitmap->Contains(row_id);
            });
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> vector_search_result,
                           vector_search_reader->VisitVectorSearch(final_vector_search));
    return std::optional<std::shared_ptr<GlobalIndexResult>>(vector_search_result);
}

Result<std::optional<std::shared_ptr<GlobalIndexResult>>>
GlobalIndexEvaluatorImpl::EvaluatePredicate(const std::shared_ptr<Predicate>& predicate) {
    if (predicate == nullptr) {
        return std::optional<std::shared_ptr<GlobalIndexResult>>();
    }

    if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        return EvaluateCompoundPredicate(compound_predicate);
    } else if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        const std::string& field_name = leaf_predicate->FieldName();
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<GlobalIndexReader>> readers,
                               GetIndexReaders(field_name));
        // calculate compound result as field may has multiple indexes
        std::optional<std::shared_ptr<GlobalIndexResult>> compound_result;
        for (const auto& index_reader : readers) {
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<GlobalIndexResult> sub_result,
                PredicateUtils::VisitPredicate<std::shared_ptr<GlobalIndexResult>>(leaf_predicate,
                                                                                   index_reader));
            if (!compound_result) {
                compound_result = sub_result;
            } else {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> and_result,
                                       compound_result.value()->And(sub_result));
                compound_result = and_result;
            }
            assert(compound_result);
            PAIMON_ASSIGN_OR_RAISE(bool is_empty, compound_result.value()->IsEmpty());
            if (is_empty) {
                return compound_result;
            }
        }
        return compound_result;
    }
    return Status::Invalid(fmt::format(
        "cannot cast predicate {} to CompoundPredicate or LeafPredicate", predicate->ToString()));
}

Result<std::optional<std::shared_ptr<GlobalIndexResult>>>
GlobalIndexEvaluatorImpl::EvaluateCompoundPredicate(
    const std::shared_ptr<CompoundPredicate>& compound_predicate) {
    if (compound_predicate->GetFunction().GetType() == Function::Type::OR) {
        std::optional<std::shared_ptr<GlobalIndexResult>> compound_result;
        for (const auto& child : compound_predicate->Children()) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<GlobalIndexResult>> sub_result,
                                   EvaluatePredicate(child));
            if (!sub_result) {
                return std::optional<std::shared_ptr<GlobalIndexResult>>();
            }
            if (!compound_result) {
                compound_result = sub_result;
            } else {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> or_result,
                                       compound_result.value()->Or(sub_result.value()));
                compound_result = or_result;
            }
        }
        return compound_result;
    } else if (compound_predicate->GetFunction().GetType() == Function::Type::AND) {
        std::optional<std::shared_ptr<GlobalIndexResult>> compound_result;
        for (const auto& child : compound_predicate->Children()) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<GlobalIndexResult>> sub_result,
                                   EvaluatePredicate(child));
            if (sub_result) {
                if (!compound_result) {
                    compound_result = sub_result;
                } else {
                    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> and_result,
                                           compound_result.value()->And(sub_result.value()));
                    compound_result = and_result;
                }
            }

            if (compound_result) {
                PAIMON_ASSIGN_OR_RAISE(bool is_empty, compound_result.value()->IsEmpty());
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
