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

#include "paimon/common/global_index/offset_global_index_reader.h"

#include <utility>

namespace paimon {

OffsetGlobalIndexReader::OffsetGlobalIndexReader(std::shared_ptr<GlobalIndexReader>&& wrapped,
                                                 int64_t offset)
    : wrapped_(std::move(wrapped)), offset_(offset) {}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitIsNotNull() {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result, wrapped_->VisitIsNotNull());
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitIsNull() {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result, wrapped_->VisitIsNull());
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitEqual(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitNotEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitNotEqual(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitLessThan(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitLessThan(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitLessOrEqual(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitGreaterThan(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitGreaterThan(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitGreaterOrEqual(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result, wrapped_->VisitIn(literals));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitNotIn(literals));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitStartsWith(
    const Literal& prefix) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitStartsWith(prefix));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitEndsWith(
    const Literal& suffix) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitEndsWith(suffix));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitContains(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitContains(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitLike(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result, wrapped_->VisitLike(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> OffsetGlobalIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    std::shared_ptr<VectorSearch> rewritten_search = vector_search;
    if (vector_search && vector_search->pre_filter) {
        auto original_filter = vector_search->pre_filter;
        auto offset = offset_;
        rewritten_search =
            vector_search->ReplacePreFilter([original_filter, offset](int64_t local_id) -> bool {
                return original_filter(local_id + offset);
            });
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitVectorSearch(rewritten_search));
    if (result == nullptr) {
        return std::shared_ptr<ScoredGlobalIndexResult>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> offset_result,
                           result->AddOffset(offset_));
    auto scored_result = std::dynamic_pointer_cast<ScoredGlobalIndexResult>(offset_result);
    if (!scored_result) {
        return Status::Invalid(
            "AddOffset on ScoredGlobalIndexResult did not return ScoredGlobalIndexResult");
    }
    return scored_result;
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    // Rewrite pre_filter to convert global ids (used externally) to local ids (used by wrapped_).
    // The original bitmap contains global ids; subtract offset_ to get local ids for the
    // underlying reader.
    std::shared_ptr<FullTextSearch> rewritten_search = full_text_search;
    if (full_text_search && full_text_search->pre_filter.has_value()) {
        RoaringBitmap64 local_bitmap;
        const auto& global_bitmap = full_text_search->pre_filter.value();
        for (auto iter = global_bitmap.Begin(); iter != global_bitmap.End(); ++iter) {
            local_bitmap.Add(*iter - offset_);
        }
        rewritten_search = full_text_search->ReplacePreFilter(std::move(local_bitmap));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitFullTextSearch(rewritten_search));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::ApplyOffset(
    const std::shared_ptr<GlobalIndexResult>& result) {
    if (result == nullptr) {
        return result;
    }
    return result->AddOffset(offset_);
}

}  // namespace paimon
