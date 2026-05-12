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
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitVectorSearch(vector_search));
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
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitFullTextSearch(full_text_search));
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
