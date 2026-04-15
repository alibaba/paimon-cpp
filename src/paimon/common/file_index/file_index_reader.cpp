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

#include "paimon/file_index/file_index_reader.h"

#include <utility>

#include "paimon/predicate/literal.h"

namespace paimon {
Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitIsNotNull() {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitIsNull() {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitStartsWith(const Literal& prefix) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitEndsWith(const Literal& suffix) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitContains(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitLike(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitLessThan(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitNotEqual(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitLessOrEqual(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitEqual(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitGreaterThan(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    std::shared_ptr<FileIndexResult> file_index_result;
    for (const Literal& key : literals) {
        if (!file_index_result) {
            PAIMON_ASSIGN_OR_RAISE(file_index_result, VisitEqual(key));
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> inner_result, VisitEqual(key));
            PAIMON_ASSIGN_OR_RAISE(file_index_result, file_index_result->Or(inner_result));
        }
    }
    return file_index_result;
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    std::shared_ptr<FileIndexResult> file_index_result;
    for (const Literal& key : literals) {
        if (!file_index_result) {
            PAIMON_ASSIGN_OR_RAISE(file_index_result, VisitNotEqual(key));
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> inner_result,
                                   VisitNotEqual(key));
            PAIMON_ASSIGN_OR_RAISE(file_index_result, file_index_result->And(inner_result));
        }
    }
    return file_index_result;
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitAnd(
    const std::vector<Result<std::shared_ptr<FileIndexResult>>>& children) {
    if (children.empty()) {
        return Status::Invalid("VisitAnd called with no children");
    }

    // Start with the first child
    PAIMON_RETURN_NOT_OK(children[0]);
    auto current = children[0].value();

    // AND with remaining children
    for (size_t i = 1; i < children.size(); ++i) {
        PAIMON_RETURN_NOT_OK(children[i]);
        auto child = children[i].value();
        PAIMON_ASSIGN_OR_RAISE(current, current->And(child));
    }

    return current;
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitOr(
    const std::vector<Result<std::shared_ptr<FileIndexResult>>>& children) {
    if (children.empty()) {
        return Status::Invalid("VisitOr called with no children");
    }

    // Start with the first child
    PAIMON_RETURN_NOT_OK(children[0]);
    auto current = children[0].value();

    // OR with remaining children
    for (size_t i = 1; i < children.size(); ++i) {
        PAIMON_RETURN_NOT_OK(children[i]);
        auto child = children[i].value();
        PAIMON_ASSIGN_OR_RAISE(current, current->Or(child));
    }

    return current;
}
}  // namespace paimon
