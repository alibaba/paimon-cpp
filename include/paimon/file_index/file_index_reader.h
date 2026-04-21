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
#include <vector>

#include "paimon/file_index/file_index_result.h"
#include "paimon/predicate/function_visitor.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
/// Evaluates filter predicates against a file-level index to determine file eligibility.
///
/// `FileIndexReader` implements the `FunctionVisitor` interface specialized to produce
/// `std::shared_ptr<FileIndexResult>` objects. It reads pre-built file-level index data
/// (e.g., bitmap, bsi or bloom filters) from index file and evaluates
/// whether a given data file may contain rows matching a specific predicate.
class PAIMON_EXPORT FileIndexReader : public FunctionVisitor<std::shared_ptr<FileIndexResult>> {
 public:
    Result<std::shared_ptr<FileIndexResult>> VisitIsNotNull() override;

    Result<std::shared_ptr<FileIndexResult>> VisitIsNull() override;

    Result<std::shared_ptr<FileIndexResult>> VisitEqual(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitNotEqual(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitLessThan(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitLessOrEqual(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitGreaterThan(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitGreaterOrEqual(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitIn(const std::vector<Literal>& literals) override;

    Result<std::shared_ptr<FileIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override;

    Result<std::shared_ptr<FileIndexResult>> VisitStartsWith(const Literal& prefix) override;

    Result<std::shared_ptr<FileIndexResult>> VisitEndsWith(const Literal& suffix) override;

    Result<std::shared_ptr<FileIndexResult>> VisitContains(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitLike(const Literal& literal) override;
};

}  // namespace paimon
