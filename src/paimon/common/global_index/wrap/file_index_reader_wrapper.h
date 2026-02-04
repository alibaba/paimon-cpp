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
#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "paimon/file_index/file_index_reader.h"
#include "paimon/global_index/global_index_reader.h"

namespace paimon {
///  A `GlobalIndexReader` wrapper for `FileIndexReader`.
class FileIndexReaderWrapper : public GlobalIndexReader {
 public:
    FileIndexReaderWrapper(const std::shared_ptr<FileIndexReader>& reader,
                           const std::function<Result<std::shared_ptr<GlobalIndexResult>>(
                               const std::shared_ptr<FileIndexResult>&)>& transform)
        : reader_(reader), transform_(transform) {}

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitIsNotNull());
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitIsNull());
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitEqual(literal));
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitNotEqual(literal));
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitLessThan(literal));
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitLessOrEqual(literal));
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitGreaterThan(literal));
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(
        const Literal& literal) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitGreaterOrEqual(literal));
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitIn(literals));
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitNotIn(literals));
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitStartsWith(prefix));
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitEndsWith(suffix));
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitContains(literal));
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> file_index_result,
                               reader_->VisitLike(literal));
        return transform_(file_index_result);
    }

    Result<std::shared_ptr<VectorSearchGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override {
        return Status::Invalid(
            "FileIndexReaderWrapper is not supposed to handle vector search query");
    }

 private:
    std::shared_ptr<FileIndexReader> reader_;
    std::function<Result<std::shared_ptr<GlobalIndexResult>>(
        const std::shared_ptr<FileIndexResult>&)>
        transform_;
};

}  // namespace paimon
