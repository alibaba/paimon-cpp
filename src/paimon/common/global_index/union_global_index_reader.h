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

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "paimon/executor.h"
#include "paimon/global_index/global_index_reader.h"

namespace paimon {
/// A GlobalIndexReader that combines results from multiple readers by performing a union
/// operation on their results.
///
/// When an executor is provided, all sub-reader actions are submitted in parallel and results are
/// collected in submission order. Otherwise, sub-readers are evaluated sequentially.
class UnionGlobalIndexReader : public GlobalIndexReader {
 public:
    UnionGlobalIndexReader(std::vector<std::shared_ptr<GlobalIndexReader>>&& readers,
                           const std::shared_ptr<Executor>& executor);

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override;

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override;

    bool IsThreadSafe() const override {
        return false;
    }

    std::string GetIndexType() const override {
        return "union";
    }

 private:
    using ReaderAction = std::function<Result<std::shared_ptr<GlobalIndexResult>>(
        const std::shared_ptr<GlobalIndexReader>&)>;

    /// Executes the given action on all readers and merges results with Union.
    Result<std::shared_ptr<GlobalIndexResult>> Union(ReaderAction action);

    /// Executes the given action on all readers (parallel or sequential) and collects results.
    template <typename R>
    std::vector<R> ExecuteAllReaders(
        const std::function<R(const std::shared_ptr<GlobalIndexReader>&)>& action);

    std::vector<std::shared_ptr<GlobalIndexReader>> readers_;
    std::shared_ptr<Executor> executor_;
};

}  // namespace paimon
