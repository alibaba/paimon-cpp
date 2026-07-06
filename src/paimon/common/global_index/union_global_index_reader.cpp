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

#include "paimon/common/global_index/union_global_index_reader.h"

#include <future>
#include <utility>

#include "paimon/common/executor/future.h"

namespace paimon {
UnionGlobalIndexReader::UnionGlobalIndexReader(
    std::vector<std::shared_ptr<GlobalIndexReader>>&& readers,
    const std::shared_ptr<Executor>& executor)
    : readers_(std::move(readers)), executor_(executor) {}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitIsNotNull() {
    return Union(
        [](const std::shared_ptr<GlobalIndexReader>& reader) { return reader->VisitIsNotNull(); });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitIsNull() {
    return Union(
        [](const std::shared_ptr<GlobalIndexReader>& reader) { return reader->VisitIsNull(); });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitEqual(
    const Literal& literal) {
    return Union([&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitEqual(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitNotEqual(
    const Literal& literal) {
    return Union([&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitNotEqual(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitLessThan(
    const Literal& literal) {
    return Union([&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitLessThan(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    return Union([&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitLessOrEqual(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitGreaterThan(
    const Literal& literal) {
    return Union([&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitGreaterThan(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    return Union([&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitGreaterOrEqual(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    return Union([&literals](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitIn(literals);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    return Union([&literals](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitNotIn(literals);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitStartsWith(
    const Literal& prefix) {
    return Union([&prefix](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitStartsWith(prefix);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitEndsWith(
    const Literal& suffix) {
    return Union([&suffix](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitEndsWith(suffix);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitContains(
    const Literal& literal) {
    return Union([&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitContains(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitLike(
    const Literal& literal) {
    return Union([&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitLike(literal);
    });
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> UnionGlobalIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    auto results = ExecuteAllReaders<Result<std::shared_ptr<ScoredGlobalIndexResult>>>(
        [&vector_search](const std::shared_ptr<GlobalIndexReader>& reader)
            -> Result<std::shared_ptr<ScoredGlobalIndexResult>> {
            return reader->VisitVectorSearch(vector_search);
        });

    std::shared_ptr<ScoredGlobalIndexResult> merged_result = nullptr;
    for (auto& result_or_status : results) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredGlobalIndexResult> result,
                               std::move(result_or_status));
        if (result == nullptr) {
            continue;
        }
        if (merged_result == nullptr) {
            merged_result = std::move(result);
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> merged_as_base,
                                   merged_result->Or(result));
            merged_result = std::dynamic_pointer_cast<ScoredGlobalIndexResult>(merged_as_base);
            if (!merged_result) {
                return Status::Invalid(
                    "Or of ScoredGlobalIndexResult did not return ScoredGlobalIndexResult in "
                    "UnionGlobalIndexReader");
            }
        }
    }

    return merged_result;
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    return Union([&full_text_search](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitFullTextSearch(full_text_search);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::Union(ReaderAction action) {
    auto results = ExecuteAllReaders<Result<std::shared_ptr<GlobalIndexResult>>>(
        [&action](const std::shared_ptr<GlobalIndexReader>& reader)
            -> Result<std::shared_ptr<GlobalIndexResult>> { return action(reader); });

    std::shared_ptr<GlobalIndexResult> merged_result = nullptr;
    for (auto& result_or_status : results) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                               std::move(result_or_status));
        if (result == nullptr) {
            continue;
        }
        if (merged_result == nullptr) {
            merged_result = std::move(result);
        } else {
            PAIMON_ASSIGN_OR_RAISE(merged_result, merged_result->Or(result));
        }
    }

    return merged_result;
}

template <typename R>
std::vector<R> UnionGlobalIndexReader::ExecuteAllReaders(
    const std::function<R(const std::shared_ptr<GlobalIndexReader>&)>& action) {
    if (executor_ == nullptr || readers_.size() == 1) {
        std::vector<R> results;
        results.reserve(readers_.size());
        for (const auto& reader : readers_) {
            results.push_back(action(reader));
        }
        return results;
    }

    // Parallel: submit all tasks to executor, then collect results in submission order
    std::vector<std::future<R>> futures;
    futures.reserve(readers_.size());
    for (const auto& reader : readers_) {
        futures.push_back(
            Via(executor_.get(), [&action, &reader]() -> R { return action(reader); }));
    }
    return CollectAll(futures);
}

}  // namespace paimon
