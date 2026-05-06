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
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/core/global_index/global_index_evaluator.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/utils/row_range_index.h"

namespace paimon {
class GlobalIndexEvaluatorImpl : public GlobalIndexEvaluator {
 public:
    /// Creates the underlying readers for the given field, optionally restricted to the
    /// provided row range. Returns an empty vector when the field has no usable index.
    using IndexReadersCreator =
        std::function<Result<std::vector<std::shared_ptr<GlobalIndexReader>>>(
            int32_t, const std::optional<RowRangeIndex>&)>;

    GlobalIndexEvaluatorImpl(const std::shared_ptr<TableSchema>& table_schema,
                             IndexReadersCreator create_index_readers)
        : table_schema_(table_schema), create_index_readers_(std::move(create_index_readers)) {}

    Result<std::shared_ptr<GlobalIndexResult>> Evaluate(
        const std::shared_ptr<Predicate>& predicate,
        const std::optional<RowRangeIndex>& row_range_index) override;

 private:
    /// Per-evaluation cache keyed by field id. Reused across recursive calls within a single
    /// `Evaluate` invocation so that the same field is not loaded twice; a fresh cache is used
    /// for every public `Evaluate` call because `row_range_index` may change between calls.
    using ReadersCache = std::map<int32_t, std::vector<std::shared_ptr<GlobalIndexReader>>>;

    Result<std::shared_ptr<GlobalIndexResult>> EvaluatePredicate(
        const std::shared_ptr<Predicate>& predicate,
        const std::optional<RowRangeIndex>& row_range_index, ReadersCache& cache);

    Result<std::shared_ptr<GlobalIndexResult>> EvaluateCompoundPredicate(
        const std::shared_ptr<CompoundPredicate>& compound_predicate,
        const std::optional<RowRangeIndex>& row_range_index, ReadersCache& cache);

    Result<std::vector<std::shared_ptr<GlobalIndexReader>>> GetIndexReaders(
        const std::string& field_name, const std::optional<RowRangeIndex>& row_range_index,
        ReadersCache& cache);

 private:
    std::shared_ptr<TableSchema> table_schema_;
    // create_index_readers_(field_id, row_range_index)
    IndexReadersCreator create_index_readers_;
};

}  // namespace paimon
