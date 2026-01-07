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

namespace paimon {
class GlobalIndexEvaluatorImpl : public GlobalIndexEvaluator {
 public:
    using IndexReadersCreator =
        std::function<Result<std::vector<std::shared_ptr<GlobalIndexReader>>>(int32_t)>;

    GlobalIndexEvaluatorImpl(const std::shared_ptr<TableSchema>& table_schema,
                             IndexReadersCreator create_index_readers)
        : table_schema_(table_schema), create_index_readers_(std::move(create_index_readers)) {}

    Result<std::optional<std::shared_ptr<GlobalIndexResult>>> Evaluate(
        const std::shared_ptr<Predicate>& predicate,
        const std::shared_ptr<VectorSearch>& vector_search) override;

 private:
    Result<std::optional<std::shared_ptr<GlobalIndexResult>>> EvaluateVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search,
        const std::optional<std::shared_ptr<GlobalIndexResult>>& predicate_result);

    Result<std::optional<std::shared_ptr<GlobalIndexResult>>> EvaluatePredicate(
        const std::shared_ptr<Predicate>& predicate);

    Result<std::optional<std::shared_ptr<GlobalIndexResult>>> EvaluateCompoundPredicate(
        const std::shared_ptr<CompoundPredicate>& compound_predicate);

    Result<std::vector<std::shared_ptr<GlobalIndexReader>>> GetIndexReaders(
        const std::string& field_name);

 private:
    std::shared_ptr<TableSchema> table_schema_;
    // create_index_readers_(field_id)
    IndexReadersCreator create_index_readers_;
    // [field_id, vector<reader>]
    std::map<int32_t, std::vector<std::shared_ptr<GlobalIndexReader>>> index_readers_cache_;
};

}  // namespace paimon
