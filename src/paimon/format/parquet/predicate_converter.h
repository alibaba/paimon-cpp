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
#include <string>
#include <vector>

#include "arrow/compute/expression.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {
class CompoundPredicate;
class Function;
class LeafPredicate;
class Literal;
class Predicate;
}  // namespace paimon

namespace paimon::parquet {

class PredicateConverter {
 public:
    PredicateConverter() = delete;
    ~PredicateConverter() = delete;

    // convert paimon predicate to arrow expression, if total node count of predicate exceed
    // predicate_node_count_limit, will return AlwaysTrue
    static Result<arrow::compute::Expression> Convert(const std::shared_ptr<Predicate>& predicate,
                                                      uint32_t predicate_node_count_limit);

    static arrow::compute::Expression AlwaysTrue();

 private:
    static Result<arrow::compute::Expression> InnerConvert(
        const std::shared_ptr<Predicate>& predicate);

    static void CollectNodeCount(const std::shared_ptr<Predicate>& predicate, uint32_t* node_count);

    static Result<arrow::compute::Expression> ConvertCompound(
        const std::shared_ptr<CompoundPredicate>& compound_predicate);

    static Status CheckLiteralNotEmpty(const std::vector<Literal>& literals,
                                       const Function& function, const std::string& field_name);

    static Result<arrow::compute::Expression> ConvertLeaf(
        const std::shared_ptr<LeafPredicate>& leaf_predicate);

    static Result<arrow::compute::Expression> ConvertFloatingPointLeaf(
        const std::shared_ptr<LeafPredicate>& leaf_predicate);

    static Result<arrow::compute::Expression> ConvertToArrowLiteral(const Literal& literal);
};

}  // namespace paimon::parquet
