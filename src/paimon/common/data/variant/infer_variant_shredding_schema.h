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

#include <memory>
#include <vector>

#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {

/// Infers a shredding type for a variant column from sampled values (mirroring the Java
/// `InferVariantShreddingSchema`). Rare fields (below the cardinality ratio) stay in the
/// un-shredded variant binary, integer types widen to int64, and the total number of shredded
/// fields is limited.
class InferVariantShreddingSchema {
 public:
    /// The mutable budget of shredded fields remaining. One instance is shared across all
    /// variant columns of a schema so that the total inferred width stays within
    /// `variant.shredding.maxSchemaWidth` (mirroring the Java `MaxFields`).
    struct MaxFields {
        int32_t remaining;
    };

    InferVariantShreddingSchema(int32_t max_schema_width, int32_t max_schema_depth,
                                double min_field_cardinality_ratio)
        : max_schema_width_(max_schema_width),
          max_schema_depth_(max_schema_depth),
          min_field_cardinality_ratio_(min_field_cardinality_ratio) {}

    /// Creates the shared shredded-field budget for one schema inference.
    MaxFields CreateMaxFieldsBudget() const {
        return MaxFields{max_schema_width_};
    }

    /// Infers the shredding type of one variant column from its sampled non-null values, e.g.
    /// `struct{a: int64, b: string}`. `arrow::null()` leaves denote untyped variant sub-values.
    /// `max_fields` is the budget shared across all columns of the schema. Returns nullptr when
    /// no useful shredding schema was found (the column should stay unshredded).
    Result<std::shared_ptr<arrow::DataType>> InferColumnShreddingType(
        const std::vector<std::shared_ptr<GenericVariant>>& samples, MaxFields* max_fields) const;

 private:
    int32_t max_schema_width_;
    int32_t max_schema_depth_;
    double min_field_cardinality_ratio_;
};

}  // namespace paimon
