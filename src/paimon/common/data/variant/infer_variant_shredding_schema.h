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

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class DataType;
class Field;
class Schema;
}  // namespace arrow

namespace paimon {

class MemoryPool;
class VariantShreddingInferenceSession;

/// Infers the complete physical write schema from sampled logical rows (mirroring the Java
/// `InferVariantShreddingSchema`). Rare fields (below the cardinality ratio) stay in the
/// un-shredded variant binary, integer types widen to int64, and the total number of shredded
/// fields is limited across all Variant columns.
class InferVariantShreddingSchema {
 public:
    using Path = std::vector<int32_t>;
    using SampleBatches = std::vector<std::shared_ptr<arrow::Array>>;

    struct SimpleSchema;

    struct ColumnEvidence {
        double root_value_count = 0;
        std::shared_ptr<SimpleSchema> observed_schema;
    };

    struct AdaptiveColumnResult {
        ColumnEvidence evidence;
        /// arrow::null() means the column remains unshredded.
        std::shared_ptr<arrow::DataType> selected_schema;
    };

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

    InferVariantShreddingSchema(const std::shared_ptr<arrow::Schema>& logical_schema,
                                const std::shared_ptr<MemoryPool>& pool, int32_t max_schema_width,
                                int32_t max_schema_depth, double min_field_cardinality_ratio);

    /// Infers one complete physical schema from the sampled logical row batches.
    Result<std::shared_ptr<arrow::Schema>> InferSchema(const SampleBatches& samples) const;

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

    /// Initial/adaptive inference primitives used by a rolling-writer-scoped session.
    Result<AdaptiveColumnResult> InferInitialColumn(
        const std::vector<std::shared_ptr<GenericVariant>>& samples, int32_t effective_sample_size,
        MaxFields* max_fields) const;

    Result<AdaptiveColumnResult> InferAdaptiveColumn(
        const ColumnEvidence& previous_evidence,
        const std::shared_ptr<arrow::DataType>& previous_selected,
        const std::vector<std::shared_ptr<GenericVariant>>& samples, int32_t effective_sample_size,
        double admission_ratio, double retention_ratio, MaxFields* max_fields) const;

 private:
    friend class VariantShreddingInferenceSession;

    struct InferenceEvidence {
        std::map<Path, ColumnEvidence> columns;
    };

    using SelectedSchemas = std::map<Path, std::shared_ptr<arrow::DataType>>;

    struct AdaptiveInferenceResult {
        std::shared_ptr<arrow::Schema> physical_schema;
        InferenceEvidence evidence;
        SelectedSchemas selected_schemas;
    };

    Result<AdaptiveInferenceResult> InferInitial(const SampleBatches& samples,
                                                 int32_t effective_sample_size) const;

    Result<AdaptiveInferenceResult> InferAdaptive(const InferenceEvidence& previous_evidence,
                                                  const SelectedSchemas& previous_selected_schemas,
                                                  const SampleBatches& samples,
                                                  int32_t effective_sample_size,
                                                  double admission_ratio,
                                                  double retention_ratio) const;

    static void CollectVariantPaths(const std::vector<std::shared_ptr<arrow::Field>>& fields,
                                    Path* current, std::vector<Path>* paths);

    Result<std::vector<std::shared_ptr<GenericVariant>>> CollectSamplesAtPath(
        const SampleBatches& sample_batches, const Path& path) const;

    Result<std::shared_ptr<arrow::Schema>> CreatePhysicalSchema(
        const SelectedSchemas& selected_schemas) const;

    Result<ColumnEvidence> AnalyzeColumn(
        const std::vector<std::shared_ptr<GenericVariant>>& samples) const;

    std::shared_ptr<arrow::Schema> logical_schema_;
    std::shared_ptr<MemoryPool> pool_;
    std::vector<Path> paths_to_variant_;
    int32_t max_schema_width_;
    int32_t max_schema_depth_;
    double min_field_cardinality_ratio_;
};

}  // namespace paimon
