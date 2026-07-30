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
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/data/shredding/shredding_write_plan_factory.h"
#include "paimon/common/data/variant/variant_shredding_inference_session.h"
#include "paimon/core/core_options.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class Schema;
}  // namespace arrow

namespace paimon {

/// Creates VARIANT shredding batch converters, either from the configured
/// `variant.shreddingSchema` or, when `variant.inferShreddingSchema` is enabled, by inferring a
/// shredding schema from sampled rows buffered per file. Variant columns nested inside ROW
/// (struct) columns are inferred and shredded too; variants inside arrays or maps are not (as in
/// Java).
class VariantShreddingWritePlanFactory : public ShreddingWritePlanFactory {
 public:
    /// Creates the factory. The `variant.*` option values are already parsed and validated by
    /// `CoreOptions::FromMap`, so creation cannot fail on configuration.
    static std::shared_ptr<VariantShreddingWritePlanFactory> Create(
        const CoreOptions& options, const std::shared_ptr<arrow::Schema>& write_schema,
        const std::shared_ptr<MemoryPool>& pool);

    bool ShouldCreateWritePlan() const override;

    bool ShouldInferWritePlan() const override;

    int32_t InferBufferRowCount() const override;

    Result<std::shared_ptr<ShreddingBatchConverter>> CreateConverter(
        const std::string& file_format_identifier,
        const std::vector<std::shared_ptr<arrow::Array>>& sample_batches) override;

    MetadataFinalizer CreateMetadataFinalizer(
        const std::shared_ptr<ShreddingBatchConverter>& converter,
        const std::string& compression) const override {
        // The shredded physical schema is self-describing; no per-file metadata is needed.
        return nullptr;
    }

    Status OnFileCompleted(const std::shared_ptr<ShreddingBatchConverter>& converter) override;

 private:
    VariantShreddingWritePlanFactory(
        std::optional<std::string> configured_schema, bool infer_enabled, int32_t max_schema_width,
        int32_t max_schema_depth, double min_field_cardinality_ratio, int32_t max_infer_buffer_row,
        VariantShreddingInferenceMode inference_mode, int32_t adaptive_max_infer_buffer_row,
        double adaptive_retention_ratio, const std::shared_ptr<arrow::Schema>& write_schema,
        const std::shared_ptr<MemoryPool>& pool);

    bool HasConfiguredShreddingSchema() const;
    /// Whether the write schema holds a shreddable variant field: at the top level or nested
    /// inside structs only (variants inside arrays or maps are never shredded).
    bool ContainsShreddableVariantField() const;

    std::shared_ptr<arrow::Schema> write_schema_;
    std::shared_ptr<MemoryPool> pool_;

    std::optional<std::string> configured_schema_;
    bool infer_enabled_ = false;
    int32_t max_schema_width_ = 0;
    int32_t max_schema_depth_ = 0;
    double min_field_cardinality_ratio_ = 0.0;
    int32_t max_infer_buffer_row_ = 0;
    int32_t adaptive_max_infer_buffer_row_ = 0;
    std::unique_ptr<VariantShreddingInferenceSession> adaptive_session_;
    bool has_pending_adaptive_inference_ = false;
    std::shared_ptr<ShreddingBatchConverter> pending_adaptive_converter_;
};

}  // namespace paimon
