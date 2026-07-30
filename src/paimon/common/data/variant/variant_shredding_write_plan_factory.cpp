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

#include "paimon/common/data/variant/variant_shredding_write_plan_factory.h"

#include <utility>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/infer_variant_shredding_schema.h"
#include "paimon/common/data/variant/variant_shredding_batch_converter.h"
#include "paimon/common/data/variant/variant_shredding_write_plan.h"
#include "paimon/common/data/variant/variant_type_utils.h"

namespace paimon {

namespace {

bool ContainsVariantFields(const arrow::FieldVector& fields) {
    for (const std::shared_ptr<arrow::Field>& field : fields) {
        if (VariantTypeUtils::IsVariantField(field)) {
            return true;
        }
        if (field->type()->id() == arrow::Type::STRUCT &&
            ContainsVariantFields(field->type()->fields())) {
            return true;
        }
    }
    return false;
}

}  // namespace

VariantShreddingWritePlanFactory::VariantShreddingWritePlanFactory(
    std::optional<std::string> configured_schema, bool infer_enabled, int32_t max_schema_width,
    int32_t max_schema_depth, double min_field_cardinality_ratio, int32_t max_infer_buffer_row,
    VariantShreddingInferenceMode inference_mode, int32_t adaptive_max_infer_buffer_row,
    double adaptive_retention_ratio, const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<MemoryPool>& pool)
    : write_schema_(write_schema),
      pool_(pool),
      configured_schema_(std::move(configured_schema)),
      infer_enabled_(infer_enabled),
      max_schema_width_(max_schema_width),
      max_schema_depth_(max_schema_depth),
      min_field_cardinality_ratio_(min_field_cardinality_ratio),
      max_infer_buffer_row_(max_infer_buffer_row),
      adaptive_max_infer_buffer_row_(adaptive_max_infer_buffer_row) {
    if (!configured_schema_.has_value() && infer_enabled_ &&
        inference_mode == VariantShreddingInferenceMode::ADAPTIVE) {
        adaptive_session_ = std::make_unique<VariantShreddingInferenceSession>(
            InferVariantShreddingSchema(write_schema_, pool_, max_schema_width_, max_schema_depth_,
                                        min_field_cardinality_ratio_),
            adaptive_max_infer_buffer_row_, min_field_cardinality_ratio_, adaptive_retention_ratio);
    }
}

std::shared_ptr<VariantShreddingWritePlanFactory> VariantShreddingWritePlanFactory::Create(
    const CoreOptions& options, const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<MemoryPool>& pool) {
    return std::shared_ptr<VariantShreddingWritePlanFactory>(new VariantShreddingWritePlanFactory(
        options.GetVariantShreddingSchema(), options.VariantInferShreddingSchemaEnabled(),
        options.GetVariantShreddingMaxSchemaWidth(), options.GetVariantShreddingMaxSchemaDepth(),
        options.GetVariantShreddingMinFieldCardinalityRatio(),
        options.GetVariantShreddingMaxInferBufferRow(), options.GetVariantShreddingInferenceMode(),
        options.GetVariantShreddingAdaptiveMaxInferBufferRow(),
        options.GetVariantShreddingAdaptiveRetentionRatio(), write_schema, pool));
}

bool VariantShreddingWritePlanFactory::ShouldCreateWritePlan() const {
    return ContainsShreddableVariantField() && (HasConfiguredShreddingSchema() || infer_enabled_);
}

bool VariantShreddingWritePlanFactory::ShouldInferWritePlan() const {
    return ContainsShreddableVariantField() && !HasConfiguredShreddingSchema() && infer_enabled_;
}

int32_t VariantShreddingWritePlanFactory::InferBufferRowCount() const {
    if (adaptive_session_ != nullptr && adaptive_session_->HasPrior()) {
        return adaptive_max_infer_buffer_row_;
    }
    return max_infer_buffer_row_;
}

bool VariantShreddingWritePlanFactory::HasConfiguredShreddingSchema() const {
    return configured_schema_.has_value();
}

bool VariantShreddingWritePlanFactory::ContainsShreddableVariantField() const {
    return ContainsVariantFields(write_schema_->fields());
}

Result<std::shared_ptr<ShreddingBatchConverter>> VariantShreddingWritePlanFactory::CreateConverter(
    const std::string& file_format_identifier,
    const std::vector<std::shared_ptr<arrow::Array>>& sample_batches) {
    if (file_format_identifier != "parquet") {
        return Status::NotImplemented(
            fmt::format("variant shredding is only supported by the parquet file format, got {}",
                        file_format_identifier));
    }

    std::shared_ptr<VariantShreddingWritePlan> plan;
    if (HasConfiguredShreddingSchema()) {
        PAIMON_ASSIGN_OR_RAISE(plan, VariantShreddingWritePlan::FromConfiguredSchema(
                                         write_schema_, configured_schema_.value()));
    } else {
        std::shared_ptr<arrow::Schema> physical_schema;
        if (adaptive_session_ != nullptr) {
            PAIMON_ASSIGN_OR_RAISE(physical_schema, adaptive_session_->InferSchema(sample_batches));
            has_pending_adaptive_inference_ = true;
        } else {
            InferVariantShreddingSchema inferrer(write_schema_, pool_, max_schema_width_,
                                                 max_schema_depth_, min_field_cardinality_ratio_);
            PAIMON_ASSIGN_OR_RAISE(physical_schema, inferrer.InferSchema(sample_batches));
        }
        PAIMON_ASSIGN_OR_RAISE(plan, VariantShreddingWritePlan::CreateFromPhysicalSchema(
                                         write_schema_, physical_schema));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<VariantShreddingBatchConverter> converter,
                           VariantShreddingBatchConverter::Create(plan, pool_));
    std::shared_ptr<ShreddingBatchConverter> result = std::move(converter);
    if (adaptive_session_ != nullptr) {
        pending_adaptive_converter_ = result;
    }
    return result;
}

Status VariantShreddingWritePlanFactory::OnFileCompleted(
    const std::shared_ptr<ShreddingBatchConverter>& converter) {
    if (adaptive_session_ == nullptr) {
        return Status::OK();
    }
    if (!has_pending_adaptive_inference_ || converter != pending_adaptive_converter_) {
        return Status::Invalid(
            "Completed Variant write plan does not match the pending adaptive inference.");
    }
    PAIMON_RETURN_NOT_OK(adaptive_session_->CommitPendingInference());
    has_pending_adaptive_inference_ = false;
    pending_adaptive_converter_.reset();
    return Status::OK();
}

}  // namespace paimon
