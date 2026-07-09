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

#include <map>
#include <utility>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/infer_variant_shredding_schema.h"
#include "paimon/common/data/variant/variant_shredding_batch_converter.h"
#include "paimon/common/data/variant/variant_shredding_write_plan.h"
#include "paimon/common/data/variant/variant_type_utils.h"

namespace paimon {

VariantShreddingWritePlanFactory::VariantShreddingWritePlanFactory(
    const CoreOptions& options, const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<MemoryPool>& pool)
    : write_schema_(write_schema), pool_(pool) {
    configured_schema_ = options.GetVariantShreddingSchema();
    auto resolve = [this](auto&& result, auto* target) {
        if (result.ok()) {
            *target = result.value();
        } else if (config_status_.ok()) {
            config_status_ = result.status();
        }
    };
    resolve(options.VariantInferShreddingSchemaEnabled(), &infer_enabled_);
    resolve(options.GetVariantShreddingMaxSchemaWidth(), &max_schema_width_);
    resolve(options.GetVariantShreddingMaxSchemaDepth(), &max_schema_depth_);
    resolve(options.GetVariantShreddingMinFieldCardinalityRatio(), &min_field_cardinality_ratio_);
    resolve(options.GetVariantShreddingMaxInferBufferRow(), &max_infer_buffer_row_);
}

bool VariantShreddingWritePlanFactory::ShouldCreateWritePlan() const {
    return ContainsVariantField() &&
           (HasConfiguredShreddingSchema() || infer_enabled_ || !config_status_.ok());
}

bool VariantShreddingWritePlanFactory::ShouldInferWritePlan() const {
    return ContainsVariantField() && !HasConfiguredShreddingSchema() && infer_enabled_ &&
           config_status_.ok();
}

int32_t VariantShreddingWritePlanFactory::InferBufferRowCount() const {
    return max_infer_buffer_row_;
}

bool VariantShreddingWritePlanFactory::HasConfiguredShreddingSchema() const {
    return configured_schema_.has_value();
}

bool VariantShreddingWritePlanFactory::ContainsVariantField() const {
    for (const auto& field : write_schema_->fields()) {
        if (VariantTypeUtils::IsVariantField(field)) {
            return true;
        }
    }
    return false;
}

Result<std::shared_ptr<ShreddingBatchConverter>> VariantShreddingWritePlanFactory::CreateConverter(
    const std::string& file_format_identifier,
    const std::vector<std::shared_ptr<arrow::Array>>& sample_batches) const {
    PAIMON_RETURN_NOT_OK(config_status_);
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
        InferVariantShreddingSchema inferrer(max_schema_width_, max_schema_depth_,
                                             min_field_cardinality_ratio_);
        std::map<std::string, std::shared_ptr<arrow::DataType>> column_shredding_types;
        for (int i = 0; i < write_schema_->num_fields(); ++i) {
            const std::shared_ptr<arrow::Field>& field = write_schema_->field(i);
            if (!VariantTypeUtils::IsVariantField(field)) {
                continue;
            }
            std::vector<std::shared_ptr<GenericVariant>> samples;
            for (const auto& sample_batch : sample_batches) {
                const auto& struct_array = static_cast<const arrow::StructArray&>(*sample_batch);
                std::shared_ptr<arrow::Array> column = struct_array.field(i);
                if (column == nullptr) {
                    return Status::Invalid(
                        fmt::format("sample batch misses the variant column '{}'", field->name()));
                }
                const auto& variant_array = static_cast<const arrow::StructArray&>(*column);
                const auto& value_array =
                    static_cast<const arrow::BinaryArray&>(*variant_array.field(0));
                const auto& metadata_array =
                    static_cast<const arrow::BinaryArray&>(*variant_array.field(1));
                for (int64_t row = 0; row < variant_array.length(); ++row) {
                    if (variant_array.IsNull(row)) {
                        continue;
                    }
                    PAIMON_ASSIGN_OR_RAISE(
                        std::shared_ptr<GenericVariant> variant,
                        GenericVariant::Create(std::string_view(value_array.GetView(row)),
                                               std::string_view(metadata_array.GetView(row)),
                                               pool_));
                    samples.push_back(std::move(variant));
                }
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> shredding_type,
                                   inferrer.InferColumnShreddingType(samples));
            if (shredding_type != nullptr) {
                column_shredding_types.emplace(field->name(), std::move(shredding_type));
            }
        }
        if (column_shredding_types.empty()) {
            // No useful shredding schema was found; write the file unshredded.
            return std::shared_ptr<ShreddingBatchConverter>(nullptr);
        }
        PAIMON_ASSIGN_OR_RAISE(
            plan, VariantShreddingWritePlan::Create(write_schema_, column_shredding_types));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<VariantShreddingBatchConverter> converter,
                           VariantShreddingBatchConverter::Create(plan, pool_));
    return std::shared_ptr<ShreddingBatchConverter>(std::move(converter));
}

}  // namespace paimon
