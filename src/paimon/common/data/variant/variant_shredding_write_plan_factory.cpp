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
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/infer_variant_shredding_schema.h"
#include "paimon/common/data/variant/variant_shredding_batch_converter.h"
#include "paimon/common/data/variant/variant_shredding_write_plan.h"
#include "paimon/common/data/variant/variant_type_utils.h"

namespace paimon {

namespace {

/// Collects the field-index paths of the shreddable variant fields: at the top level or nested
/// inside structs only, mirroring the Java `InferVariantShreddingSchema.getPathsToVariant`.
void CollectVariantPaths(const arrow::FieldVector& fields, std::vector<int32_t>* current,
                         std::vector<std::vector<int32_t>>* paths) {
    for (int32_t i = 0; i < static_cast<int32_t>(fields.size()); ++i) {
        const std::shared_ptr<arrow::Field>& field = fields[i];
        current->push_back(i);
        if (VariantTypeUtils::IsVariantField(field)) {
            paths->push_back(*current);
        } else if (field->type()->id() == arrow::Type::STRUCT) {
            CollectVariantPaths(field->type()->fields(), current, paths);
        }
        current->pop_back();
    }
}

std::vector<std::vector<int32_t>> GetPathsToVariant(const arrow::Schema& schema) {
    std::vector<std::vector<int32_t>> paths;
    std::vector<int32_t> current;
    CollectVariantPaths(schema.fields(), &current, &paths);
    return paths;
}

/// Collects the non-null variant values of one sample batch at the given field-index path,
/// descending through struct arrays. Rows that are null at any level contribute no sample.
Result<std::vector<std::shared_ptr<GenericVariant>>> CollectSamplesAtPath(
    const std::vector<std::shared_ptr<arrow::Array>>& sample_batches,
    const std::vector<int32_t>& path, const std::shared_ptr<MemoryPool>& pool) {
    std::vector<std::shared_ptr<GenericVariant>> samples;
    for (const auto& sample_batch : sample_batches) {
        std::shared_ptr<arrow::Array> column = sample_batch;
        // The structs enclosing the variant column (the batch root excluded): child slot
        // contents under a null ancestor are unspecified in Arrow and must not be decoded.
        std::vector<std::shared_ptr<arrow::Array>> ancestors;
        for (int32_t index : path) {
            if (column == nullptr || column->type_id() != arrow::Type::STRUCT) {
                return Status::Invalid("sample batch does not match the variant column path");
            }
            if (column != sample_batch) {
                ancestors.push_back(column);
            }
            column = arrow::internal::checked_cast<const arrow::StructArray&>(*column).field(index);
        }
        if (column == nullptr) {
            return Status::Invalid("sample batch misses the planned variant column");
        }
        const auto& variant_array =
            arrow::internal::checked_cast<const arrow::StructArray&>(*column);
        const auto& value_array =
            arrow::internal::checked_cast<const arrow::BinaryArray&>(*variant_array.field(0));
        const auto& metadata_array =
            arrow::internal::checked_cast<const arrow::BinaryArray&>(*variant_array.field(1));
        auto row_is_null = [&](int64_t row) {
            for (const auto& ancestor : ancestors) {
                if (ancestor->IsNull(row)) {
                    return true;
                }
            }
            return variant_array.IsNull(row);
        };
        for (int64_t row = 0; row < variant_array.length(); ++row) {
            if (row_is_null(row)) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<GenericVariant> variant,
                GenericVariant::Create(std::string_view(value_array.GetView(row)),
                                       std::string_view(metadata_array.GetView(row)), pool));
            samples.push_back(std::move(variant));
        }
    }
    return samples;
}

}  // namespace

VariantShreddingWritePlanFactory::VariantShreddingWritePlanFactory(
    std::optional<std::string> configured_schema, bool infer_enabled, int32_t max_schema_width,
    int32_t max_schema_depth, double min_field_cardinality_ratio, int32_t max_infer_buffer_row,
    const std::shared_ptr<arrow::Schema>& write_schema, const std::shared_ptr<MemoryPool>& pool)
    : write_schema_(write_schema),
      pool_(pool),
      configured_schema_(std::move(configured_schema)),
      infer_enabled_(infer_enabled),
      max_schema_width_(max_schema_width),
      max_schema_depth_(max_schema_depth),
      min_field_cardinality_ratio_(min_field_cardinality_ratio),
      max_infer_buffer_row_(max_infer_buffer_row) {}

std::shared_ptr<VariantShreddingWritePlanFactory> VariantShreddingWritePlanFactory::Create(
    const CoreOptions& options, const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<MemoryPool>& pool) {
    return std::shared_ptr<VariantShreddingWritePlanFactory>(new VariantShreddingWritePlanFactory(
        options.GetVariantShreddingSchema(), options.VariantInferShreddingSchemaEnabled(),
        options.GetVariantShreddingMaxSchemaWidth(), options.GetVariantShreddingMaxSchemaDepth(),
        options.GetVariantShreddingMinFieldCardinalityRatio(),
        options.GetVariantShreddingMaxInferBufferRow(), write_schema, pool));
}

bool VariantShreddingWritePlanFactory::ShouldCreateWritePlan() const {
    return ContainsShreddableVariantField() && (HasConfiguredShreddingSchema() || infer_enabled_);
}

bool VariantShreddingWritePlanFactory::ShouldInferWritePlan() const {
    return ContainsShreddableVariantField() && !HasConfiguredShreddingSchema() && infer_enabled_;
}

int32_t VariantShreddingWritePlanFactory::InferBufferRowCount() const {
    return max_infer_buffer_row_;
}

bool VariantShreddingWritePlanFactory::HasConfiguredShreddingSchema() const {
    return configured_schema_.has_value();
}

bool VariantShreddingWritePlanFactory::ContainsShreddableVariantField() const {
    return !GetPathsToVariant(*write_schema_).empty();
}

Result<std::shared_ptr<ShreddingBatchConverter>> VariantShreddingWritePlanFactory::CreateConverter(
    const std::string& file_format_identifier,
    const std::vector<std::shared_ptr<arrow::Array>>& sample_batches) const {
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
        // One budget is shared across all variant columns so that the total inferred width stays
        // within `variant.shredding.maxSchemaWidth` (as in Java).
        InferVariantShreddingSchema::MaxFields max_fields = inferrer.CreateMaxFieldsBudget();
        std::map<std::vector<int32_t>, std::shared_ptr<arrow::DataType>> path_shredding_types;
        for (const std::vector<int32_t>& path : GetPathsToVariant(*write_schema_)) {
            PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<GenericVariant>> samples,
                                   CollectSamplesAtPath(sample_batches, path, pool_));
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> shredding_type,
                                   inferrer.InferColumnShreddingType(samples, &max_fields));
            if (shredding_type != nullptr) {
                path_shredding_types.emplace(path, std::move(shredding_type));
            }
        }
        if (path_shredding_types.empty()) {
            // No useful shredding schema was found; write the file unshredded.
            return std::shared_ptr<ShreddingBatchConverter>(nullptr);
        }
        PAIMON_ASSIGN_OR_RAISE(
            plan, VariantShreddingWritePlan::CreateFromPaths(write_schema_, path_shredding_types));
    }
    if (plan == nullptr) {
        // The configured schema names no variant column; write the file unshredded.
        return std::shared_ptr<ShreddingBatchConverter>(nullptr);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<VariantShreddingBatchConverter> converter,
                           VariantShreddingBatchConverter::Create(plan, pool_));
    return std::shared_ptr<ShreddingBatchConverter>(std::move(converter));
}

}  // namespace paimon
