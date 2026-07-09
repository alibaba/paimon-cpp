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

#include "paimon/common/data/variant/variant_shredding_read_plan_factory.h"

#include <utility>
#include <vector>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_access_utils.h"
#include "paimon/common/data/variant/variant_binary_util.h"
#include "paimon/common/data/variant/variant_builder.h"
#include "paimon/common/data/variant/variant_get.h"
#include "paimon/common/data/variant/variant_reassembler.h"
#include "paimon/common/data/variant/variant_schema.h"
#include "paimon/common/data/variant/variant_shredding_utils.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

namespace {

/// Reassembles the full variant of a shredded file column back into
/// `struct<value, metadata>` (a plain VARIANT read).
class FullVariantColumnReadPlan : public ShreddingColumnReadPlan {
 public:
    FullVariantColumnReadPlan(std::shared_ptr<arrow::Field> logical_field,
                              std::shared_ptr<arrow::Field> physical_field,
                              std::shared_ptr<VariantSchema> schema)
        : logical_field_(std::move(logical_field)),
          physical_field_(std::move(physical_field)),
          schema_(std::move(schema)) {}

    const std::shared_ptr<arrow::Field>& LogicalField() const override {
        return logical_field_;
    }

    const std::shared_ptr<arrow::Field>& PhysicalField() const override {
        return physical_field_;
    }

    Result<std::shared_ptr<arrow::Array>> Assemble(const std::shared_ptr<arrow::Array>& physical,
                                                   arrow::MemoryPool* pool) const override {
        if (physical->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid(fmt::format("cannot cast shredded variant field {} to a struct",
                                               physical_field_->name()));
        }
        auto physical_struct = std::static_pointer_cast<arrow::StructArray>(physical);
        return VariantReassembler::AssembleVariantArray(physical_struct, schema_, pool);
    }

 private:
    std::shared_ptr<arrow::Field> logical_field_;
    std::shared_ptr<arrow::Field> physical_field_;
    std::shared_ptr<VariantSchema> schema_;
};

/// One access path segment resolved against the shredded schema of one file.
struct ResolvedSegment {
    VariantPathSegment raw;
    bool is_object = false;
    // The `typed_value` index at this level, or -1 when the path leaves the shredded schema
    // here and continues inside the `value` binary.
    int32_t typed_idx = -1;
    // The object field index inside the typed object, or the array element index.
    int32_t extraction_idx = -1;
};

struct ResolvedSpec {
    VariantAccessSpec spec;
    std::vector<ResolvedSegment> segments;
};

ResolvedSpec ResolveSpec(const VariantAccessSpec& spec, const VariantSchema* root) {
    ResolvedSpec resolved;
    resolved.spec = spec;
    const VariantSchema* schema = root;
    for (const auto& segment : spec.segments) {
        ResolvedSegment r;
        r.raw = segment;
        if (segment.kind == VariantPathSegment::Kind::kObjectExtraction) {
            r.is_object = true;
            if (schema != nullptr && !schema->object_schema.empty()) {
                auto it = schema->object_schema_map.find(segment.key);
                if (it != schema->object_schema_map.end()) {
                    r.typed_idx = schema->typed_idx;
                    r.extraction_idx = it->second;
                    schema = schema->object_schema[it->second].schema.get();
                } else {
                    schema = nullptr;
                }
            } else {
                schema = nullptr;
            }
        } else {
            if (schema != nullptr && schema->array_schema != nullptr) {
                r.typed_idx = schema->typed_idx;
                r.extraction_idx = segment.index;
                schema = schema->array_schema.get();
            } else {
                schema = nullptr;
            }
        }
        resolved.segments.push_back(std::move(r));
    }
    return resolved;
}

/// Extracts the paths described by a variant-access projection, reading typed sub-columns
/// directly and falling back to the `value` binary where the path is not shredded.
class VariantAccessColumnReadPlan : public ShreddingColumnReadPlan {
 public:
    VariantAccessColumnReadPlan(std::shared_ptr<arrow::Field> logical_field,
                                std::shared_ptr<arrow::Field> physical_field,
                                std::shared_ptr<VariantSchema> schema,
                                std::vector<ResolvedSpec> specs, std::shared_ptr<MemoryPool> pool)
        : logical_field_(std::move(logical_field)),
          physical_field_(std::move(physical_field)),
          schema_(std::move(schema)),
          specs_(std::move(specs)),
          pool_(std::move(pool)) {}

    const std::shared_ptr<arrow::Field>& LogicalField() const override {
        return logical_field_;
    }

    const std::shared_ptr<arrow::Field>& PhysicalField() const override {
        return physical_field_;
    }

    Result<std::shared_ptr<arrow::Array>> Assemble(const std::shared_ptr<arrow::Array>& physical,
                                                   arrow::MemoryPool* pool) const override {
        if (physical->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid(fmt::format("cannot cast shredded variant field {} to a struct",
                                               physical_field_->name()));
        }
        const auto& physical_struct = static_cast<const arrow::StructArray&>(*physical);
        std::unique_ptr<arrow::ArrayBuilder> builder;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::MakeBuilder(pool, logical_field_->type(), &builder));
        auto* struct_builder = static_cast<arrow::StructBuilder*>(builder.get());
        for (int64_t row = 0; row < physical_struct.length(); ++row) {
            if (physical_struct.IsNull(row)) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->AppendNull());
                continue;
            }
            if (physical_struct.field(schema_->top_level_metadata_idx)->IsNull(row)) {
                return VariantBinaryUtil::MalformedVariant();
            }
            std::string_view metadata = static_cast<const arrow::BinaryArray&>(
                                            *physical_struct.field(schema_->top_level_metadata_idx))
                                            .GetView(row);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->Append());
            for (size_t i = 0; i < specs_.size(); ++i) {
                PAIMON_RETURN_NOT_OK(
                    ExtractField(physical_struct, row, metadata, specs_[i],
                                 struct_builder->field_builder(static_cast<int>(i))));
            }
        }
        std::shared_ptr<arrow::Array> result;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Finish(&result));
        return result;
    }

 private:
    Status ExtractField(const arrow::StructArray& root, int64_t root_row, std::string_view metadata,
                        const ResolvedSpec& resolved, arrow::ArrayBuilder* builder) const {
        const arrow::StructArray* current = &root;
        int64_t row = root_row;
        const VariantSchema* schema = schema_.get();
        size_t segment_idx = 0;
        while (segment_idx < resolved.segments.size()) {
            const ResolvedSegment& segment = resolved.segments[segment_idx];
            if (segment.typed_idx < 0) {
                // The path leaves the shredded schema here; walk the remaining raw path inside
                // the `value` binary.
                return ExtractFromBinary(*current, row, metadata, resolved, segment_idx, schema,
                                         builder);
            }
            if (current->field(segment.typed_idx)->IsNull(row)) {
                return ToPaimonStatus(builder->AppendNull());
            }
            if (segment.is_object) {
                const auto& object_array =
                    static_cast<const arrow::StructArray&>(*current->field(segment.typed_idx));
                const auto& field_array = static_cast<const arrow::StructArray&>(
                    *object_array.field(segment.extraction_idx));
                if (field_array.IsNull(row)) {
                    // Shredded object fields must not be null.
                    return VariantBinaryUtil::MalformedVariant();
                }
                schema = schema->object_schema[segment.extraction_idx].schema.get();
                current = &field_array;
                // A field is missing when neither its typed_value nor its value is present.
                bool typed_present =
                    schema->typed_idx >= 0 && !current->field(schema->typed_idx)->IsNull(row);
                bool variant_present =
                    schema->variant_idx >= 0 && !current->field(schema->variant_idx)->IsNull(row);
                if (!typed_present && !variant_present) {
                    return ToPaimonStatus(builder->AppendNull());
                }
            } else {
                const auto& list_array =
                    static_cast<const arrow::ListArray&>(*current->field(segment.typed_idx));
                if (segment.extraction_idx >= list_array.value_length(row)) {
                    return ToPaimonStatus(builder->AppendNull());
                }
                int64_t element_row = list_array.value_offset(row) + segment.extraction_idx;
                const auto& element_array =
                    static_cast<const arrow::StructArray&>(*list_array.values());
                if (element_array.IsNull(element_row)) {
                    // Shredded array elements must not be null.
                    return VariantBinaryUtil::MalformedVariant();
                }
                schema = schema->array_schema.get();
                current = &element_array;
                row = element_row;
            }
            ++segment_idx;
        }

        // The terminal position: rebuild the (sub-)variant and cast it to the target type.
        if (schema->typed_idx >= 0 && !current->field(schema->typed_idx)->IsNull(row)) {
            VariantBuilder variant_builder(/*allow_duplicate_keys=*/false);
            PAIMON_RETURN_NOT_OK(VariantReassembler::RebuildValue(*current, row, metadata, *schema,
                                                                  pool_, &variant_builder));
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> variant,
                                   variant_builder.Build(pool_));
            return VariantGetExecutor::CastToBuilder(variant, resolved.spec.target_field, builder,
                                                     resolved.spec.cast_args, pool_);
        }
        if (schema->variant_idx >= 0 && !current->field(schema->variant_idx)->IsNull(row)) {
            std::string_view value =
                static_cast<const arrow::BinaryArray&>(*current->field(schema->variant_idx))
                    .GetView(row);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> variant,
                                   GenericVariant::Create(value, metadata, pool_));
            return VariantGetExecutor::CastToBuilder(variant, resolved.spec.target_field, builder,
                                                     resolved.spec.cast_args, pool_);
        }
        return VariantBinaryUtil::MalformedVariant();
    }

    Status ExtractFromBinary(const arrow::StructArray& current, int64_t row,
                             std::string_view metadata, const ResolvedSpec& resolved,
                             size_t segment_idx, const VariantSchema* schema,
                             arrow::ArrayBuilder* builder) const {
        if (schema->variant_idx < 0 || current.field(schema->variant_idx)->IsNull(row)) {
            return ToPaimonStatus(builder->AppendNull());
        }
        std::string_view value =
            static_cast<const arrow::BinaryArray&>(*current.field(schema->variant_idx))
                .GetView(row);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> variant,
                               GenericVariant::Create(value, metadata, pool_));
        for (; segment_idx < resolved.segments.size() && variant != nullptr; ++segment_idx) {
            const VariantPathSegment& raw = resolved.segments[segment_idx].raw;
            PAIMON_ASSIGN_OR_RAISE(VariantValueType type, variant->GetType());
            if (raw.kind == VariantPathSegment::Kind::kObjectExtraction &&
                type == VariantValueType::kObject) {
                PAIMON_ASSIGN_OR_RAISE(variant, variant->GetFieldByKey(raw.key));
            } else if (raw.kind == VariantPathSegment::Kind::kArrayExtraction &&
                       type == VariantValueType::kArray) {
                PAIMON_ASSIGN_OR_RAISE(variant, variant->GetElementAtIndex(raw.index));
            } else {
                variant = nullptr;
            }
        }
        return VariantGetExecutor::CastToBuilder(variant, resolved.spec.target_field, builder,
                                                 resolved.spec.cast_args, pool_);
    }

    std::shared_ptr<arrow::Field> logical_field_;
    std::shared_ptr<arrow::Field> physical_field_;
    std::shared_ptr<VariantSchema> schema_;
    std::vector<ResolvedSpec> specs_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace

Result<std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>>>
VariantShreddingReadPlanFactory::CreateReadPlans(const std::shared_ptr<arrow::Schema>& read_schema,
                                                 const std::shared_ptr<arrow::Schema>& file_schema,
                                                 const std::shared_ptr<MemoryPool>& pool) {
    std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>> plans;
    for (const auto& read_field : read_schema->fields()) {
        if (!VariantTypeUtils::IsVariantField(read_field) &&
            !VariantAccessUtils::IsVariantAccessType(read_field->type())) {
            continue;
        }
        auto file_field = file_schema->GetFieldByName(read_field->name());
        if (file_field == nullptr) {
            // The column is absent in the file (schema evolution); it is filled with nulls
            // downstream.
            continue;
        }
        bool file_shredded = VariantShreddingUtils::IsShreddedFileType(file_field->type());
        if (VariantAccessUtils::IsVariantAccessType(read_field->type())) {
            PAIMON_ASSIGN_OR_RAISE(std::vector<VariantAccessSpec> specs,
                                   VariantAccessUtils::ParseAccessSpecs(read_field));
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> physical_field,
                                   VariantAccessUtils::ClipShreddedFileField(specs, file_field));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<VariantSchema> schema,
                VariantShreddingUtils::BuildVariantSchema(physical_field->type()));
            std::vector<ResolvedSpec> resolved;
            resolved.reserve(specs.size());
            for (const auto& spec : specs) {
                resolved.push_back(ResolveSpec(spec, schema.get()));
            }
            plans.emplace(read_field->name(), std::make_shared<VariantAccessColumnReadPlan>(
                                                  read_field, physical_field, std::move(schema),
                                                  std::move(resolved), pool));
        } else if (file_shredded) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<VariantSchema> schema,
                                   VariantShreddingUtils::BuildVariantSchema(file_field->type()));
            plans.emplace(read_field->name(), std::make_shared<FullVariantColumnReadPlan>(
                                                  read_field, file_field, std::move(schema)));
        }
        // A plain VARIANT read of an unshredded file needs no plan.
    }
    return plans;
}

}  // namespace paimon
