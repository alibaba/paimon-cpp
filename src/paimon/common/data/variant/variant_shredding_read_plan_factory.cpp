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
#include "arrow/util/checked_cast.h"
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
                              std::shared_ptr<VariantSchema> schema,
                              std::shared_ptr<MemoryPool> pool)
        : logical_field_(std::move(logical_field)),
          physical_field_(std::move(physical_field)),
          schema_(std::move(schema)),
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
        auto physical_struct = std::static_pointer_cast<arrow::StructArray>(physical);
        return VariantReassembler::AssembleVariantArray(physical_struct, schema_, pool_, pool);
    }

 private:
    std::shared_ptr<arrow::Field> logical_field_;
    std::shared_ptr<arrow::Field> physical_field_;
    std::shared_ptr<VariantSchema> schema_;
    std::shared_ptr<MemoryPool> pool_;
};

/// Restores the logical `struct<value, metadata>` field order of an untyped physical
/// `struct<metadata, value>` without copying either binary child.
class UntypedVariantColumnReadPlan : public ShreddingColumnReadPlan {
 public:
    UntypedVariantColumnReadPlan(std::shared_ptr<arrow::Field> logical_field,
                                 std::shared_ptr<arrow::Field> physical_field)
        : logical_field_(std::move(logical_field)), physical_field_(std::move(physical_field)) {}

    const std::shared_ptr<arrow::Field>& LogicalField() const override {
        return logical_field_;
    }

    const std::shared_ptr<arrow::Field>& PhysicalField() const override {
        return physical_field_;
    }

    Result<std::shared_ptr<arrow::Array>> Assemble(const std::shared_ptr<arrow::Array>& physical,
                                                   arrow::MemoryPool*) const override {
        if (physical->type_id() != arrow::Type::STRUCT ||
            physical->data()->child_data.size() != 2) {
            return Status::Invalid(fmt::format("cannot reorder untyped physical variant field {}",
                                               physical_field_->name()));
        }
        std::shared_ptr<arrow::ArrayData> logical_data = physical->data()->Copy();
        logical_data->type = logical_field_->type();
        std::swap(logical_data->child_data[0], logical_data->child_data[1]);
        return arrow::MakeArray(std::move(logical_data));
    }

 private:
    std::shared_ptr<arrow::Field> logical_field_;
    std::shared_ptr<arrow::Field> physical_field_;
};

/// A node of a nested variant plan tree: a variant position with its own leaf plan, or a nested
/// container level to descend through.
struct NestedVariantNode {
    /// The leaf plan when this position is a variant column: a full reassembly or a
    /// variant-access extraction.
    std::shared_ptr<ShreddingColumnReadPlan> plan;
    /// The children whose subtree holds planned variants, by Arrow child index. The index
    /// addresses struct fields, the list element field, and the map entries field alike.
    std::map<int32_t, NestedVariantNode> children;
};

/// Applies the leaf plans of the variant columns nested inside a top-level STRUCT / LIST / MAP
/// column, rebuilding the containers around them.
class NestedVariantColumnReadPlan : public ShreddingColumnReadPlan {
 public:
    NestedVariantColumnReadPlan(std::shared_ptr<arrow::Field> logical_field,
                                std::shared_ptr<arrow::Field> physical_field,
                                NestedVariantNode root)
        : logical_field_(std::move(logical_field)),
          physical_field_(std::move(physical_field)),
          root_(std::move(root)) {}

    const std::shared_ptr<arrow::Field>& LogicalField() const override {
        return logical_field_;
    }

    const std::shared_ptr<arrow::Field>& PhysicalField() const override {
        return physical_field_;
    }

    Result<std::shared_ptr<arrow::Array>> Assemble(const std::shared_ptr<arrow::Array>& physical,
                                                   arrow::MemoryPool* pool) const override {
        return AssembleNode(physical, logical_field_, root_, pool);
    }

 private:
    Result<std::shared_ptr<arrow::Array>> AssembleNode(
        const std::shared_ptr<arrow::Array>& physical,
        const std::shared_ptr<arrow::Field>& logical_field, const NestedVariantNode& node,
        arrow::MemoryPool* pool) const {
        if (node.plan != nullptr) {
            return node.plan->Assemble(physical, pool);
        }
        const std::shared_ptr<arrow::DataType>& logical_type = logical_field->type();
        const std::shared_ptr<arrow::ArrayData>& physical_data = physical->data();
        if (physical->type_id() != logical_type->id()) {
            return Status::Invalid(fmt::format(
                "shredded variant field {} is stored as {} but read as {}", logical_field->name(),
                physical->type()->ToString(), logical_type->ToString()));
        }
        // Swap the planned children into a copy of the physical array's own data: the validity
        // bitmap, list offsets and slice offset carry over untouched, so STRUCT, LIST and MAP
        // rebuild alike. Children are assembled unsliced to stay aligned with those offsets.
        std::shared_ptr<arrow::ArrayData> data = physical_data->Copy();
        data->type = logical_type;
        for (const auto& [index, child_node] : node.children) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> child,
                                   AssembleNode(arrow::MakeArray(physical_data->child_data[index]),
                                                logical_type->field(index), child_node, pool));
            data->child_data[index] = child->data();
        }
        return arrow::MakeArray(data);
    }

    std::shared_ptr<arrow::Field> logical_field_;
    std::shared_ptr<arrow::Field> physical_field_;
    NestedVariantNode root_;
};

/// Whether the field is read as a variant column: either as a plain VARIANT or as a
/// variant-access projection (which keeps the variant marker but replaces the type).
bool IsVariantReadField(const std::shared_ptr<arrow::Field>& field) {
    return VariantTypeUtils::IsVariantField(field) ||
           VariantAccessUtils::IsVariantAccessType(field->type());
}

/// Whether the type is a nested container the plan tree descends through.
bool IsNestedContainer(const std::shared_ptr<arrow::DataType>& type) {
    return type->id() == arrow::Type::STRUCT || type->id() == arrow::Type::LIST ||
           type->id() == arrow::Type::MAP;
}

/// Whether the field is a nested container (that is not itself a variant) holding a variant
/// field in its subtree.
bool ContainsNestedVariant(const std::shared_ptr<arrow::Field>& field) {
    if (IsVariantReadField(field) || !IsNestedContainer(field->type())) {
        return false;
    }
    for (const auto& child : field->type()->fields()) {
        if (IsVariantReadField(child) || ContainsNestedVariant(child)) {
            return true;
        }
    }
    return false;
}

// Both are defined below: CreateVariantColumnPlan after the access plan it builds, and
// BuildNestedVariantPlan because it and PlanChild are mutually recursive.
Result<std::shared_ptr<ShreddingColumnReadPlan>> CreateVariantColumnPlan(
    const std::shared_ptr<arrow::Field>& read_field,
    const std::shared_ptr<arrow::Field>& file_field, const std::shared_ptr<MemoryPool>& pool,
    bool allow_pruning);

Result<bool> BuildNestedVariantPlan(const std::shared_ptr<arrow::Field>& read_field,
                                    const std::shared_ptr<arrow::Field>& file_field,
                                    const std::shared_ptr<MemoryPool>& pool, bool inside_repeated,
                                    std::shared_ptr<arrow::Field>* physical_field,
                                    NestedVariantNode* node);

/// Plans one child position: a variant leaf, or a nested container to descend into. Returns
/// whether the position needs a plan; `physical_child` receives the field to push down for it.
Result<bool> PlanChild(const std::shared_ptr<arrow::Field>& read_child,
                       const std::shared_ptr<arrow::Field>& file_child,
                       const std::shared_ptr<MemoryPool>& pool, bool inside_repeated,
                       std::shared_ptr<arrow::Field>* physical_child, NestedVariantNode* node) {
    *physical_child = read_child;
    if (IsVariantReadField(read_child)) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<ShreddingColumnReadPlan> plan,
            CreateVariantColumnPlan(read_child, file_child, pool, !inside_repeated));
        if (plan == nullptr) {
            // An unshredded column read as a plain VARIANT needs no plan.
            return false;
        }
        *physical_child = read_child->WithType(plan->PhysicalField()->type());
        node->plan = std::move(plan);
        return true;
    }
    if (!ContainsNestedVariant(read_child) ||
        file_child->type()->id() != read_child->type()->id()) {
        return false;
    }
    return BuildNestedVariantPlan(read_child, file_child, pool, inside_repeated, physical_child,
                                  node);
}

/// Whether the read and file children of a level inside a repeated group line up one to one,
/// which they must because the file subtree is pushed down verbatim there.
///
/// Only STRUCT children are matched by name: LIST and MAP child names are format conventions
/// that differ between the read schema and the file (`element` vs `item`, `entries` vs the
/// parquet `key_value` group), so those match positionally.
bool ChildrenLineUp(const arrow::DataType& read_type, const arrow::DataType& file_type) {
    if (read_type.num_fields() != file_type.num_fields()) {
        return false;
    }
    if (read_type.id() != arrow::Type::STRUCT) {
        return true;
    }
    for (int32_t i = 0; i < read_type.num_fields(); ++i) {
        if (read_type.field(i)->name() != file_type.field(i)->name()) {
            return false;
        }
    }
    return true;
}

/// Builds the nested plan of one column subtree: substitutes the physical file types at the
/// nested variant positions into `read_field` (producing the physical field to push down) and
/// records their leaf plans in `node`. Returns whether any nested position needs a plan.
///
/// A STRUCT level is matched by field name and keeps per-child pruning. A LIST or MAP level is
/// matched positionally and pushes the file subtree down verbatim, because the parquet reader
/// rejects partial projection inside a repeated group; everything below it is therefore read in
/// full and only reassembled back.
Result<bool> BuildNestedVariantPlan(const std::shared_ptr<arrow::Field>& read_field,
                                    const std::shared_ptr<arrow::Field>& file_field,
                                    const std::shared_ptr<MemoryPool>& pool, bool inside_repeated,
                                    std::shared_ptr<arrow::Field>* physical_field,
                                    NestedVariantNode* node) {
    const arrow::DataType& read_type = *read_field->type();
    const arrow::DataType& file_type = *file_field->type();
    // True for a LIST or MAP level and for everything below it, including plain STRUCT levels.
    const bool in_repeated_subtree = inside_repeated || read_type.id() != arrow::Type::STRUCT;
    *physical_field = read_field;
    if (in_repeated_subtree && !ChildrenLineUp(read_type, file_type)) {
        return false;
    }

    arrow::FieldVector physical_children = read_type.fields();
    bool needs_plan = false;
    for (int32_t i = 0; i < read_type.num_fields(); ++i) {
        const std::shared_ptr<arrow::Field>& read_child = read_type.field(i);
        std::shared_ptr<arrow::Field> file_child =
            in_repeated_subtree
                ? file_type.field(i)
                : arrow::internal::checked_cast<const arrow::StructType&>(file_type).GetFieldByName(
                      read_child->name());
        if (file_child == nullptr) {
            // The nested column is absent in the file (schema evolution); it is filled with
            // nulls downstream.
            continue;
        }
        NestedVariantNode child_node;
        PAIMON_ASSIGN_OR_RAISE(bool child_needs_plan,
                               PlanChild(read_child, file_child, pool, in_repeated_subtree,
                                         &physical_children[i], &child_node));
        if (child_needs_plan) {
            node->children[i] = std::move(child_node);
            needs_plan = true;
        }
    }
    if (!needs_plan) {
        return false;
    }
    *physical_field =
        in_repeated_subtree ? file_field : read_field->WithType(arrow::struct_(physical_children));
    return true;
}

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
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Reserve(physical_struct.length()));
        auto* struct_builder = static_cast<arrow::StructBuilder*>(builder.get());
        for (int64_t row = 0; row < physical_struct.length(); ++row) {
            if (physical_struct.IsNull(row)) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->AppendNull());
                continue;
            }
            if (physical_struct.field(schema_->top_level_metadata_idx)->IsNull(row)) {
                return VariantBinaryUtil::MalformedVariant("the variant metadata column is null");
            }
            std::string_view metadata = static_cast<const arrow::BinaryArray&>(
                                            *physical_struct.field(schema_->top_level_metadata_idx))
                                            .GetView(row);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->Append());
            for (size_t i = 0; i < specs_.size(); ++i) {
                PAIMON_RETURN_NOT_OK(
                    ExtractField(physical_struct, row, metadata, specs_[i],
                                 struct_builder->field_builder(static_cast<int32_t>(i))));
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
                    return VariantBinaryUtil::MalformedVariant(
                        "a shredded object field group is null");
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
                    return VariantBinaryUtil::MalformedVariant(
                        "a shredded array element group is null");
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
            return VariantGetExecutor::CastToBuilder(variant, resolved.spec.target_field,
                                                     resolved.spec.cast_args, pool_, builder);
        }
        if (schema->variant_idx >= 0 && !current->field(schema->variant_idx)->IsNull(row)) {
            std::string_view value =
                static_cast<const arrow::BinaryArray&>(*current->field(schema->variant_idx))
                    .GetView(row);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> variant,
                                   GenericVariant::Create(value, metadata, pool_));
            return VariantGetExecutor::CastToBuilder(variant, resolved.spec.target_field,
                                                     resolved.spec.cast_args, pool_, builder);
        }
        return VariantBinaryUtil::MalformedVariant(
            "both typed_value and value of a required variant are null");
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
        return VariantGetExecutor::CastToBuilder(variant, resolved.spec.target_field,
                                                 resolved.spec.cast_args, pool_, builder);
    }

    std::shared_ptr<arrow::Field> logical_field_;
    std::shared_ptr<arrow::Field> physical_field_;
    std::shared_ptr<VariantSchema> schema_;
    std::vector<ResolvedSpec> specs_;
    std::shared_ptr<MemoryPool> pool_;
};

/// Builds the leaf read plan of one variant position, at the top level or nested inside a
/// container column: a variant-access projection extracts the described paths (from a shredded
/// or an unshredded file column), and a plain VARIANT read reassembles a shredded file column.
/// Returns nullptr when a plain VARIANT read of an unshredded file column needs no plan.
///
/// `allow_pruning` narrows the scan to the sub-columns the access paths need. It is off inside a
/// repeated group, where the file subtree must be read whole.
Result<std::shared_ptr<ShreddingColumnReadPlan>> CreateVariantColumnPlan(
    const std::shared_ptr<arrow::Field>& read_field,
    const std::shared_ptr<arrow::Field>& file_field, const std::shared_ptr<MemoryPool>& pool,
    bool allow_pruning) {
    if (VariantAccessUtils::IsVariantAccessType(read_field->type())) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<VariantAccessSpec> specs,
                               VariantAccessUtils::ParseAccessSpecs(read_field));
        std::shared_ptr<arrow::Field> physical_field = file_field;
        if (allow_pruning) {
            PAIMON_ASSIGN_OR_RAISE(physical_field,
                                   VariantAccessUtils::ClipShreddedFileField(specs, file_field));
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<VariantSchema> schema,
                               VariantShreddingUtils::BuildVariantSchema(physical_field->type()));
        std::vector<ResolvedSpec> resolved;
        resolved.reserve(specs.size());
        for (const auto& spec : specs) {
            resolved.push_back(ResolveSpec(spec, schema.get()));
        }
        return std::make_shared<VariantAccessColumnReadPlan>(
            read_field, physical_field, std::move(schema), std::move(resolved), pool);
    }
    if (VariantShreddingUtils::IsUntypedPhysicalVariantType(file_field->type())) {
        return std::make_shared<UntypedVariantColumnReadPlan>(read_field, file_field);
    }
    if (!VariantShreddingUtils::IsShreddedFileType(file_field->type())) {
        return std::shared_ptr<ShreddingColumnReadPlan>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<VariantSchema> schema,
                           VariantShreddingUtils::BuildVariantSchema(file_field->type()));
    return std::make_shared<FullVariantColumnReadPlan>(read_field, file_field, std::move(schema),
                                                       pool);
}

}  // namespace

Result<std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>>>
VariantShreddingReadPlanFactory::CreateReadPlans(const std::shared_ptr<arrow::Schema>& read_schema,
                                                 const std::shared_ptr<arrow::Schema>& file_schema,
                                                 const std::shared_ptr<MemoryPool>& pool) {
    std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>> plans;
    for (const auto& read_field : read_schema->fields()) {
        bool nested_variant = ContainsNestedVariant(read_field);
        if (!IsVariantReadField(read_field) && !nested_variant) {
            continue;
        }
        auto file_field = file_schema->GetFieldByName(read_field->name());
        if (file_field == nullptr) {
            // The column is absent in the file (schema evolution); it is filled with nulls
            // downstream.
            continue;
        }
        if (nested_variant) {
            if (file_field->type()->id() != read_field->type()->id()) {
                continue;
            }
            std::shared_ptr<arrow::Field> physical_field;
            NestedVariantNode root;
            PAIMON_ASSIGN_OR_RAISE(
                bool needs_plan,
                BuildNestedVariantPlan(read_field, file_field, pool,
                                       /*inside_repeated=*/false, &physical_field, &root));
            if (needs_plan) {
                plans.emplace(read_field->name(), std::make_shared<NestedVariantColumnReadPlan>(
                                                      read_field, physical_field, std::move(root)));
            }
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<ShreddingColumnReadPlan> plan,
            CreateVariantColumnPlan(read_field, file_field, pool, /*allow_pruning=*/true));
        if (plan != nullptr) {
            plans.emplace(read_field->name(), std::move(plan));
        }
    }
    return plans;
}

}  // namespace paimon
