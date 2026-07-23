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

#include "paimon/common/data/variant/variant_shredding_batch_converter.h"

#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_shredding_writer.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

namespace {

/// Whether any enclosing struct is null at `row`. The contents of child slots under a null
/// ancestor are unspecified in Arrow and must not be decoded.
bool AnyAncestorNull(const std::vector<const arrow::Array*>& ancestors, int64_t row) {
    for (const arrow::Array* ancestor : ancestors) {
        if (ancestor->IsNull(row)) {
            return true;
        }
    }
    return false;
}

/// Shreds one variant column array into its physical shredded representation.
Result<std::shared_ptr<arrow::Array>> ShredVariantColumn(
    const arrow::Array& column, const std::string& field_name,
    const std::vector<const arrow::Array*>& ancestors,
    const std::shared_ptr<VariantSchema>& variant_schema,
    const std::shared_ptr<arrow::DataType>& physical_type, const std::shared_ptr<MemoryPool>& pool,
    arrow::MemoryPool* arrow_pool) {
    if (column.type_id() != arrow::Type::STRUCT) {
        return Status::Invalid(
            fmt::format("variant column {} is not a struct<value, metadata> column", field_name));
    }
    const auto& variant_column = arrow::internal::checked_cast<const arrow::StructArray&>(column);
    if (variant_column.num_fields() != 2) {
        return Status::Invalid(
            fmt::format("variant column {} is not a struct<value, metadata> column", field_name));
    }
    const auto& value_column =
        arrow::internal::checked_cast<const arrow::BinaryArray&>(*variant_column.field(0));
    const auto& metadata_column =
        arrow::internal::checked_cast<const arrow::BinaryArray&>(*variant_column.field(1));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<VariantShreddedColumnWriter> writer,
        VariantShreddedColumnWriter::Create(variant_schema, physical_type, arrow_pool));
    for (int64_t row = 0; row < variant_column.length(); ++row) {
        if (variant_column.IsNull(row) || AnyAncestorNull(ancestors, row)) {
            PAIMON_RETURN_NOT_OK(writer->AppendNull());
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<GenericVariant> variant,
            GenericVariant::Create(value_column.GetView(row), metadata_column.GetView(row), pool));
        PAIMON_RETURN_NOT_OK(writer->Append(*variant));
    }
    return writer->Finish();
}

}  // namespace

VariantShreddingBatchConverter::VariantShreddingBatchConverter(
    const std::shared_ptr<VariantShreddingWritePlan>& plan, const std::shared_ptr<MemoryPool>& pool)
    : plan_(plan), pool_(pool), arrow_pool_(GetArrowPool(pool)) {}

Result<std::shared_ptr<VariantShreddingBatchConverter>> VariantShreddingBatchConverter::Create(
    const std::shared_ptr<VariantShreddingWritePlan>& plan,
    const std::shared_ptr<MemoryPool>& pool) {
    if (!plan) {
        return Status::Invalid("variant shredding batch converter requires a write plan");
    }
    return std::shared_ptr<VariantShreddingBatchConverter>(
        new VariantShreddingBatchConverter(plan, pool));
}

const std::shared_ptr<arrow::Schema>& VariantShreddingBatchConverter::GetPhysicalSchema() const {
    return plan_->PhysicalSchema();
}

Result<std::shared_ptr<arrow::Array>> VariantShreddingBatchConverter::ConvertField(
    const std::shared_ptr<arrow::Array>& logical,
    const std::shared_ptr<arrow::Field>& logical_field,
    const std::shared_ptr<arrow::Field>& physical_field, std::vector<int32_t>* path,
    std::vector<const arrow::Array*>* ancestors) const {
    for (const auto& column : plan_->Columns()) {
        if (column.path == *path) {
            return ShredVariantColumn(*logical, logical_field->name(), *ancestors,
                                      column.variant_schema, column.physical_type, pool_,
                                      arrow_pool_.get());
        }
    }
    if (logical_field->type()->Equals(*physical_field->type())) {
        return logical;
    }
    // The types differ below this struct field: convert the planned descendants recursively and
    // rebuild the struct with its original validity.
    if (logical->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid(fmt::format("variant shredding cannot convert non-struct field {}",
                                           logical_field->name()));
    }
    const auto& logical_struct = arrow::internal::checked_cast<const arrow::StructArray&>(*logical);
    const auto& logical_type =
        arrow::internal::checked_cast<const arrow::StructType&>(*logical_field->type());
    const auto& physical_type =
        arrow::internal::checked_cast<const arrow::StructType&>(*physical_field->type());
    if (logical_type.num_fields() != physical_type.num_fields()) {
        return Status::Invalid(fmt::format("variant shredding physical struct {} does not match",
                                           physical_field->name()));
    }
    arrow::ArrayVector converted_children(logical_struct.num_fields());
    ancestors->push_back(logical.get());
    for (int32_t i = 0; i < logical_struct.num_fields(); ++i) {
        path->push_back(i);
        PAIMON_ASSIGN_OR_RAISE(converted_children[i],
                               ConvertField(logical_struct.field(i), logical_type.field(i),
                                            physical_type.field(i), path, ancestors));
        path->pop_back();
    }
    ancestors->pop_back();
    std::shared_ptr<arrow::Buffer> validity;
    int64_t null_count = logical_struct.null_count();
    if (null_count > 0) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            validity,
            arrow::internal::CopyBitmap(arrow_pool_.get(), logical_struct.null_bitmap_data(),
                                        logical_struct.offset(), logical_struct.length()));
    }
    return std::make_shared<arrow::StructArray>(physical_field->type(), logical_struct.length(),
                                                std::move(converted_children), std::move(validity),
                                                null_count);
}

Result<std::unique_ptr<ArrowArray>> VariantShreddingBatchConverter::Convert(
    ArrowArray* logical_batch) {
    auto logical_struct_type = arrow::struct_(plan_->LogicalSchema()->fields());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> logical_array,
                                      arrow::ImportArray(logical_batch, logical_struct_type));
    const auto& logical_struct = std::static_pointer_cast<arrow::StructArray>(logical_array);

    const auto& logical_fields = plan_->LogicalSchema()->fields();
    const auto& physical_fields = plan_->PhysicalSchema()->fields();
    arrow::ArrayVector physical_arrays(logical_struct->num_fields());
    std::vector<int32_t> path;
    std::vector<const arrow::Array*> ancestors;
    for (int32_t i = 0; i < logical_struct->num_fields(); ++i) {
        path.push_back(i);
        PAIMON_ASSIGN_OR_RAISE(physical_arrays[i],
                               ConvertField(logical_struct->field(i), logical_fields[i],
                                            physical_fields[i], &path, &ancestors));
        path.pop_back();
    }

    arrow::FieldVector physical_struct_fields(physical_fields.begin(), physical_fields.end());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> physical_struct,
        arrow::StructArray::Make(physical_arrays, physical_struct_fields));
    auto result = std::make_unique<ArrowArray>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*physical_struct, result.get()));
    return result;
}

}  // namespace paimon
