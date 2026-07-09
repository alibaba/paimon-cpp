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
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_shredding_writer.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

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

Result<std::unique_ptr<ArrowArray>> VariantShreddingBatchConverter::Convert(
    ArrowArray* logical_batch) {
    auto logical_struct_type = arrow::struct_(plan_->LogicalSchema()->fields());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> logical_array,
                                      arrow::ImportArray(logical_batch, logical_struct_type));
    const auto& logical_struct = std::static_pointer_cast<arrow::StructArray>(logical_array);

    arrow::ArrayVector physical_arrays = logical_struct->fields();
    const auto& physical_fields = plan_->PhysicalSchema()->fields();
    for (int32_t i = 0; i < logical_struct->num_fields(); ++i) {
        const std::string& field_name = logical_struct->struct_type()->field(i)->name();
        auto schema_it = plan_->ColumnSchemas().find(field_name);
        if (schema_it == plan_->ColumnSchemas().end()) {
            continue;
        }
        const auto& physical_type = plan_->ColumnPhysicalTypes().at(field_name);
        if (logical_struct->field(i)->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid(fmt::format(
                "variant column {} is not a struct<value, metadata> column", field_name));
        }
        auto variant_column =
            std::static_pointer_cast<arrow::StructArray>(logical_struct->field(i));
        if (variant_column->num_fields() != 2) {
            return Status::Invalid(fmt::format(
                "variant column {} is not a struct<value, metadata> column", field_name));
        }
        const auto& value_column =
            std::static_pointer_cast<arrow::BinaryArray>(variant_column->field(0));
        const auto& metadata_column =
            std::static_pointer_cast<arrow::BinaryArray>(variant_column->field(1));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<VariantShreddedColumnWriter> writer,
                               VariantShreddedColumnWriter::Create(schema_it->second, physical_type,
                                                                   arrow_pool_.get()));
        for (int64_t row = 0; row < variant_column->length(); ++row) {
            if (variant_column->IsNull(row)) {
                PAIMON_RETURN_NOT_OK(writer->AppendNull());
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GenericVariant> variant,
                                   GenericVariant::Create(value_column->GetView(row),
                                                          metadata_column->GetView(row), pool_));
            PAIMON_RETURN_NOT_OK(writer->Append(*variant));
        }
        PAIMON_ASSIGN_OR_RAISE(physical_arrays[i], writer->Finish());
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
