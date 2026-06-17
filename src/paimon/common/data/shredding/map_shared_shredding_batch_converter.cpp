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

#include "paimon/common/data/shredding/map_shared_shredding_batch_converter.h"

#include <algorithm>
#include <string>
#include <utility>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/data/shredding/map_shared_shredding_context.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/data/shredding/map_shredding_defs.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
namespace paimon {
/// Checks that a dynamic_cast result is not null, returning Status::Invalid on failure.
#define PAIMON_CHECK_NOT_NULL(ptr, msg)          \
    do {                                         \
        if (PAIMON_UNLIKELY((ptr) == nullptr)) { \
            return Status::Invalid(msg);         \
        }                                        \
    } while (false)

Result<MapSharedShreddingBatchConverter::ConverterBundle>
MapSharedShreddingBatchConverter::CreateConverter(
    const std::shared_ptr<arrow::Schema>& logical_schema,
    const std::shared_ptr<MapSharedShreddingContext>& context,
    const std::shared_ptr<MemoryPool>& pool) {
    ConverterBundle bundle;
    if (!context) {
        return bundle;
    }

    std::map<std::string, int32_t> field_to_k = context->ComputeNextK();
    PAIMON_ASSIGN_OR_RAISE(bundle.physical_schema, MapSharedShreddingUtils::LogicalToPhysicalSchema(
                                                       logical_schema, field_to_k));
    bundle.converter = std::make_shared<MapSharedShreddingBatchConverter>(
        logical_schema, bundle.physical_schema, field_to_k, pool);
    return bundle;
}

MapSharedShreddingBatchConverter::MapSharedShreddingBatchConverter(
    const std::shared_ptr<arrow::Schema>& logical_schema,
    const std::shared_ptr<arrow::Schema>& physical_schema,
    const std::map<std::string, int32_t>& field_to_num_columns,
    const std::shared_ptr<MemoryPool>& pool)
    : logical_schema_(logical_schema),
      physical_schema_(physical_schema),
      pool_(GetArrowPool(pool)) {
    // Iterate in schema field order (not map order) so that shredding_field_names_
    // matches the order in which shredding columns appear in the schema.
    // This is critical for the sequential matching logic in Convert().
    for (int32_t i = 0; i < logical_schema->num_fields(); ++i) {
        const std::string& name = logical_schema->field(i)->name();
        auto it = field_to_num_columns.find(name);
        if (it != field_to_num_columns.end()) {
            contexts_.emplace_back(name, it->second);
            shredding_field_names_.push_back(name);
        }
    }
}

Result<std::unique_ptr<ArrowArray>> MapSharedShreddingBatchConverter::Convert(
    ArrowArray* logical_batch) {
    std::shared_ptr<arrow::DataType> logical_type = arrow::struct_(logical_schema_->fields());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> logical_array,
                                      arrow::ImportArray(logical_batch, logical_type));
    auto logical_struct = std::dynamic_pointer_cast<arrow::StructArray>(logical_array);
    PAIMON_CHECK_NOT_NULL(logical_struct,
                          "MapSharedShreddingBatchConverter: input is not a StructArray");

    int32_t num_fields = logical_schema_->num_fields();
    arrow::ArrayVector physical_columns;
    physical_columns.reserve(num_fields);
    size_t context_idx = 0;
    for (int32_t col = 0; col < num_fields; ++col) {
        auto column = logical_struct->field(col);
        const std::string& field_name = logical_schema_->field(col)->name();
        if (context_idx < shredding_field_names_.size() &&
            shredding_field_names_[context_idx] == field_name) {
            auto physical_struct_type = physical_schema_->field(col)->type();
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::Array> physical_column,
                ConvertOneColumn(column, physical_struct_type, &contexts_[context_idx]));
            physical_columns.push_back(std::move(physical_column));
            ++context_idx;
        } else {
            physical_columns.push_back(column);
        }
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> physical_struct,
        arrow::StructArray::Make(physical_columns, physical_schema_->field_names()));

    std::unique_ptr<ArrowArray> result = std::make_unique<ArrowArray>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*physical_struct, result.get()));
    return result;
}

Result<std::shared_ptr<arrow::Array>> MapSharedShreddingBatchConverter::ConvertOneColumn(
    const std::shared_ptr<arrow::Array>& map_column,
    const std::shared_ptr<arrow::DataType>& physical_struct_type, ColumnContext* context) const {
    auto map_array = std::dynamic_pointer_cast<arrow::MapArray>(map_column);
    PAIMON_CHECK_NOT_NULL(map_array, "MapSharedShreddingBatchConverter: column is not a MapArray");

    int64_t num_rows = map_array->length();
    int32_t num_cols = context->num_columns;

    auto keys_array = std::dynamic_pointer_cast<arrow::StringArray>(map_array->keys());
    PAIMON_CHECK_NOT_NULL(keys_array,
                          "MapSharedShreddingBatchConverter: MAP keys are not StringArray");
    auto values_array = map_array->items();

    // Create StructBuilder from physical struct type — it owns all child builders.
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> struct_builder_base,
                                      arrow::MakeBuilder(physical_struct_type, pool_.get()));
    auto* struct_builder = dynamic_cast<arrow::StructBuilder*>(struct_builder_base.get());
    PAIMON_CHECK_NOT_NULL(struct_builder,
                          "MapSharedShreddingBatchConverter: failed to create StructBuilder");
    PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->Reserve(num_rows));

    // Extract child builders: [field_mapping, col_0..K-1, overflow]
    auto* field_mapping_builder =
        dynamic_cast<arrow::ListBuilder*>(struct_builder->field_builder(0));
    PAIMON_CHECK_NOT_NULL(field_mapping_builder,
                          "MapSharedShreddingBatchConverter: field_mapping is not a ListBuilder");
    auto* field_mapping_value_builder =
        dynamic_cast<arrow::Int32Builder*>(field_mapping_builder->value_builder());
    PAIMON_CHECK_NOT_NULL(
        field_mapping_value_builder,
        "MapSharedShreddingBatchConverter: field_mapping value is not Int32Builder");
    PAIMON_RETURN_NOT_OK_FROM_ARROW(field_mapping_builder->Reserve(num_rows));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(field_mapping_value_builder->Reserve(num_rows * num_cols));

    std::vector<arrow::ArrayBuilder*> col_builders_raw;
    col_builders_raw.reserve(num_cols);
    for (int32_t c = 0; c < num_cols; ++c) {
        arrow::ArrayBuilder* col_builder = struct_builder->field_builder(1 + c);
        PAIMON_CHECK_NOT_NULL(col_builder, "MapSharedShreddingBatchConverter: col builder is null");
        PAIMON_RETURN_NOT_OK_FROM_ARROW(col_builder->Reserve(num_rows));
        col_builders_raw.push_back(col_builder);
    }

    int32_t overflow_field_idx = 1 + num_cols;
    auto* overflow_builder =
        dynamic_cast<arrow::MapBuilder*>(struct_builder->field_builder(overflow_field_idx));
    PAIMON_CHECK_NOT_NULL(overflow_builder,
                          "MapSharedShreddingBatchConverter: overflow is not a MapBuilder");
    auto* overflow_key_builder =
        dynamic_cast<arrow::Int32Builder*>(overflow_builder->key_builder());
    PAIMON_CHECK_NOT_NULL(overflow_key_builder,
                          "MapSharedShreddingBatchConverter: overflow key is not Int32Builder");
    arrow::ArrayBuilder* overflow_value_builder = overflow_builder->item_builder();
    PAIMON_CHECK_NOT_NULL(overflow_value_builder,
                          "MapSharedShreddingBatchConverter: overflow value builder is null");
    PAIMON_RETURN_NOT_OK_FROM_ARROW(overflow_builder->Reserve(num_rows));

    // Process each row
    for (int64_t row = 0; row < num_rows; ++row) {
        if (map_array->IsNull(row)) {
            // StructBuilder::AppendNull() auto-appends empty values to all children.
            PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->AppendNull());
            continue;
        }

        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->Append());

        int64_t start = map_array->value_offset(row);
        int64_t length = map_array->value_length(row);

        // Extract field ids and build lookup map
        std::vector<int32_t> field_ids;
        std::unordered_map<int32_t, int64_t> field_id_to_value_index;
        ExtractRowFields(keys_array, start, length, &context->dict, &field_ids,
                         &field_id_to_value_index);

        // Allocate columns
        RowAllocation allocation = context->allocator.AllocateRow(field_ids);

        // Fill sub-columns
        PAIMON_RETURN_NOT_OK(AppendFieldMapping(allocation, num_cols, field_mapping_builder,
                                                field_mapping_value_builder));
        PAIMON_RETURN_NOT_OK(AppendColumnValues(values_array, allocation, field_id_to_value_index,
                                                num_cols, col_builders_raw));
        PAIMON_RETURN_NOT_OK(AppendOverflow(values_array, allocation, field_id_to_value_index,
                                            overflow_builder, overflow_key_builder,
                                            overflow_value_builder));
    }

    // Finalize
    std::shared_ptr<arrow::StructArray> result;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->Finish(&result));
    return result;
}

void MapSharedShreddingBatchConverter::ExtractRowFields(
    const std::shared_ptr<arrow::StringArray>& keys_array, int64_t start, int64_t length,
    MapSharedShreddingFieldDict* dict, std::vector<int32_t>* field_ids_out,
    std::unordered_map<int32_t, int64_t>* field_id_to_value_index_out) const {
    field_ids_out->clear();
    field_ids_out->reserve(length);
    field_id_to_value_index_out->clear();
    field_id_to_value_index_out->reserve(length);
    for (int64_t j = 0; j < length; ++j) {
        std::string key_str = keys_array->GetString(start + j);
        int32_t field_id = dict->GetOrAssign(key_str);
        field_ids_out->push_back(field_id);
        (*field_id_to_value_index_out)[field_id] = start + j;
    }
}

Status MapSharedShreddingBatchConverter::AppendFieldMapping(
    const RowAllocation& allocation, int32_t num_cols, arrow::ListBuilder* list_builder,
    arrow::Int32Builder* value_builder) const {
    PAIMON_RETURN_NOT_OK_FROM_ARROW(list_builder->Append());
    for (int32_t c = 0; c < num_cols; ++c) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->Append(allocation.col_to_field[c]));
    }
    return Status::OK();
}

Status MapSharedShreddingBatchConverter::AppendColumnValues(
    const std::shared_ptr<arrow::Array>& values_array, const RowAllocation& allocation,
    const std::unordered_map<int32_t, int64_t>& field_id_to_value_index, int32_t num_cols,
    const std::vector<arrow::ArrayBuilder*>& col_builders) const {
    for (int32_t c = 0; c < num_cols; ++c) {
        int32_t assigned_field_id = allocation.col_to_field[c];
        if (assigned_field_id == -1) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(col_builders[c]->AppendNull());
        } else {
            auto it = field_id_to_value_index.find(assigned_field_id);
            if (PAIMON_UNLIKELY(it == field_id_to_value_index.end())) {
                return Status::Invalid(
                    fmt::format("MapSharedShreddingBatchConverter: field_id {} assigned to col {} "
                                "but not found in current row",
                                assigned_field_id, c));
            }
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                col_builders[c]->AppendArraySlice(*values_array->data(), it->second, 1));
        }
    }
    return Status::OK();
}

Status MapSharedShreddingBatchConverter::AppendOverflow(
    const std::shared_ptr<arrow::Array>& values_array, const RowAllocation& allocation,
    const std::unordered_map<int32_t, int64_t>& field_id_to_value_index,
    arrow::MapBuilder* overflow_builder, arrow::Int32Builder* overflow_key_builder,
    arrow::ArrayBuilder* overflow_value_builder) const {
    if (allocation.overflow_fields.empty()) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(overflow_builder->AppendNull());
        return Status::OK();
    }

    PAIMON_RETURN_NOT_OK_FROM_ARROW(overflow_builder->Append());
    for (int32_t overflow_field_id : allocation.overflow_fields) {
        auto it = field_id_to_value_index.find(overflow_field_id);
        if (PAIMON_UNLIKELY(it == field_id_to_value_index.end())) {
            return Status::Invalid(fmt::format(
                "MapSharedShreddingBatchConverter: overflow field_id {} not found in current row",
                overflow_field_id));
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(overflow_key_builder->Append(overflow_field_id));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            overflow_value_builder->AppendArraySlice(*values_array->data(), it->second, 1));
    }
    return Status::OK();
}

Result<MapSharedShreddingFieldMeta> MapSharedShreddingBatchConverter::BuildFieldMeta(
    const std::string& field_name) const {
    for (const auto& context : contexts_) {
        if (context.field_name == field_name) {
            MapSharedShreddingFieldMeta meta;
            meta.name_to_id = context.dict.GetNameToId();
            // Convert set<int32_t> -> vector<int32_t> for field_to_columns
            for (const auto& [field_id, col_set] : context.allocator.GetFieldToColumns()) {
                meta.field_to_columns[field_id] =
                    std::vector<int32_t>(col_set.begin(), col_set.end());
            }
            meta.overflow_field_set = context.allocator.GetOverflowFieldSet();
            meta.num_columns = context.allocator.GetNumColumns();
            meta.max_row_width = context.allocator.GetMaxRowWidth();
            return meta;
        }
    }
    return Status::Invalid(fmt::format(
        "cannot find field_name '{}' in MapSharedShreddingBatchConverter contexts", field_name));
}

const std::vector<std::string>& MapSharedShreddingBatchConverter::GetShreddingColumnNames() const {
    return shredding_field_names_;
}

}  // namespace paimon
