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
#include <unordered_map>
#include <vector>

#include "arrow/memory_pool.h"
#include "arrow/type_fwd.h"
#include "paimon/common/data/shredding/map_shared_shredding_column_allocator.h"
#include "paimon/common/data/shredding/map_shared_shredding_field_dict.h"
#include "paimon/common/data/shredding/map_shredding_defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

struct ArrowArray;

namespace paimon {

class CoreOptions;
class MapSharedShreddingContext;

/// Converts logical batches containing MAP<STRING, T> columns into physical batches
/// where each shared-shredding MAP column is replaced by
/// STRUCT<__field_mapping, __col_0..K-1, __overflow>.
///
/// Non-shared-shredding columns are passed through unchanged.
/// Each shared-shredding column has its own FieldDict and ColumnAllocator.
class MapSharedShreddingBatchConverter {
 public:
    /// Creates a converter for one file write cycle.
    /// Computes per-file K from context, builds physical schema, and constructs the converter.
    /// @param logical_schema The original schema with MAP<STRING, T> columns.
    /// @param context The cross-file shared context for K adaptation.
    /// @param options CoreOptions used to read each column's placement policy.
    /// @param pool Paimon memory pool for Arrow allocations.
    /// @return The converter.
    static Result<std::shared_ptr<MapSharedShreddingBatchConverter>> Create(
        const std::shared_ptr<arrow::Schema>& logical_schema,
        const std::shared_ptr<MapSharedShreddingContext>& context, const CoreOptions& options,
        const std::shared_ptr<MemoryPool>& pool);

    /// Returns the physical schema produced for this converter.
    const std::shared_ptr<arrow::Schema>& GetPhysicalSchema() const;

    /// Converts a logical batch to a physical batch.
    /// @param logical_batch Input ArrowArray (C ABI) with logical schema. Consumed on success.
    /// @return Owned physical ArrowArray (C ABI) with physical schema.
    Result<std::unique_ptr<ArrowArray>> Convert(ArrowArray* logical_batch);

    /// Builds MapSharedShreddingFieldMeta for one shredding column (by field name).
    /// Called at file close to serialize metadata.
    Result<MapSharedShreddingFieldMeta> BuildFieldMeta(const std::string& field_name) const;

    /// Returns all shredding column field names.
    const std::vector<std::string>& GetShreddingColumnNames() const;

 private:
    /// Per-column context for one shared-shredding MAP column.
    struct ColumnContext {
        std::string field_name;
        int32_t num_columns;  // K
        MapSharedShreddingFieldDict dict;
        std::unique_ptr<MapSharedShreddingColumnAllocator> allocator;

        ColumnContext(const std::string& field_name, int32_t num_columns,
                      std::unique_ptr<MapSharedShreddingColumnAllocator>&& allocator);
    };

    /// Constructs a converter.
    /// @param logical_schema The original schema with MAP<STRING, T> columns.
    /// @param physical_schema The physical schema (MAP columns replaced with STRUCT).
    /// @param contexts Per-shredding-column conversion contexts.
    /// @param shredding_field_names Shared-shredding field names in schema order.
    /// @param pool Paimon memory pool for Arrow allocations.
    MapSharedShreddingBatchConverter(const std::shared_ptr<arrow::Schema>& logical_schema,
                                     const std::shared_ptr<arrow::Schema>& physical_schema,
                                     std::vector<ColumnContext>&& contexts,
                                     std::vector<std::string>&& shredding_field_names,
                                     const std::shared_ptr<MemoryPool>& pool);

    /// Converts one MAP<STRING, T> column to physical STRUCT for all rows.
    /// @param physical_struct_type The physical struct type from physical_schema for this column.
    Result<std::shared_ptr<arrow::Array>> ConvertOneColumn(
        const std::shared_ptr<arrow::Array>& map_column,
        const std::shared_ptr<arrow::DataType>& physical_struct_type, ColumnContext* context) const;

    /// Extracts field ids and builds field_id -> value_index map for one row.
    void ExtractRowFields(const std::shared_ptr<arrow::StringArray>& keys_array, int64_t start,
                          int64_t length, MapSharedShreddingFieldDict* dict,
                          std::vector<int32_t>* field_ids_out,
                          std::unordered_map<int32_t, int64_t>* field_id_to_value_index_out) const;

    /// Appends __field_mapping list for one row.
    Status AppendFieldMapping(const RowAllocation& allocation, int32_t num_cols,
                              arrow::ListBuilder* list_builder,
                              arrow::Int32Builder* value_builder) const;

    /// Appends __col_0..K-1 values for one row.
    Status AppendColumnValues(const std::shared_ptr<arrow::Array>& values_array,
                              const RowAllocation& allocation,
                              const std::unordered_map<int32_t, int64_t>& field_id_to_value_index,
                              int32_t num_cols,
                              const std::vector<arrow::ArrayBuilder*>& col_builders) const;

    /// Appends __overflow entries for one row.
    Status AppendOverflow(const std::shared_ptr<arrow::Array>& values_array,
                          const RowAllocation& allocation,
                          const std::unordered_map<int32_t, int64_t>& field_id_to_value_index,
                          arrow::MapBuilder* overflow_builder,
                          arrow::Int32Builder* overflow_key_builder,
                          arrow::ArrayBuilder* overflow_value_builder) const;

    std::shared_ptr<arrow::Schema> logical_schema_;
    std::shared_ptr<arrow::Schema> physical_schema_;
    std::vector<ColumnContext> contexts_;
    std::vector<std::string> shredding_field_names_;
    std::shared_ptr<arrow::MemoryPool> pool_;
};

}  // namespace paimon
