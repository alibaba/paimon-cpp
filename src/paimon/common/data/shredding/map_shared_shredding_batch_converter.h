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

/// Converts logical batches containing MAP<STRING, T> columns into physical batches
/// where each shared-shredding MAP column is replaced by
/// STRUCT<__field_mapping, __col_0..K-1, __overflow>.
///
/// Non-shared-shredding columns are passed through unchanged.
/// Each shared-shredding column has its own FieldDict and ColumnAllocator.
class MapSharedShreddingBatchConverter {
 public:
    /// Per-column context for one shared-shredding MAP column.
    struct ColumnContext {
        int32_t logical_index;
        int32_t num_columns;  // K
        MapSharedShreddingFieldDict dict;
        MapSharedShreddingColumnAllocator allocator;

        ColumnContext(int32_t logical_index, int32_t num_columns)
            : logical_index(logical_index), num_columns(num_columns), allocator(num_columns) {}
    };

    /// Constructs a converter.
    /// @param logical_schema The original schema with MAP<STRING, T> columns.
    /// @param physical_schema The physical schema (MAP columns replaced with STRUCT).
    /// @param column_to_num_columns Map from logical column index to K.
    /// @param pool Paimon memory pool for Arrow allocations.
    MapSharedShreddingBatchConverter(const std::shared_ptr<arrow::Schema>& logical_schema,
                                     const std::shared_ptr<arrow::Schema>& physical_schema,
                                     const std::map<int32_t, int32_t>& column_to_num_columns,
                                     const std::shared_ptr<MemoryPool>& pool);

    /// Converts a logical batch to a physical batch.
    /// @param logical_batch Input ArrowArray (C ABI) with logical schema. Consumed on success.
    /// @return Owned physical ArrowArray (C ABI) with physical schema.
    Result<std::unique_ptr<ArrowArray>> Convert(ArrowArray* logical_batch);

    /// Builds MapSharedShreddingFieldMeta for one shredding column (by logical index).
    /// Called at file close to serialize metadata.
    MapSharedShreddingFieldMeta BuildFieldMeta(int32_t logical_col_index) const;

    /// Returns all shredding column logical indices.
    const std::vector<int32_t>& GetShreddingColumnIndices() const;

 private:
    /// Converts one MAP<STRING, T> column to physical STRUCT for all rows.
    /// @param physical_struct_type The physical struct type from physical_schema for this column.
    Result<std::shared_ptr<arrow::Array>> ConvertOneColumn(
        const std::shared_ptr<arrow::Array>& map_column,
        const std::shared_ptr<arrow::DataType>& physical_struct_type, ColumnContext* context);

    /// Extracts field ids and builds field_id -> value_index map for one row.
    void ExtractRowFields(const std::shared_ptr<arrow::StringArray>& keys_array, int64_t start,
                          int64_t length, MapSharedShreddingFieldDict* dict,
                          std::vector<int32_t>* field_ids_out,
                          std::unordered_map<int32_t, int64_t>* field_id_to_value_index_out);

    /// Appends __field_mapping list for one row.
    Status AppendFieldMapping(const RowAllocation& allocation, int32_t num_cols,
                              arrow::ListBuilder* list_builder, arrow::Int32Builder* value_builder);

    /// Appends __col_0..K-1 values for one row.
    Status AppendColumnValues(const std::shared_ptr<arrow::Array>& values_array,
                              const RowAllocation& allocation,
                              const std::unordered_map<int32_t, int64_t>& field_id_to_value_index,
                              int32_t num_cols,
                              const std::vector<arrow::ArrayBuilder*>& col_builders);

    /// Appends __overflow entries for one row.
    Status AppendOverflow(const std::shared_ptr<arrow::Array>& values_array,
                          const RowAllocation& allocation,
                          const std::unordered_map<int32_t, int64_t>& field_id_to_value_index,
                          arrow::MapBuilder* overflow_builder,
                          arrow::Int32Builder* overflow_key_builder,
                          arrow::ArrayBuilder* overflow_value_builder);

    std::shared_ptr<arrow::Schema> logical_schema_;
    std::shared_ptr<arrow::Schema> physical_schema_;
    std::vector<ColumnContext> contexts_;
    std::vector<int32_t> shredding_indices_;
    std::shared_ptr<arrow::MemoryPool> pool_;
};

}  // namespace paimon
