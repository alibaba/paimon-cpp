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
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/io/caching.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "paimon/format/parquet/row_ranges.h"
#include "paimon/result.h"
#include "parquet/column_reader.h"
#include "parquet/file_reader.h"
#include "parquet/page_index.h"

namespace parquet::arrow {
class FileReader;
}  // namespace parquet::arrow

namespace paimon::parquet {

/// Reads a single row group using page-level filtering.
/// Non-matching rows are skipped at the decoding level via RecordReader::SkipRecords,
/// using RowRanges computed from the page index (ColumnIndex + OffsetIndex).
/// MakePageFilter is available for future I/O-level page skipping optimization.
class PageFilteredRowGroupReader {
 public:
    PageFilteredRowGroupReader() = delete;
    ~PageFilteredRowGroupReader() = delete;

    /// Read a row group with page-level filtering.
    /// @param parquet_reader The underlying ParquetFileReader
    /// @param arrow_file_reader The Arrow FileReader for fallback nested-column reads
    /// @param target_row_group Target row group with index and row ranges
    /// @param column_indices Leaf column indices to read
    /// @param leaf_to_field_idx Global mapping from parquet leaf column index to arrow field index.
    ///        -1 for non-nested, >=0 for nested (owning field index in file schema).
    /// @param arrow_schema The target Arrow schema for output columns
    /// @param pool Memory pool
    /// @param cache_options Cache options for PreBuffer
    /// @param pre_buffered If true, assumes PreBuffer was already called externally
    ///        and only waits via WhenBuffered (no redundant PreBuffer).
    /// @param page_ranges If non-empty, wait via WhenBufferedRanges instead of WhenBuffered
    /// @param max_chunksize Per-batch row cap for the returned reader.
    /// @return A RecordBatchReader streaming the filtered rows.
    static Result<std::unique_ptr<arrow::RecordBatchReader>> ReadFilteredRowGroup(
        ::parquet::arrow::FileReader* arrow_file_reader, const TargetRowGroup& target_row_group,
        const std::vector<int32_t>& column_indices, const std::vector<int32_t>& leaf_to_field_idx,
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const ::arrow::io::CacheOptions& cache_options, bool pre_buffered,
        const std::vector<::arrow::io::ReadRange>& page_ranges, int64_t max_chunksize,
        std::shared_ptr<::arrow::MemoryPool> pool);

    /// Compute the byte ranges of pages that overlap with the given RowRanges.
    /// Uses OffsetIndex to determine per-page file offsets and sizes.
    /// Includes dictionary pages unconditionally.
    /// Falls back to entire column chunk range if OffsetIndex is unavailable.
    static std::vector<::arrow::io::ReadRange> ComputePageRanges(
        ::parquet::ParquetFileReader* parquet_reader, const TargetRowGroup& target_row_group,
        const std::vector<int32_t>& column_indices);

 private:
    /// Get the [first_row, last_row] range of a page given page locations.
    static std::pair<int64_t, int64_t> GetPageRowRange(
        const std::vector<::parquet::PageLocation>& page_locations, int32_t page_idx,
        int64_t row_group_row_count);

    /// Wait for pre-buffered data to become available before reading.
    static Status WaitForPreBuffer(::parquet::ParquetFileReader* parquet_reader,
                                   int32_t row_group_index,
                                   const std::vector<int32_t>& column_indices,
                                   const ::arrow::io::CacheOptions& cache_options,
                                   bool pre_buffered,
                                   const std::vector<::arrow::io::ReadRange>& page_ranges,
                                   std::shared_ptr<::arrow::MemoryPool> pool);

    /// Execute the skip/read pattern on a RecordReader based on RowRanges.
    static Status ExecuteSkipReadPattern(
        std::shared_ptr<::parquet::internal::RecordReader> record_reader, const RowRanges& ranges,
        int64_t total_row_count, int32_t row_group_index, int32_t column_index);

    /// Create a data_page_filter callback for a column based on RowRanges + OffsetIndex.
    static std::function<bool(const ::parquet::DataPageStats&)> MakePageFilter(
        const RowRanges& row_ranges, const std::shared_ptr<::parquet::OffsetIndex>& offset_index,
        int64_t row_group_row_count);

    /// Read a single column using skip/read pattern driven by RowRanges.
    static Result<std::shared_ptr<arrow::ChunkedArray>> ReadFilteredColumn(
        const std::shared_ptr<::parquet::RowGroupReader>& row_group_reader,
        ::parquet::ParquetFileReader* parquet_reader,
        const std::shared_ptr<::parquet::RowGroupPageIndexReader>& rg_page_index_reader,
        int32_t row_group_index, int32_t column_index, const RowRanges& row_ranges,
        const std::shared_ptr<arrow::Field>& field, int64_t row_group_row_count,
        std::shared_ptr<::arrow::MemoryPool> pool);

    /// Compute compressed RowRanges after data_page_filter skips non-matching pages.
    static std::pair<RowRanges, int64_t> ComputeCompressedRowRanges(
        const RowRanges& original_ranges,
        const std::shared_ptr<::parquet::OffsetIndex>& offset_index, int64_t row_group_row_count);

    /// Build an Int64 array of row indices from RowRanges for use with arrow::compute::Take.
    static Result<std::shared_ptr<arrow::Array>> BuildTakeIndices(
        const RowRanges& row_ranges, int64_t expected_rows,
        std::shared_ptr<::arrow::MemoryPool> pool);

    /// Read all nested columns for a row group via arrow FileReader fallback,
    /// then filter them using Take with the given row_ranges.
    /// Returns a map from field name to filtered ChunkedArray.
    static Result<std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>>>
    ReadNestedColumns(::parquet::arrow::FileReader* arrow_file_reader, int32_t row_group_index,
                      const std::vector<int32_t>& column_indices,
                      const std::vector<int32_t>& leaf_to_field_idx, const RowRanges& row_ranges,
                      int64_t expected_rows, std::shared_ptr<::arrow::MemoryPool> pool);

    /// Assemble output columns by dispatching each arrow field to either
    /// ReadFilteredColumn (non-nested) or the pre-computed nested columns map.
    static Result<std::vector<std::shared_ptr<arrow::ChunkedArray>>> AssembleFilteredColumns(
        const std::shared_ptr<::parquet::RowGroupReader>& row_group_reader,
        ::parquet::ParquetFileReader* parquet_reader,
        const std::shared_ptr<::parquet::RowGroupPageIndexReader>& rg_page_index_reader,
        int32_t row_group_index, const std::vector<int32_t>& column_indices,
        const std::vector<int32_t>& leaf_to_field_idx, const RowRanges& row_ranges,
        int64_t row_group_row_count, int64_t expected_rows,
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>>& nested_columns,
        std::shared_ptr<::arrow::MemoryPool> pool);
};

}  // namespace paimon::parquet
