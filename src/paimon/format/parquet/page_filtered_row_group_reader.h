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
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "arrow/io/caching.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "paimon/format/parquet/row_ranges.h"
#include "paimon/format/parquet/target_row_group.h"
#include "paimon/result.h"
#include "parquet/arrow/reader.h"
#include "parquet/column_reader.h"
#include "parquet/file_reader.h"
#include "parquet/page_index.h"

namespace paimon::parquet {

/// Reads a single row group using page-level filtering.
/// Non-matching rows are skipped at the decoding level via RecordReader::SkipRecords,
/// using RowRanges computed from the page index (ColumnIndex + OffsetIndex).
class PageFilteredRowGroupReader {
 public:
    PageFilteredRowGroupReader() = delete;
    ~PageFilteredRowGroupReader() = delete;

    /// Read a row group with page-level filtering.
    /// @param target_row_group Target row group with index and row ranges
    /// @param column_indices Leaf column indices to read
    /// @param pool Memory pool
    /// @param cache_options Cache options for PreBuffer
    /// @param pre_buffered If true, assumes PreBuffer was already called externally
    ///        and only waits via WhenBuffered (no redundant PreBuffer).
    /// @param page_ranges If non-empty, wait via WhenBufferedRanges instead of WhenBuffered
    /// @param max_chunksize Per-batch row cap for the returned reader.
    /// @param arrow_file_reader The Arrow FileReader for ColumnReader tree creation
    /// @return A RecordBatchReader streaming the filtered rows.
    static Result<std::unique_ptr<arrow::RecordBatchReader>> ReadFilteredRowGroup(
        const TargetRowGroup& target_row_group, const std::vector<int32_t>& column_indices,
        const ::arrow::io::CacheOptions& cache_options, bool pre_buffered,
        const std::vector<::arrow::io::ReadRange>& page_ranges, int64_t max_chunksize,
        std::shared_ptr<::arrow::MemoryPool> pool, ::parquet::arrow::FileReader* arrow_file_reader);

    /// Compute the byte ranges of pages that overlap with the given RowRanges.
    /// Uses OffsetIndex to determine per-page file offsets and sizes.
    /// Includes dictionary pages unconditionally.
    /// Falls back to entire column chunk range if OffsetIndex is unavailable.
    static std::vector<::arrow::io::ReadRange> ComputePageRanges(
        const TargetRowGroup& target_row_group, const std::vector<int32_t>& column_indices,
        ::parquet::ParquetFileReader* parquet_reader);

 private:
    struct DataPageReadPlan {
        int64_t first_data_page_offset;
        std::vector<::parquet::DataPageReadPlanEntry> data_pages;
    };

    /// Get the [first_row, last_row] range of a page given page locations.
    static std::pair<int64_t, int64_t> GetPageRowRange(
        const std::vector<::parquet::PageLocation>& page_locations, int32_t page_idx,
        int64_t row_group_row_count);

    /// Wait for pre-buffered data to become available before reading.
    static Status WaitForPreBuffer(int32_t row_group_index,
                                   const std::vector<int32_t>& column_indices,
                                   const ::arrow::io::CacheOptions& cache_options,
                                   bool pre_buffered,
                                   const std::vector<::arrow::io::ReadRange>& page_ranges,
                                   std::shared_ptr<::arrow::MemoryPool> pool,
                                   ::parquet::ParquetFileReader* parquet_reader);

    /// Build a direct data page read plan for a column based on RowRanges + OffsetIndex.
    /// The returned first data page offset and all page offsets are relative to the
    /// beginning of the column chunk stream used by Arrow's PageReader.
    static std::optional<DataPageReadPlan> MakeDataPageReadPlan(
        const RowRanges& row_ranges, const std::shared_ptr<::parquet::OffsetIndex>& offset_index,
        const ::parquet::ColumnChunkMetaData& column_chunk, int64_t row_group_row_count);

    /// Compute compressed RowRanges after the direct read plan skips non-matching pages.
    static std::pair<RowRanges, int64_t> ComputeCompressedRowRanges(
        const RowRanges& original_ranges,
        const std::shared_ptr<::parquet::OffsetIndex>& offset_index, int64_t row_group_row_count);

    /// Reset the given leaf and replay the skip/read pattern derived from `ranges`
    /// directly against the ColumnReader (ResetLeaf + SkipRecords/ReadRecords).
    static Status ExecuteSkipReadPattern(int col_idx, const RowRanges& ranges, int64_t total,
                                         ::parquet::arrow::ColumnReader* column_reader);

    /// Read a field (flat or nested) using ColumnReader tree.
    /// Sets a direct page read plan on all leaves via factory, then drives each leaf
    /// independently via ResetLeaf/SkipRecords/ReadRecords using its own
    /// compressed_ranges.
    static Result<std::shared_ptr<arrow::ChunkedArray>> ReadFilteredField(
        const std::shared_ptr<::parquet::RowGroupPageIndexReader>& rg_page_index_reader,
        int32_t row_group_index, int32_t field_index, const std::vector<int32_t>& column_indices,
        const RowRanges& row_ranges, int64_t row_group_row_count,
        ::parquet::arrow::FileReader* arrow_file_reader);
};

}  // namespace paimon::parquet
