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
    /// @param row_group_index Row group to read
    /// @param row_ranges Matching row ranges within this row group
    /// @param column_indices Leaf column indices to read
    /// @param arrow_schema The target Arrow schema for output columns
    /// @param pool Memory pool
    /// @param cache_options Cache options for PreBuffer
    /// @param pre_buffered If true, assumes PreBuffer was already called externally
    ///        and only waits via WhenBuffered (no redundant PreBuffer).
    /// @param page_ranges If non-empty, wait via WhenBufferedRanges instead of WhenBuffered
    /// @param max_chunksize Per-batch row cap for the returned reader, mirroring Arrow's
    ///        TableBatchReader::set_chunksize. Each batch yields at most this many rows;
    ///        actual size may be smaller when an underlying ChunkedArray's chunk boundary
    ///        is reached first (zero-copy slice).
    /// @return A RecordBatchReader streaming the filtered rows. Multi-chunk variable-length
    ///         columns are emitted as multiple zero-copy-sliced batches along chunk boundaries
    ///         instead of being concatenated, avoiding the deep copy of CombineChunks.
    static Result<std::unique_ptr<arrow::RecordBatchReader>> ReadFilteredRowGroup(
        ::parquet::ParquetFileReader* parquet_reader, int32_t row_group_index,
        const RowRanges& row_ranges, const std::vector<int32_t>& column_indices,
        const std::shared_ptr<arrow::Schema>& arrow_schema, ::arrow::MemoryPool* pool,
        const ::arrow::io::CacheOptions& cache_options = ::arrow::io::CacheOptions::Defaults(),
        bool pre_buffered = false, const std::vector<::arrow::io::ReadRange>& page_ranges = {},
        int64_t max_chunksize = std::numeric_limits<int64_t>::max());

    /// Compute the byte ranges of pages that overlap with the given RowRanges.
    /// Uses OffsetIndex to determine per-page file offsets and sizes.
    /// Includes dictionary pages unconditionally.
    /// Falls back to entire column chunk range if OffsetIndex is unavailable.
    static std::vector<::arrow::io::ReadRange> ComputePageRanges(
        ::parquet::ParquetFileReader* parquet_reader, int32_t row_group_index,
        const RowRanges& row_ranges, const std::vector<int32_t>& column_indices);

 private:
    /// Create a data_page_filter callback for a column based on RowRanges + OffsetIndex.
    /// Returns true (skip) if the page's row range has no overlap with RowRanges.
    static std::function<bool(const ::parquet::DataPageStats&)> MakePageFilter(
        const RowRanges& row_ranges, const std::shared_ptr<::parquet::OffsetIndex>& offset_index,
        int64_t row_group_row_count);

    /// Read a single column using skip/read pattern driven by RowRanges.
    /// When OffsetIndex is available, uses data_page_filter for I/O-level page skipping
    /// and compressed RowRanges for decode-level row skipping.
    static Result<std::shared_ptr<arrow::ChunkedArray>> ReadFilteredColumn(
        const std::shared_ptr<::parquet::RowGroupReader>& row_group_reader,
        ::parquet::ParquetFileReader* parquet_reader,
        const std::shared_ptr<::parquet::PageIndexReader>& page_index_reader,
        int32_t row_group_index, int32_t column_index, const RowRanges& row_ranges,
        const std::shared_ptr<arrow::Field>& field, int64_t row_group_row_count,
        ::arrow::MemoryPool* pool);

    /// Compute compressed RowRanges after data_page_filter skips non-matching pages.
    /// Maps original RowRanges to the compressed row space where skipped pages are removed.
    /// @return pair of (compressed RowRanges, compressed total row count)
    static std::pair<RowRanges, int64_t> ComputeCompressedRowRanges(
        const RowRanges& original_ranges,
        const std::shared_ptr<::parquet::OffsetIndex>& offset_index, int64_t row_group_row_count);
};

}  // namespace paimon::parquet
