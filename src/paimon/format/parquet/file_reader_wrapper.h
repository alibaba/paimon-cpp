/*
 * Copyright 2024-present Alibaba Inc.
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
#include <map>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/compute/api.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/io/caching.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/parquet/row_ranges.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "parquet/arrow/reader.h"
#include "parquet/page_index.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::parquet {

// The FileReaderWrapper is a decorator class designed to support seek functionality, as well as the
// methods GetPreviousBatchFirstRowNumber and GetNextRowToRead.
class FileReaderWrapper {
 public:
    ~FileReaderWrapper();

    static Result<std::unique_ptr<FileReaderWrapper>> Create(
        std::unique_ptr<::parquet::arrow::FileReader>&& reader, ::arrow::MemoryPool* pool,
        int64_t batch_size);

    /// Seek to the specified row number.
    /// @param row_number The row to seek to (must be at a row group boundary).
    Status SeekToRow(uint64_t row_number);

    /// Read the next batch of rows.
    /// @return The next RecordBatch, or nullptr if end of data.
    Result<std::shared_ptr<arrow::RecordBatch>> Next();

    /// Get the first row number of the previously returned batch.
    Result<uint64_t> GetPreviousBatchFirstRowNumber() const {
        return previous_first_row_;
    }

    /// Get the row number that will be read next.
    uint64_t GetNextRowToRead() const {
        return next_row_to_read_;
    }

    /// Get the total number of rows in the file.
    uint64_t GetNumberOfRows() const {
        return num_rows_;
    }

    /// Get the number of row groups in the file.
    int32_t GetNumberOfRowGroups() const {
        return file_reader_->num_row_groups();
    }

    /// Get the underlying Parquet file reader.
    ::parquet::arrow::FileReader* GetFileReader() const {
        return file_reader_.get();
    }

    /// Get the [start, end) ranges for all row groups.
    const std::vector<std::pair<uint64_t, uint64_t>>& GetAllRowGroupRanges() const {
        return all_row_group_ranges_;
    }

    /// Get the Arrow schema of the file.
    Result<std::shared_ptr<arrow::Schema>> GetSchema() const;

    /// Close the batch reader and release resources.
    Status Close();

    /// Get the [start, end) ranges for the specified row groups.
    /// @param row_group_indices The row group indices to get ranges for.
    Result<std::vector<std::pair<uint64_t, uint64_t>>> GetRowGroupRanges(
        const std::set<int32_t>& row_group_indices) const;

    /// Prepare for lazy reading of the specified row groups and columns.
    /// Actual reader initialization is deferred until the first Next() call.
    Status PrepareForReadingLazy(const std::set<int32_t>& row_group_indices,
                                 const std::vector<int32_t>& column_indices);

    /// Prepare for immediate reading of the specified row groups and columns.
    /// Initializes the reader and starts pre-buffering I/O.
    Status PrepareForReading(const std::set<int32_t>& row_group_indices,
                             const std::vector<int32_t>& column_indices);

    /// Filter row groups by read ranges, returning only those that overlap.
    Result<std::set<int32_t>> FilterRowGroupsByReadRanges(
        const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges,
        const std::vector<int32_t>& src_row_groups) const;

    /// Set per-row-group RowRanges for page-level filtering.
    /// Only partially matched row groups should have entries.
    void SetRowGroupRowRanges(const std::map<int32_t, RowRanges>& ranges) {
        row_group_row_ranges_ = ranges;
    }

    /// Get the page index reader for the file.
    /// Returns nullptr if page index is not available.
    std::shared_ptr<::parquet::PageIndexReader> GetPageIndexReader();

    /// Calculate filtered row ranges for a row group based on predicate.
    /// @param row_group_index The row group index.
    /// @param predicate The predicate to evaluate.
    /// @param column_name_to_index Map from column name to column index.
    /// @return RowRanges that may contain matching rows.
    Result<RowRanges> CalculateFilteredRowRanges(
        int32_t row_group_index, const std::shared_ptr<Predicate>& predicate,
        const std::map<std::string, int32_t>& column_name_to_index);

 private:
    FileReaderWrapper(std::unique_ptr<::parquet::arrow::FileReader>&& file_reader,
                      const std::vector<std::pair<uint64_t, uint64_t>>& all_row_group_ranges,
                      uint64_t num_rows, ::arrow::MemoryPool* pool, int64_t batch_size);

    Result<std::set<int32_t>> ReadRangesToRowGroupIds(
        const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) const;
    Result<int32_t> GetRowGroupId(std::pair<uint64_t, uint64_t> target_range) const;

    std::unique_ptr<::parquet::arrow::FileReader> file_reader_;
    std::unique_ptr<arrow::RecordBatchReader> batch_reader_;

    std::vector<std::pair<uint64_t, uint64_t>> all_row_group_ranges_;
    std::set<int32_t> target_row_group_indices_;
    std::vector<std::pair<uint64_t, uint64_t>> target_row_groups_;
    std::vector<int32_t> target_column_indices_;

    ::arrow::MemoryPool* pool_;
    int64_t batch_size_;  // 0 means no limit

    const uint64_t num_rows_;
    uint64_t next_row_to_read_ = std::numeric_limits<uint64_t>::max();
    uint64_t previous_first_row_ = std::numeric_limits<uint64_t>::max();
    uint64_t current_row_group_idx_ = 0;
    bool reader_initialized_ = false;

    // Streaming reader for the currently-active page-filtered row group. Created lazily
    // on the first Next() call into a page-filtered RG, drained batch-by-batch, then reset
    // when ReadNext returns nullptr (end of that RG).
    std::unique_ptr<arrow::RecordBatchReader> current_page_filtered_reader_;
    int64_t filtered_global_offset_ = 0;      // Cumulative filtered-row offset within RG
    RowRanges current_filtered_row_ranges_;   // RowRanges for the active page-filtered RG
    uint64_t current_filtered_rg_start_ = 0;  // Absolute row-group start row number

    // Page-level filtering state. Externally injected via SetRowGroupRowRanges and
    // looked up by row group index when entering a page-filtered RG.
    std::map<int32_t, RowRanges> row_group_row_ranges_;

    // Set of target_row_groups_ positional indices that use page-filtered reading.
    // Built in PrepareForReading from row_group_row_ranges_.
    std::set<uint64_t> page_filtered_indices_;

    // Arrow schema covering target_column_indices_, used when constructing the per-RG
    // page-filtered reader. Cached in PrepareForReading because it's identical across
    // all page-filtered RGs in a session.
    std::shared_ptr<arrow::Schema> page_filtered_read_schema_;

    // Track pre-buffered ranges so we can wait on destruction
    std::vector<::arrow::io::ReadRange> prebuffered_ranges_;

    /// Wait for all pending PreBuffer operations to complete.
    void WaitForPendingPreBuffer();
};

}  // namespace paimon::parquet
