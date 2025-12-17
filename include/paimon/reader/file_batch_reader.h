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

#include <memory>
#include <utility>
#include <vector>

#include "paimon/predicate/predicate.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/utils/roaring_bitmap32.h"
namespace paimon {
/// The batch reader for a single file supports returning the line number of the last batch read for
/// deletion vector judgment.
class PAIMON_EXPORT FileBatchReader : public BatchReader {
 public:
    /// @return The schema of the file.
    virtual Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const = 0;

    /// Resets the read schema and predicate.
    ///
    /// If `SetReadSchema()` is not called, `NextBatch()` will return data with the file schema.
    /// After resetting the read schema, `NextBatch()` will read data starting from the first row.
    ///
    /// @param read_schema The schema to set for reading.
    /// @param predicate The predicate to apply for filtering data.
    /// @param selection_bitmap The bitmap to apply for filtering data.
    /// @return The status of the operation.
    virtual Status SetReadSchema(::ArrowSchema* read_schema,
                                 const std::shared_ptr<Predicate>& predicate,
                                 const std::optional<RoaringBitmap32>& selection_bitmap) = 0;
    using BatchReader::NextBatch;
    using BatchReader::NextBatchWithBitmap;

    /// Seeks to a specific row in the file.
    /// @param row_number The row number to seek to.
    /// @return The status of the operation.
    virtual Status SeekToRow(uint64_t row_number) = 0;

    /// Get the row number of the first row in the previously read batch.
    virtual uint64_t GetPreviousBatchFirstRowNumber() const = 0;

    /// Get the number of rows in the file.
    virtual uint64_t GetNumberOfRows() const = 0;

    /// Retrieves the row number of the next row to be read.
    /// This method indicates the current read position within the file.
    /// @return The row number of the next row to read.
    virtual uint64_t GetNextRowToRead() const = 0;

    /// Generates a list of row ranges to be read in batches.
    /// Each range specifies the start and end row numbers for a batch,
    /// allowing for efficient batch processing.
    ///
    /// The underlying format layer (e.g., parquet) is responsible for determining
    /// the most effective way to split the data. This could be by row groups, stripes,
    /// or other internal data structures. The key principle is to split the data
    /// into contiguous, seekable ranges to minimize read amplification.
    ///
    /// For example:
    /// - A parquet format could split by RowGroup directly, ensuring each range aligns
    /// with a single RowGroup.
    ///
    /// The smallest splittable unit must be seekable to its start position, and the
    /// splitting strategy should aim to avoid read amplification.
    ///
    /// @param need_prefetch A pointer to a boolean. The format layer sets this to indicate whether
    /// prefetching is beneficial for the current scenario, to avoid performance regression in
    /// certain cases.
    /// @return A vector of pairs, where each pair represents a range with a start and end row
    /// number.
    virtual Result<std::vector<std::pair<uint64_t, uint64_t>>> GenReadRanges(
        bool* need_prefetch) const = 0;

    /// Sets the specific row ranges as a hint to be read from format file.
    ///
    /// If the specific file format does not support explicit range-based reads, implementations may
    /// gracefully ignore this hint and provide an empty (no-op) implementation.
    ///
    /// @param read_ranges A vector of pairs, where each pair defines a half-open interval
    /// `[start_row, end_row)`. The `start_row` is inclusive, and the `end_row` is exclusive.
    virtual Status SetReadRanges(const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) = 0;

    /// Get whether or not support read precisely while bitmap pushed down.
    virtual bool SupportPreciseBitmapSelection() const = 0;
};

}  // namespace paimon
