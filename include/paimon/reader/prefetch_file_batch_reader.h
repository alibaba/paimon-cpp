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

#include <memory>
#include <utility>
#include <vector>

#include "paimon/reader/file_batch_reader.h"

namespace paimon {

/// The prefetch file batch reader extends the basic FileBatchReader interface for prefetch read,
/// if a format implementation inherits from this class, it will automatically support the C++
/// Paimon prefetch capability and integrate with the Paimon prefetch framework.
class PAIMON_EXPORT PrefetchFileBatchReader : public FileBatchReader {
 public:
    /// Seeks to a specific row in the file.
    /// @param row_number The row number to seek to.
    /// @return The status of the operation.
    virtual Status SeekToRow(uint64_t row_number) = 0;

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

    /// Returns a list of file offset/length ranges that should be prefetched for the current read
    /// scenario.
    ///
    /// This method should analyze the columns selected by the user and return the minimal set of
    /// physical file ranges (offset, length) that need to be read, avoiding unnecessary IO
    /// amplification. For example, if only a subset of columns is requested, the implementation
    /// should only return the byte ranges corresponding to those columns, rather than the entire
    /// row group or block.
    ///
    /// This enables the cache to prefetch only the required data, reducing disk and network load
    /// and improving performance for columnar formats and selective queries.
    ///
    /// By default, returns an empty list (no prefetching). Format-specific implementations should
    /// override this method to provide accurate offset/length hints for efficient IO.
    /// @return A vector of pairs, where each pair contains the file offset and length to be
    /// prefetched.
    virtual Result<std::vector<std::pair<uint64_t, uint64_t>>> PreBufferRange() {
        return std::vector<std::pair<uint64_t, uint64_t>>{};
    }
};

}  // namespace paimon
