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

#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/predicate/predicate.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::blob {

/// Binary Blob File Layout Specification
///
/// This file format is designed for the efficient storage of a sequence of 'bins' (data
/// blocks/records) with associated metadata. The structure consists of one or more data bins
/// (bin_0, bin_1, ...), followed by an Index section and a Footer.
///
/// Endianness:
/// - All multi-byte fields (magic number, bin length, crc32, index len, index) use little-endian
/// byte order.
///
/// ====================================================================
/// 1. Data Bins Section
/// ====================================================================
/// The file consists of zero or more contiguous 'bins' (bin_0, bin_1, bin_2, ...); rows whose
/// index entry is negative (see Section 2) have no bin.
/// The structure of each bin is as follows:
///
/// | Field Name        | Length (bytes) | Description                                             |
/// |-------------------|----------------|---------------------------------------------------------|
/// | magic number      | 4              | A fixed number identifying the start of the block.      |
/// | blob content      | bin len - 16   | The actual data payload.                                |
/// | bin length        | 8              | The total length of the entire bin (including metadata).|
/// | bin CRC32         | 4              | The 32-bit Cyclic Redundancy Checksum for the bin.      |
///
/// Note:
/// - Current magic number is 1481511375.
///
/// ====================================================================
/// 2. Index Section
/// ====================================================================
/// The Index is located after all data bins and is used for quick lookup and management.
///
/// Purpose: Records one signed length entry per row, in row order.
///
/// Special lengths: an entry does not always describe a bin present in the Data Bins Section.
/// - -1 (BlobDefs::kNullBinLength): a null blob; no bin is written.
/// - -2 (BlobDefs::kPlaceholderBinLength): a placeholder blob written by a data-evolution
///   partial update for a row it did not touch; no bin is written and the value must be
///   resolved from an older blob file covering the same row.
///
/// Encoding:
/// - Uses Delta Encoding to store differences between successive length values.
/// - Uses Varints (Variable-length Integers) to store long values efficiently.
///
/// ====================================================================
/// 3. File Footer
/// ====================================================================
/// Metadata located at the very end of the file, describing the index and file version.
///
/// | Field Name    | Length (bytes) | Description                                       |
/// |---------------|----------------|---------------------------------------------------|
/// | Index Len     | 4              | The byte length of the preceding Index section.   |
/// | version       | 1              | The file format version number.                   |
///
/// Note:
/// - Current version is 1.
class BlobFileBatchReader : public FileBatchReader {
 public:
    /// `emit_placeholder_sentinel` controls how placeholder entries (bin_length ==
    /// BlobDefs::kPlaceholderBinLength) are read: when false they fail the read, as resolving
    /// them requires the data-evolution blob fallback path; when true they are returned as the
    /// non-null BlobDefs::kPlaceholderSentinel bytes for that path to merge away. Stored values
    /// are returned verbatim; see BlobDefs::kPlaceholderSentinel for the accepted collision
    /// with a user value exactly equal to the sentinel.
    static Result<std::unique_ptr<BlobFileBatchReader>> Create(
        const std::shared_ptr<InputStream>& input_stream, int32_t batch_size,
        bool blob_as_descriptor, bool emit_placeholder_sentinel,
        const std::shared_ptr<MemoryPool>& pool);

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Result<ReadBatch> NextBatch() override;

    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override {
        if (previous_batch_row_count_ == 0) {
            if (previous_batch_start_pos_ == std::numeric_limits<size_t>::max()) {
                return Status::Invalid("No batch has been read yet.");
            } else {
                return Status::Invalid("Last batch was EOF.");
            }
        }
        if (batch_row_id >= previous_batch_row_count_) {
            return Status::Invalid(
                fmt::format("batch_row_id {} is out of range, last batch row count is {}",
                            batch_row_id, previous_batch_row_count_));
        }
        // target_blob_row_indexes_ maps every selected position back to its original file row
        // index, so this stays correct after a bitmap selection removed rows.
        return target_blob_row_indexes_[previous_batch_start_pos_ + batch_row_id];
    }

    Result<uint64_t> GetNumberOfRows() const override {
        return all_blob_lengths_.size();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        closed_ = true;
    }

    bool SupportPreciseBitmapSelection() const override {
        return true;
    }

 private:
    static constexpr uint64_t kDefaultReadChunkSize = 1024 * 1024;

    static int32_t GetIndexLength(const int8_t* bytes, int32_t offset);

    BlobFileBatchReader(const std::shared_ptr<InputStream>& input_stream,
                        const std::string& file_path, const std::vector<int64_t>& blob_lengths,
                        const std::vector<int64_t>& blob_offsets, int32_t batch_size,
                        bool blob_as_descriptor, bool emit_placeholder_sentinel,
                        const std::shared_ptr<MemoryPool>& pool);

    Status ReadBlobContentAt(const int64_t offset, const int64_t length, uint8_t* content) const;

    Result<std::shared_ptr<arrow::Buffer>> NextBlobOffsets(int32_t rows_to_read) const;
    Result<std::shared_ptr<arrow::Buffer>> NextBlobContents(int32_t rows_to_read) const;
    /// Builds a null bitmap buffer for the given rows. Returns nullptr if no nulls.
    Result<std::shared_ptr<arrow::Buffer>> BuildNullBitmap(int32_t rows_to_read) const;
    Result<std::shared_ptr<arrow::Array>> BuildContentArray(int32_t rows_to_read) const;
    Result<std::shared_ptr<arrow::Array>> BuildTargetArray(int32_t rows_to_read) const;

    /// Returns true if the blob at the given index is null (bin_length == kNullBinLength).
    bool IsTargetNull(size_t index) const {
        return target_blob_lengths_[index] == BlobDefs::kNullBinLength;
    }

    bool IsTargetPlaceholder(size_t index) const {
        return target_blob_lengths_[index] == BlobDefs::kPlaceholderBinLength;
    }

    /// Content bytes the blob at the given index contributes to the output buffer: nothing for
    /// a null entry, the sentinel bytes for a placeholder entry (which occupies no file space),
    /// the stored bytes otherwise.
    int64_t GetTargetOutputLength(size_t index) const {
        if (IsTargetNull(index)) {
            return 0;
        }
        if (IsTargetPlaceholder(index)) {
            return BlobDefs::kPlaceholderSentinelLength;
        }
        return GetTargetContentLength(index);
    }

    int64_t GetTargetContentOffset(size_t index) const {
        return target_blob_offsets_[index] + BlobDefs::kContentStartOffset;
    }

    int64_t GetTargetContentLength(size_t index) const {
        return target_blob_lengths_[index] - BlobDefs::kTotalMetaLength;
    }

    std::shared_ptr<InputStream> input_stream_;
    const std::string file_path_;
    const std::vector<int64_t> all_blob_lengths_;
    const std::vector<int64_t> all_blob_offsets_;

    std::vector<int64_t> target_blob_lengths_;
    std::vector<int64_t> target_blob_offsets_;
    std::vector<uint64_t> target_blob_row_indexes_;

    const int32_t batch_size_;
    const bool blob_as_descriptor_;
    const bool emit_placeholder_sentinel_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;

    std::shared_ptr<arrow::DataType> target_type_;
    std::shared_ptr<Metrics> metrics_;

    size_t current_pos_ = 0;
    /// Start position of the previous batch in the (possibly selection-filtered) target index
    /// space; max() until the first batch is read.
    size_t previous_batch_start_pos_ = std::numeric_limits<size_t>::max();
    uint64_t previous_batch_row_count_ = 0;
    bool closed_ = false;
};

}  // namespace paimon::blob
