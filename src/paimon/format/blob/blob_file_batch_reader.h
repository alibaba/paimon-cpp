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
/// The file consists of one or more contiguous 'bins' (bin_0, bin_1, bin_2, ...).
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
/// Purpose: Records the lengths (record lens) of all data bins.
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
    static constexpr uint32_t kBlobFileHeaderLength = 5;

    static Result<std::unique_ptr<BlobFileBatchReader>> Create(
        const std::shared_ptr<InputStream>& input_stream, int32_t batch_size,
        bool blob_as_descriptor, const std::shared_ptr<MemoryPool>& pool);

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Result<ReadBatch> NextBatch() override;

    uint64_t GetPreviousBatchFirstRowNumber() const override {
        return previous_batch_first_row_number_;
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
    static constexpr int32_t kBlobContentStartOffset = 4;
    static constexpr int32_t kBlobTotalMetaLength = 16;
    static constexpr uint64_t kDefaultReadChunkSize = 1024 * 1024;

    static int32_t GetIndexLength(const int8_t* bytes, int32_t offset);

    BlobFileBatchReader(const std::shared_ptr<InputStream>& input_stream,
                        const std::string& file_path, const std::vector<int64_t>& blob_lengths,
                        const std::vector<int64_t>& blob_offsets, int32_t batch_size,
                        bool blob_as_descriptor, const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<arrow::Array>> ToArrowArray(
        const std::vector<PAIMON_UNIQUE_PTR<Bytes>>& blobs) const;

    Status ReadBlobContentAt(const int64_t offset, const int64_t length, uint8_t* content) const;

    Result<std::shared_ptr<arrow::Buffer>> NextBlobOffsets(int32_t rows_to_read) const;
    Result<std::shared_ptr<arrow::Buffer>> NextBlobContents(int32_t rows_to_read) const;
    Result<std::shared_ptr<arrow::Array>> BuildContentArray(int32_t rows_to_read) const;
    Result<std::shared_ptr<arrow::Array>> BuildTargetArray(int32_t rows_to_read) const;

    int64_t GetTargetContentOffset(size_t index) const {
        return target_blob_offsets_[index] + kBlobContentStartOffset;
    }

    int64_t GetTargetContentLength(size_t index) const {
        return target_blob_lengths_[index] - kBlobTotalMetaLength;
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
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;

    std::shared_ptr<arrow::DataType> target_type_;
    std::shared_ptr<Metrics> metrics_;

    size_t current_pos_ = 0;
    uint64_t previous_batch_first_row_number_ = std::numeric_limits<uint64_t>::max();
    bool closed_ = false;
};

}  // namespace paimon::blob
