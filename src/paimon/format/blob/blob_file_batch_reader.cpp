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

#include "paimon/format/blob/blob_file_batch_reader.h"

#include <algorithm>
#include <cstring>
#include <numeric>

#include "arrow/api.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_nested.h"
#include "arrow/c/bridge.h"
#include "arrow/util/bit_util.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/io/offset_input_stream.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/delta_varint_compressor.h"
#include "paimon/common/utils/stream_utils.h"
#include "paimon/data/blob.h"

namespace paimon::blob {

Result<std::unique_ptr<BlobFileBatchReader>> BlobFileBatchReader::Create(
    const std::shared_ptr<InputStream>& input_stream, int32_t batch_size, bool blob_as_descriptor,
    bool emit_placeholder_sentinel, const std::shared_ptr<MemoryPool>& pool) {
    if (input_stream == nullptr) {
        return Status::Invalid("blob file batch reader create failed: input stream is nullptr");
    }
    if (batch_size <= 0) {
        return Status::Invalid(fmt::format(
            "blob file batch reader create failed: read batch size '{}' should be larger than zero",
            batch_size));
    }

    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, input_stream->Length());
    PAIMON_RETURN_NOT_OK(
        input_stream->Seek(file_size - BlobDefs::kBlobFileFooterLength, FS_SEEK_SET));
    int8_t footer[BlobDefs::kBlobFileFooterLength];
    PAIMON_ASSIGN_OR_RAISE(
        int64_t actual_size,
        input_stream->Read(reinterpret_cast<char*>(footer), BlobDefs::kBlobFileFooterLength));
    if (actual_size != BlobDefs::kBlobFileFooterLength) {
        return Status::Invalid(
            fmt::format("actual read size {} not match with expect footer length {}", actual_size,
                        BlobDefs::kBlobFileFooterLength));
    }
    int8_t version = footer[4];
    if (version != BlobDefs::kFileVersion) {
        return Status::Invalid(fmt::format(
            "create blob format reader failed. unsupported blob file version: {}", version));
    }
    int32_t index_length = GetIndexLength(footer, 0);
    PAIMON_RETURN_NOT_OK(input_stream->Seek(
        file_size - BlobDefs::kBlobFileFooterLength - index_length, FS_SEEK_SET));
    std::vector<char> index_bytes(index_length, '\0');
    PAIMON_ASSIGN_OR_RAISE(actual_size, input_stream->Read(index_bytes.data(), index_length));
    if (actual_size != index_length) {
        return Status::Invalid(
            fmt::format("actual read size {} not match with expect index length {}", actual_size,
                        index_length));
    }
    PAIMON_ASSIGN_OR_RAISE(const std::vector<int64_t> blob_lengths,
                           DeltaVarintCompressor::Decompress(index_bytes));

    std::vector<int64_t> blob_offsets;
    blob_offsets.reserve(blob_lengths.size());
    int64_t offset = 0;
    for (const auto& blob_length : blob_lengths) {
        blob_offsets.push_back(offset);
        // null (-1) and placeholder (-2) entries occupy no file space
        if (blob_length >= 0) {
            offset += blob_length;
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::string file_path, input_stream->GetUri());
    auto reader = std::unique_ptr<BlobFileBatchReader>(
        new BlobFileBatchReader(input_stream, file_path, blob_lengths, blob_offsets, batch_size,
                                blob_as_descriptor, emit_placeholder_sentinel, pool));
    return reader;
}

BlobFileBatchReader::BlobFileBatchReader(const std::shared_ptr<InputStream>& input_stream,
                                         const std::string& file_path,
                                         const std::vector<int64_t>& blob_lengths,
                                         const std::vector<int64_t>& blob_offsets,
                                         int32_t batch_size, bool blob_as_descriptor,
                                         bool emit_placeholder_sentinel,
                                         const std::shared_ptr<MemoryPool>& pool)
    : input_stream_(input_stream),
      file_path_(file_path),
      all_blob_lengths_(blob_lengths),
      all_blob_offsets_(blob_offsets),
      target_blob_lengths_(blob_lengths),
      target_blob_offsets_(blob_offsets),
      batch_size_(batch_size),
      blob_as_descriptor_(blob_as_descriptor),
      emit_placeholder_sentinel_(emit_placeholder_sentinel),
      pool_(pool),
      arrow_pool_(GetArrowPool(pool_)),
      metrics_(std::make_shared<MetricsImpl>()) {
    target_blob_row_indexes_.resize(target_blob_lengths_.size());
    std::iota(target_blob_row_indexes_.begin(), target_blob_row_indexes_.end(), 0);
}

Status BlobFileBatchReader::SetReadSchema(::ArrowSchema* read_schema,
                                          const std::shared_ptr<Predicate>& predicate,
                                          const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (!read_schema) {
        return Status::Invalid("SetReadSchema failed: read schema cannot be nullptr");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(read_schema));
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid(
            fmt::format("read schema field number {} is not 1", arrow_schema->num_fields()));
    }
    if (!BlobUtils::IsBlobField(arrow_schema->field(0))) {
        return Status::Invalid(
            fmt::format("field {} is not BLOB", arrow_schema->field(0)->ToString()));
    }
    if (selection_bitmap != std::nullopt) {
        int32_t cardinality = selection_bitmap->Cardinality();
        std::vector<int64_t> new_lengths(cardinality);
        std::vector<int64_t> new_offsets(cardinality);
        std::vector<uint64_t> new_row_indexes(cardinality);

        PAIMON_ASSIGN_OR_RAISE(uint64_t total_rows, GetNumberOfRows());
        RoaringBitmap32::Iterator iterator(*selection_bitmap);
        for (int32_t i = 0; i < cardinality; i++) {
            int32_t row_index = *iterator;
            if (static_cast<size_t>(row_index) >= total_rows) {
                return Status::Invalid(fmt::format(
                    "row index {} is out of bound of total row number {}", row_index, total_rows));
            }
            ++iterator;
            new_lengths[i] = all_blob_lengths_[row_index];
            new_offsets[i] = all_blob_offsets_[row_index];
            new_row_indexes[i] = row_index;
        }
        target_blob_lengths_ = new_lengths;
        target_blob_offsets_ = new_offsets;
        target_blob_row_indexes_ = new_row_indexes;
    }
    target_type_ = arrow::struct_(arrow_schema->fields());
    current_pos_ = 0;
    previous_batch_start_pos_ = std::numeric_limits<size_t>::max();
    previous_batch_row_count_ = 0;
    return Status::OK();
}

Result<std::shared_ptr<arrow::Buffer>> BlobFileBatchReader::NextBlobOffsets(
    int32_t rows_to_read) const {
    arrow::TypedBufferBuilder<int64_t> buffer_builder(arrow_pool_.get());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(buffer_builder.Reserve(rows_to_read + 1));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(buffer_builder.Append(0));
    int64_t data_length = 0;
    for (int32_t k = 0; k < rows_to_read; ++k) {
        data_length += GetTargetOutputLength(current_pos_ + k);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(buffer_builder.Append(data_length));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Buffer> offset_buffer,
                                      buffer_builder.Finish());
    return offset_buffer;
}

Result<std::shared_ptr<arrow::Buffer>> BlobFileBatchReader::NextBlobContents(
    int32_t rows_to_read) const {
    int64_t total_length = 0;
    for (int32_t k = 0; k < rows_to_read; ++k) {
        total_length += GetTargetOutputLength(current_pos_ + k);
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Buffer> data_buffer,
                                      arrow::AllocateBuffer(total_length, arrow_pool_.get()));
    uint8_t* buffer = data_buffer->mutable_data();
    for (int32_t k = 0; k < rows_to_read; ++k) {
        const size_t i = current_pos_ + k;
        if (IsTargetNull(i)) {
            continue;
        }
        if (IsTargetPlaceholder(i)) {
            // a placeholder entry has no data bytes in the file; emit the sentinel for the
            // data-evolution blob fallback merge to identify it
            memcpy(buffer, BlobDefs::kPlaceholderSentinel, BlobDefs::kPlaceholderSentinelLength);
            buffer += BlobDefs::kPlaceholderSentinelLength;
            continue;
        }
        int64_t offset = GetTargetContentOffset(i);
        int64_t length = GetTargetContentLength(i);
        PAIMON_RETURN_NOT_OK(ReadBlobContentAt(offset, length, buffer));
        buffer += length;
    }
    return data_buffer;
}

Result<std::shared_ptr<arrow::Buffer>> BlobFileBatchReader::BuildNullBitmap(
    int32_t rows_to_read) const {
    bool has_null = false;
    for (int32_t k = 0; k < rows_to_read; ++k) {
        if (IsTargetNull(current_pos_ + k)) {
            has_null = true;
            break;
        }
    }
    if (!has_null) {
        return std::shared_ptr<arrow::Buffer>();
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Buffer> null_bitmap,
                                      arrow::AllocateBitmap(rows_to_read, arrow_pool_.get()));
    // Initialize all bits to 1 (valid), then clear bits for null rows
    memset(null_bitmap->mutable_data(), 0xFF, null_bitmap->size());
    for (int32_t k = 0; k < rows_to_read; ++k) {
        if (IsTargetNull(current_pos_ + k)) {
            arrow::bit_util::ClearBit(null_bitmap->mutable_data(), k);
        }
    }
    return null_bitmap;
}

Result<std::shared_ptr<arrow::Array>> BlobFileBatchReader::BuildContentArray(
    int32_t rows_to_read) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> value_offsets,
                           NextBlobOffsets(rows_to_read));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> data, NextBlobContents(rows_to_read));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> child_null_bitmap,
                           BuildNullBitmap(rows_to_read));

    auto large_binary_array = std::make_shared<arrow::LargeBinaryArray>(rows_to_read, value_offsets,
                                                                        data, child_null_bitmap);
    std::vector<std::shared_ptr<arrow::ArrayData>> child_data;
    child_data.emplace_back(large_binary_array->data());
    std::shared_ptr<arrow::ArrayData> struct_array_data =
        arrow::ArrayData::Make(target_type_, large_binary_array->length(), {nullptr}, child_data);
    return std::make_shared<arrow::StructArray>(struct_array_data);
}

Result<std::shared_ptr<arrow::Array>> BlobFileBatchReader::BuildTargetArray(
    int32_t rows_to_read) const {
    std::shared_ptr<arrow::Array> blob_array;
    if (!blob_as_descriptor_) {
        return BuildContentArray(rows_to_read);
    }
    // For descriptor mode, build using StructBuilder to handle nulls properly
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> array_builder,
                                      arrow::MakeBuilder(target_type_, arrow_pool_.get()));
    auto builder = dynamic_cast<arrow::StructBuilder*>(array_builder.get());
    if (builder == nullptr) {
        return Status::Invalid("cast to struct builder failed");
    }
    auto field_builder = dynamic_cast<arrow::LargeBinaryBuilder*>(builder->field_builder(0));
    if (field_builder == nullptr) {
        return Status::Invalid("cast to large binary builder failed");
    }
    for (int32_t k = 0; k < rows_to_read; ++k) {
        const size_t i = current_pos_ + k;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append());
        if (IsTargetNull(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(field_builder->AppendNull());
        } else if (IsTargetPlaceholder(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(field_builder->Append(
                BlobDefs::kPlaceholderSentinel, BlobDefs::kPlaceholderSentinelLength));
        } else {
            int64_t offset = GetTargetContentOffset(i);
            int64_t length = GetTargetContentLength(i);
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Blob> blob,
                                   Blob::FromPath(file_path_, offset, length));
            auto descriptor = blob->ToDescriptor(pool_);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                field_builder->Append(descriptor->data(), descriptor->size()));
        }
    }
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Finish(&array));
    return array;
}

Result<BatchReader::ReadBatch> BlobFileBatchReader::NextBatch() {
    if (closed_) {
        return Status::Invalid("blob file batch reader is closed");
    }
    if (target_type_ == nullptr) {
        return Status::Invalid("target type is nullptr, call SetReadSchema first");
    }
    if (current_pos_ >= target_blob_lengths_.size()) {
        previous_batch_start_pos_ = target_blob_lengths_.size();
        previous_batch_row_count_ = 0;
        return BatchReader::MakeEofBatch();
    }
    int32_t left_rows = target_blob_lengths_.size() - current_pos_;
    int32_t rows_to_read = std::min(left_rows, batch_size_);
    if (!emit_placeholder_sentinel_) {
        for (int32_t k = 0; k < rows_to_read; ++k) {
            if (IsTargetPlaceholder(current_pos_ + k)) {
                return Status::Invalid(fmt::format(
                    "blob file {} contains a placeholder entry (bin_length {}) written by a "
                    "data-evolution partial update; it can only be resolved by the data-evolution "
                    "blob fallback read path",
                    file_path_, BlobDefs::kPlaceholderBinLength));
            }
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> blob_array,
                           BuildTargetArray(rows_to_read));
    std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*blob_array, c_array.get(), c_schema.get()));
    previous_batch_start_pos_ = current_pos_;
    current_pos_ += rows_to_read;
    previous_batch_row_count_ = c_array->length;
    return make_pair(std::move(c_array), std::move(c_schema));
}

Status BlobFileBatchReader::ReadBlobContentAt(const int64_t offset, const int64_t length,
                                              uint8_t* content) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OffsetInputStream> offset_input_stream,
                           OffsetInputStream::Create(input_stream_, length, offset));
    return StreamUtils::ReadAsyncFully(std::move(offset_input_stream),
                                       reinterpret_cast<char*>(content));
}

int32_t BlobFileBatchReader::GetIndexLength(const int8_t* bytes, int32_t offset) {
    return static_cast<int32_t>(
        (static_cast<uint32_t>(static_cast<uint8_t>(bytes[offset + 3])) << 24) |
        (static_cast<uint32_t>(static_cast<uint8_t>(bytes[offset + 2])) << 16) |
        (static_cast<uint32_t>(static_cast<uint8_t>(bytes[offset + 1])) << 8) |
        static_cast<uint32_t>(static_cast<uint8_t>(bytes[offset])));
}

// Note: blob file has no self-describing schema, use read schema instead.
Result<std::unique_ptr<::ArrowSchema>> BlobFileBatchReader::GetFileSchema() const {
    return Status::NotImplemented("blob file has no self-describing file schema");
}

}  // namespace paimon::blob
