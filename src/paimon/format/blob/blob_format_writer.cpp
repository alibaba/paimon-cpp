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

#include "paimon/format/blob/blob_format_writer.h"

#include <algorithm>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/delta_varint_compressor.h"
#include "paimon/data/blob.h"
#include "paimon/io/byte_array_input_stream.h"

namespace paimon::blob {

BlobFormatWriter::BlobFormatWriter(bool blob_as_descriptor,
                                   const std::shared_ptr<OutputStream>& out,
                                   const std::shared_ptr<arrow::DataType>& data_type,
                                   const std::shared_ptr<FileSystem>& fs,
                                   const std::shared_ptr<MemoryPool>& pool)
    : blob_as_descriptor_(blob_as_descriptor),
      out_(out),
      data_type_(data_type),
      fs_(fs),
      pool_(pool) {
    metrics_ = std::make_shared<MetricsImpl>();
    tmp_buffer_ = Bytes::AllocateBytes(TMP_BUFFER_SIZE, pool_.get());
}

Result<std::unique_ptr<BlobFormatWriter>> BlobFormatWriter::Create(
    bool blob_as_descriptor, const std::shared_ptr<OutputStream>& out,
    const std::shared_ptr<arrow::DataType>& data_type, const std::shared_ptr<FileSystem>& fs,
    const std::shared_ptr<MemoryPool>& pool) {
    if (out == nullptr) {
        return Status::Invalid("blob format writer create failed. out is nullptr");
    }
    if (data_type == nullptr) {
        return Status::Invalid("blob format writer create failed. data_type is nullptr");
    }
    if (pool == nullptr) {
        return Status::Invalid("blob format writer create failed. pool is nullptr");
    }
    if (data_type->num_fields() != 1) {
        return Status::Invalid(
            fmt::format("blob data type field number {} is not 1", data_type->num_fields()));
    }
    if (!BlobUtils::IsBlobField(data_type->field(0))) {
        return Status::Invalid(
            fmt::format("field {} is not BLOB", data_type->field(0)->ToString()));
    }
    return std::unique_ptr<BlobFormatWriter>(
        new BlobFormatWriter(blob_as_descriptor, out, data_type, fs, pool));
}

Status BlobFormatWriter::AddBatch(ArrowArray* batch) {
    if (batch == nullptr) {
        return Status::Invalid("blob format writer add batch failed. batch is nullptr");
    }
    if (batch->length != 1) {
        return Status::Invalid("BlobFormatWriter only supports batch with a row count of 1");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(batch, data_type_));

    assert(arrow_array->num_fields() == 1);
    auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(arrow_array);
    auto child_array = struct_array->field(0);
    if (arrow_array->null_count() != 0 || child_array->null_count() != 0) {
        return Status::Invalid("BlobFormatWriter only support non-null blob.");
    }
    if (child_array->type_id() != arrow::Type::type::LARGE_BINARY) {
        return Status::Invalid("BlobFormatWriter only support large binary type.");
    }

    const auto& blob_array =
        arrow::internal::checked_cast<const arrow::LargeBinaryArray&>(*child_array);
    assert(blob_array.length() == 1);
    PAIMON_RETURN_NOT_OK(WriteBlob(blob_array.GetView(0)));

    PAIMON_RETURN_NOT_OK(Flush());
    return Status::OK();
}

Status BlobFormatWriter::Flush() {
    return out_->Flush();
}

Status BlobFormatWriter::Finish() {
    // index
    const auto& index_bytes = DeltaVarintCompressor::Compress(bin_lengths_);
    PAIMON_RETURN_NOT_OK(WriteBytes(index_bytes.data(), index_bytes.size()));
    // header
    PAIMON_UNIQUE_PTR<Bytes> index_length_bytes =
        IntegerToLittleEndian<int32_t>(static_cast<int32_t>(index_bytes.size()), pool_);
    PAIMON_RETURN_NOT_OK(WriteBytes(index_length_bytes->data(), index_length_bytes->size()));
    PAIMON_RETURN_NOT_OK(WriteBytes(reinterpret_cast<const char*>(&VERSION), sizeof(VERSION)));

    PAIMON_RETURN_NOT_OK(Flush());

    tmp_buffer_.reset();
    return Status::OK();
}

Status BlobFormatWriter::WriteBlob(std::string_view blob_data) {
    crc32_ = 0;
    PAIMON_ASSIGN_OR_RAISE(int64_t previous_pos, out_->GetPos());

    // write magic number
    static PAIMON_UNIQUE_PTR<Bytes> MAGIC_NUMBER_BYTES =
        IntegerToLittleEndian<int32_t>(MAGIC_NUMBER, pool_);
    PAIMON_RETURN_NOT_OK(WriteWithCrc32(MAGIC_NUMBER_BYTES->data(), MAGIC_NUMBER_BYTES->size()));

    // write blob content
    std::unique_ptr<InputStream> in;
    if (blob_as_descriptor_) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Blob> blob,
                               Blob::FromDescriptor(blob_data.data(), blob_data.size()));
        PAIMON_ASSIGN_OR_RAISE(in, blob->NewInputStream(fs_));
    } else {
        in = std::make_unique<ByteArrayInputStream>(blob_data.data(), blob_data.size());
    }
    PAIMON_ASSIGN_OR_RAISE(uint64_t file_length, in->Length());
    uint64_t total_read_length = 0;
    uint32_t read_len = std::min(file_length, tmp_buffer_->size());
    while (read_len > 0) {
        PAIMON_ASSIGN_OR_RAISE(int32_t actual_read_len, in->Read(tmp_buffer_->data(), read_len));
        if (static_cast<uint32_t>(actual_read_len) != read_len) {
            return Status::Invalid("actual read length {}, not match with expect length {}",
                                   actual_read_len, read_len);
        }
        PAIMON_RETURN_NOT_OK(WriteWithCrc32(tmp_buffer_->data(), actual_read_len));
        total_read_length += actual_read_len;
        read_len = std::min(file_length - total_read_length, tmp_buffer_->size());
    }

    // write bin length
    PAIMON_ASSIGN_OR_RAISE(int64_t current_pos, out_->GetPos());
    /// magic number(4) + blob content(bin length - 16) + bin length(8) + crc32(4)
    /// ↑                                             ↑
    /// previous_pos                               current_pos
    int64_t bin_length = current_pos - previous_pos + 8 + 4;
    bin_lengths_.push_back(bin_length);
    PAIMON_UNIQUE_PTR<Bytes> bin_length_bytes = IntegerToLittleEndian<int64_t>(bin_length, pool_);
    PAIMON_RETURN_NOT_OK(WriteWithCrc32(bin_length_bytes->data(), bin_length_bytes->size()));

    // write crc32
    PAIMON_UNIQUE_PTR<Bytes> crc32_bytes = IntegerToLittleEndian<int32_t>(crc32_, pool_);
    PAIMON_RETURN_NOT_OK(WriteBytes(crc32_bytes->data(), crc32_bytes->size()));

    return Status::OK();
}

Status BlobFormatWriter::WriteBytes(const char* data, int32_t length) {
    PAIMON_ASSIGN_OR_RAISE(int32_t actual, out_->Write(data, length));
    if (actual != length) {
        return Status::Invalid("not suppose actual length {} not match with expect {}", actual,
                               length);
    }
    return Status::OK();
}

Status BlobFormatWriter::WriteWithCrc32(const char* data, int32_t length) {
    crc32_ = arrow::internal::crc32(crc32_, data, length);
    return WriteBytes(data, length);
}

Result<bool> BlobFormatWriter::ReachTargetSize(bool suggested_check, int64_t target_size) const {
    PAIMON_ASSIGN_OR_RAISE(int64_t current_pos, out_->GetPos());
    return current_pos >= target_size;
}

template <typename T>
PAIMON_UNIQUE_PTR<Bytes> BlobFormatWriter::IntegerToLittleEndian(
    T value, const std::shared_ptr<MemoryPool>& pool) {
    static_assert(std::is_integral_v<T>, "IntegerToLittleEndian() only supports integral types.");
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
    out.WriteValue<T>(value);
    return MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
}

}  // namespace paimon::blob
