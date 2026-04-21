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

#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"

#include "arrow/util/crc32.h"
#include "fmt/format.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"

namespace paimon {

Result<int32_t> BitmapDeletionVector::SerializeTo(const std::shared_ptr<MemoryPool>& pool,
                                                  DataOutputStream* out) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> data, SerializeToBytes(pool));
    int64_t size = data->size();
    if (size < 0 || size > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid("BitmapDeletionVector serialize size out of range: ", size);
    }
    PAIMON_RETURN_NOT_OK(out->WriteValue<int32_t>(static_cast<int32_t>(size)));
    PAIMON_RETURN_NOT_OK(out->WriteBytes(data));
    uint32_t crc32 = 0;
    crc32 = arrow::internal::crc32(crc32, data->data(), size);
    PAIMON_RETURN_NOT_OK(out->WriteValue<int32_t>(static_cast<int32_t>(crc32)));
    return static_cast<int32_t>(size);
}

Result<PAIMON_UNIQUE_PTR<Bytes>> BitmapDeletionVector::SerializeToBytes(
    const std::shared_ptr<MemoryPool>& pool) {
    std::shared_ptr<Bytes> bitmap_bytes = roaring_bitmap_.Serialize(pool.get());
    if (bitmap_bytes == nullptr) {
        assert(bitmap_bytes);
        return Status::Invalid("roaring bitmap serialize failed");
    }
    MemorySegmentOutputStream output(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    output.WriteValue<int32_t>(MAGIC_NUMBER);
    output.WriteBytes(bitmap_bytes);
    return MemorySegmentUtils::CopyToBytes(output.Segments(), /*offset=*/0, output.CurrentSize(),
                                           pool.get());
}

Status BitmapDeletionVector::CheckPosition(int64_t position) const {
    if (position > RoaringBitmap32::MAX_VALUE) {
        return Status::Invalid(fmt::format(
            "The file has too many rows, RoaringBitmap32 only supports files with row count "
            "not exceeding {}.",
            RoaringBitmap32::MAX_VALUE));
    }
    return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<DeletionVector>> BitmapDeletionVector::DeserializeWithoutMagicNumber(
    const char* buffer, int32_t length, MemoryPool* pool) {
    RoaringBitmap32 roaring_bitmap;
    PAIMON_RETURN_NOT_OK(roaring_bitmap.Deserialize(buffer, length));
    return pool->AllocateUnique<BitmapDeletionVector>(roaring_bitmap);
}

Result<PAIMON_UNIQUE_PTR<DeletionVector>> BitmapDeletionVector::Deserialize(const char* buffer,
                                                                            int32_t length,
                                                                            MemoryPool* pool) {
    auto in = std::make_shared<ByteArrayInputStream>(buffer, length);
    DataInputStream input(in);
    PAIMON_ASSIGN_OR_RAISE(int32_t magic_num, input.ReadValue<int32_t>());
    if (magic_num != MAGIC_NUMBER) {
        return Status::Invalid(fmt::format(
            "Unable to deserialize deletion vector, invalid magic number: {}", magic_num));
    }
    return DeserializeWithoutMagicNumber(buffer + MAGIC_NUMBER_SIZE_BYTES,
                                         length - MAGIC_NUMBER_SIZE_BYTES, pool);
}

Status BitmapDeletionVector::Merge(const std::shared_ptr<DeletionVector>& deletion_vector) {
    if (!deletion_vector || deletion_vector->IsEmpty()) {
        return Status::OK();
    }
    auto* other = dynamic_cast<BitmapDeletionVector*>(deletion_vector.get());
    if (other != nullptr) {
        roaring_bitmap_ |= other->roaring_bitmap_;
    } else {
        return Status::Invalid(
            "Cannot merge a non-BitmapDeletionVector into a BitmapDeletionVector");
    }
    return Status::OK();
}

}  // namespace paimon
