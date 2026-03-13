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

#include "paimon/core/deletionvectors/deletion_vector.h"

#include <cstddef>
#include <string>

#include "fmt/format.h"
#include "paimon/core/deletionvectors/bitmap64_deletion_vector.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

Result<PAIMON_UNIQUE_PTR<DeletionVector>> DeletionVector::DeserializeFromBytes(const Bytes* bytes,
                                                                               MemoryPool* pool) {
    return BitmapDeletionVector::Deserialize(bytes->data(), bytes->size(), pool);
}

Result<PAIMON_UNIQUE_PTR<DeletionVector>> DeletionVector::Read(const FileSystem* file_system,
                                                               const DeletionFile& deletion_file,
                                                               MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input,
                           file_system->Open(deletion_file.path));
    DataInputStream file_input_stream(input);
    PAIMON_RETURN_NOT_OK(file_input_stream.Seek(deletion_file.offset));
    PAIMON_ASSIGN_OR_RAISE(int32_t actual_length, file_input_stream.ReadValue<int32_t>());
    if (actual_length != deletion_file.length) {
        return Status::Invalid(
            fmt::format("Size not match, actual size: {}, expect size: {}, file path: {}",
                        actual_length, deletion_file.length, deletion_file.path));
    }
    auto bytes = Bytes::AllocateBytes(deletion_file.length, pool);
    PAIMON_RETURN_NOT_OK(file_input_stream.ReadBytes(bytes.get()));
    return DeserializeFromBytes(bytes.get(), pool);
}

Result<PAIMON_UNIQUE_PTR<DeletionVector>> DeletionVector::Read(DataInputStream* input_stream,
                                                               std::optional<int64_t> length,
                                                               MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(int32_t bitmap_length, input_stream->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t magic_number, input_stream->ReadValue<int32_t>());

    if (magic_number == BitmapDeletionVector::MAGIC_NUMBER) {
        if (length.has_value() && bitmap_length != length.value()) {
            return Status::Invalid(fmt::format("Size not match, actual size: {}, expected size: {}",
                                               bitmap_length, length.value()));
        }

        int32_t payload_length = bitmap_length - BitmapDeletionVector::MAGIC_NUMBER_SIZE_BYTES;
        if (payload_length < 0) {
            return Status::Invalid(fmt::format("Invalid bitmap length: {}", bitmap_length));
        }

        auto bytes = Bytes::AllocateBytes(payload_length, pool);
        PAIMON_RETURN_NOT_OK(input_stream->ReadBytes(bytes.get()));
        // skip crc (4 bytes)
        PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] int32_t unused_crc,
                               input_stream->ReadValue<int32_t>());

        return BitmapDeletionVector::DeserializeWithoutMagicNumber(bytes->data(), bytes->size(),
                                                                   pool);
    } else if (EndianSwapValue(magic_number) == Bitmap64DeletionVector::MAGIC_NUMBER) {
        return Status::NotImplemented(
            "bitmap64 deletion vectors are not supported in this version, "
            "please use bitmap deletion vectors instead or upgrade to a version "
            "that supports bitmap64.");
    }

    return Status::Invalid(fmt::format(
        "Invalid magic number: {}, v1 dv magic number: {}, v2 magic number: {}", magic_number,
        BitmapDeletionVector::MAGIC_NUMBER, Bitmap64DeletionVector::MAGIC_NUMBER));
}

PAIMON_UNIQUE_PTR<DeletionVector> DeletionVector::FromPrimitiveArray(
    const std::vector<char>& is_deleted, MemoryPool* pool) {
    RoaringBitmap32 roaring;
    for (size_t i = 0; i < is_deleted.size(); i++) {
        if (is_deleted[i] == static_cast<char>(1)) {
            roaring.Add(i);
        }
    }
    return pool->AllocateUnique<BitmapDeletionVector>(roaring);
}

}  // namespace paimon
