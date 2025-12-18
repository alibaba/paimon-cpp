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

#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/utils/roaring_bitmap32.h"
namespace paimon {
/// A `DeletionVector` based on `RoaringBitmap32`, it only supports files with row count
/// not exceeding `RoaringBitmap32::MAX_VALUE`.
class BitmapDeletionVector : public DeletionVector {
 public:
    explicit BitmapDeletionVector(const RoaringBitmap32& roaring_bitmap)
        : roaring_bitmap_(roaring_bitmap) {}

    Status Delete(int64_t position) override {
        PAIMON_RETURN_NOT_OK(CheckPosition(position));
        roaring_bitmap_.Add(static_cast<int32_t>(position));
        return Status::OK();
    }

    Result<bool> CheckedDelete(int64_t position) override {
        PAIMON_RETURN_NOT_OK(CheckPosition(position));
        return roaring_bitmap_.CheckedAdd(static_cast<int32_t>(position));
    }

    Result<bool> IsDeleted(int64_t position) const override {
        PAIMON_RETURN_NOT_OK(CheckPosition(position));
        return roaring_bitmap_.Contains(static_cast<int32_t>(position));
    }

    bool IsEmpty() const override {
        return roaring_bitmap_.IsEmpty();
    }

    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeToBytes(
        const std::shared_ptr<MemoryPool>& pool) override {
        std::shared_ptr<Bytes> bitmap_bytes = roaring_bitmap_.Serialize(pool.get());
        if (bitmap_bytes == nullptr) {
            assert(bitmap_bytes);
            return Status::Invalid("roaring bitmap serialize failed");
        }
        MemorySegmentOutputStream output(/*segment_size=*/1024, pool);
        output.WriteValue<int32_t>(MAGIC_NUMBER);
        output.WriteBytes(bitmap_bytes);
        return MemorySegmentUtils::CopyToBytes(output.Segments(), /*offset=*/0,
                                               output.CurrentSize(), pool.get());
    }

    const RoaringBitmap32* GetBitmap() const {
        return &roaring_bitmap_;
    }

    static Result<PAIMON_UNIQUE_PTR<DeletionVector>> Deserialize(const char* buffer, int32_t length,
                                                                 MemoryPool* pool) {
        auto in = std::make_shared<ByteArrayInputStream>(buffer, length);
        DataInputStream input(in);
        PAIMON_ASSIGN_OR_RAISE(int32_t magic_num, input.ReadValue<int32_t>());
        if (magic_num != MAGIC_NUMBER) {
            return Status::Invalid("Invalid magic number: ", std::to_string(magic_num));
        }
        RoaringBitmap32 roaring_bitmap;
        PAIMON_RETURN_NOT_OK(roaring_bitmap.Deserialize(buffer + sizeof(MAGIC_NUMBER),
                                                        length - sizeof(MAGIC_NUMBER)));
        return pool->AllocateUnique<BitmapDeletionVector>(roaring_bitmap);
    }

    static constexpr int32_t MAGIC_NUMBER = 1581511376;

 private:
    Status CheckPosition(int64_t position) const {
        if (position > RoaringBitmap32::MAX_VALUE) {
            return Status::Invalid(
                "The file has too many rows, RoaringBitmap32 only supports files with row count "
                "not exceeding 2147483647.");
        }
        return Status::OK();
    }

 private:
    RoaringBitmap32 roaring_bitmap_;
};
}  // namespace paimon
