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

#include "paimon/common/io/data_output_stream.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {

/// A `DeletionVector` based on `RoaringBitmap32`, it only supports files with row count
/// not exceeding `RoaringBitmap32::MAX_VALUE`.
class BitmapDeletionVector : public DeletionVector {
 public:
    static constexpr int32_t MAGIC_NUMBER = 1581511376;
    static constexpr int32_t MAGIC_NUMBER_SIZE_BYTES = 4;

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

    int64_t GetCardinality() const override {
        return roaring_bitmap_.Cardinality();
    }

    Result<int32_t> SerializeTo(const std::shared_ptr<MemoryPool>& pool,
                                DataOutputStream* out) override;

    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeToBytes(
        const std::shared_ptr<MemoryPool>& pool) override;

    Status Merge(const std::shared_ptr<DeletionVector>& deletion_vector) override;

    const RoaringBitmap32* GetBitmap() const {
        return &roaring_bitmap_;
    }

    static Result<PAIMON_UNIQUE_PTR<DeletionVector>> Deserialize(const char* buffer, int32_t length,
                                                                 MemoryPool* pool);

    static Result<PAIMON_UNIQUE_PTR<DeletionVector>> DeserializeWithoutMagicNumber(
        const char* buffer, int32_t length, MemoryPool* pool);

 private:
    Status CheckPosition(int64_t position) const;

    RoaringBitmap32 roaring_bitmap_;
};

}  // namespace paimon
