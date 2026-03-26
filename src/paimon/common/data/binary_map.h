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

#include "paimon/common/data/binary_array.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/common/memory/memory_segment_utils.h"
namespace paimon {
/// A binary implementation of `InternalMap` which is backed by a single `MemorySegment`.
/// Binary layout: [4 byte(keyArray size in bytes)] + [Key BinaryArray] + [Value BinaryArray].
/// `BinaryMap` is influenced by Apache Spark UnsafeMapData.
///
/// @note: Unlike the Java implementation where data may span multiple MemorySegments,
/// in this C++ implementation all data resides within a single MemorySegment.
class BinaryMap : public BinarySection, public InternalMap {
 public:
    BinaryMap() = default;

    int32_t Size() const override {
        return keys_->Size();
    }

    std::shared_ptr<InternalArray> KeyArray() const override {
        return keys_;
    }
    std::shared_ptr<InternalArray> ValueArray() const override {
        return values_;
    }

    void PointTo(const MemorySegment& segment, int32_t offset, int32_t size_in_bytes) override {
        // Read the numBytes of key array from the first 4 bytes.
        auto key_array_bytes = MemorySegmentUtils::GetValue<int32_t>({segment}, offset);
        assert(key_array_bytes >= 0);
        int32_t value_array_bytes = size_in_bytes - key_array_bytes - kHeaderSize;
        assert(value_array_bytes >= 0);

        assert(keys_);
        keys_->PointTo(segment, offset + kHeaderSize, key_array_bytes);
        assert(values_);
        values_->PointTo(segment, offset + kHeaderSize + key_array_bytes, value_array_bytes);

        assert(keys_->Size() == values_->Size());

        segment_ = segment;
        offset_ = offset;
        size_in_bytes_ = size_in_bytes;
    }

    static std::shared_ptr<BinaryMap> ValueOf(const BinaryArray& key, const BinaryArray& value,
                                              MemoryPool* pool) {
        auto bytes = std::make_shared<Bytes>(
            kHeaderSize + key.GetSizeInBytes() + value.GetSizeInBytes(), pool);
        MemorySegment segment = MemorySegment::Wrap(bytes);
        segment.PutValue<int32_t>(0, key.GetSizeInBytes());
        const auto& key_segment = key.GetSegment();
        key_segment.CopyTo(key.GetOffset(), &segment, /*target_offset=*/kHeaderSize,
                           key.GetSizeInBytes());
        const auto& value_segment = value.GetSegment();
        value_segment.CopyTo(value.GetOffset(), &segment,
                             /*target_offset=*/kHeaderSize + key.GetSizeInBytes(),
                             value.GetSizeInBytes());
        auto binary_map = std::make_shared<BinaryMap>();
        binary_map->PointTo(segment, /*offset=*/0, bytes->size());
        return binary_map;
    }

 private:
    static constexpr int32_t kHeaderSize = sizeof(int32_t);

    std::shared_ptr<BinaryArray> keys_ = std::make_shared<BinaryArray>();
    std::shared_ptr<BinaryArray> values_ = std::make_shared<BinaryArray>();
};
}  // namespace paimon
