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

#include <cstdint>
#include <memory>
#include <vector>

#include "paimon/common/memory/memory_segment.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/visibility.h"

namespace paimon {
/// Describe a section of a single MemorySegment.
///
/// @note: Unlike the Java implementation where data may span multiple MemorySegments,
/// in this C++ implementation all data resides within a single MemorySegment.
class PAIMON_EXPORT BinarySection {
 public:
    BinarySection() = default;
    BinarySection(const MemorySegment& segment, int32_t offset, int32_t size_in_bytes)
        : segment_(segment), offset_(offset), size_in_bytes_(size_in_bytes) {}
    virtual ~BinarySection() = default;
    /// It decides whether to put data in FixLenPart or VarLenPart. See more in `/// BinaryRow`.
    /// If len is less than 8, its binary format is: 1-bit mark(1) = 1, 7-bits len, and
    /// 7-bytes data. Data is stored in fix-length part.
    /// If len is greater or equal to 8, its binary format is: 1-bit mark(1) = 0,
    /// 31-bits offset to the data, and 4-bytes length of data. Data is stored in
    /// variable-length part.

    static constexpr int32_t MAX_FIX_PART_DATA_SIZE = 7;
    /// To get the mark in highest bit of int64_t. Form: 10000000 00000000 ... (8 bytes)
    /// This is used to decide whether the data is stored in fixed-length part or
    /// variable-length part. see `MAX_FIX_PART_DATA_SIZE` for more information.

    static constexpr int64_t HIGHEST_FIRST_BIT = 0x80L << 56;
    /// To get the 7 bits length in second bit to eighth bit out of a int64_t. Form:
    /// 01111111 00000000... (8 bytes)
    /// This is used to get the length of the data which is stored in this int64_t. see
    /// `MAX_FIX_PART_DATA_SIZE` for more information.

    static constexpr int64_t HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7FL << 56;

    /// Get binary, if len less than 8, will be include in variable_part_offset_and_len.
    /// @note Need to consider the ByteOrder.
    /// @param base_offset base offset of composite binary format.
    /// @param field_offset absolute start offset of variable_part_offset_and_len.
    /// @param variable_part_offset_and_len a long value, real data or offset and len.
    static PAIMON_UNIQUE_PTR<Bytes> ReadBinary(const MemorySegment& segment, int32_t base_offset,
                                               int32_t field_offset,
                                               int64_t variable_part_offset_and_len,
                                               MemoryPool* pool);

    bool operator==(const BinarySection& that) const;

    virtual void PointTo(const MemorySegment& segment, int32_t offset, int32_t size_in_bytes) {
        segment_ = segment;
        offset_ = offset;
        size_in_bytes_ = size_in_bytes;
    }

    const MemorySegment& GetSegment() const {
        return segment_;
    }

    int32_t GetOffset() const {
        return offset_;
    }

    int32_t GetSizeInBytes() const {
        return size_in_bytes_;
    }

    std::shared_ptr<Bytes> ToBytes(MemoryPool* pool) const;

    virtual int32_t HashCode() const;

 protected:
    MemorySegment segment_;
    int32_t offset_ = 0;
    int32_t size_in_bytes_ = 0;
};

}  // namespace paimon
