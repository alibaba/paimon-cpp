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

#include "paimon/common/data/binary_section.h"

#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/byte_order.h"

namespace paimon {
bool BinarySection::operator==(const BinarySection& that) const {
    if (this == &that) {
        return true;
    }
    return size_in_bytes_ == that.size_in_bytes_ &&
           MemorySegmentUtils::Equals({segment_}, offset_, {that.segment_}, that.offset_,
                                      size_in_bytes_);
}

std::shared_ptr<Bytes> BinarySection::ToBytes(MemoryPool* pool) const {
    return MemorySegmentUtils::GetBytes({segment_}, offset_, size_in_bytes_, pool);
}

int32_t BinarySection::HashCode() const {
    return MemorySegmentUtils::Hash({segment_}, offset_, size_in_bytes_, GetDefaultPool().get());
}

PAIMON_UNIQUE_PTR<Bytes> BinarySection::ReadBinary(const MemorySegment& segment,
                                                   int32_t base_offset, int32_t field_offset,
                                                   int64_t variable_part_offset_and_len,
                                                   MemoryPool* pool) {
    int64_t mark = variable_part_offset_and_len & BinarySection::HIGHEST_FIRST_BIT;
    if (mark == 0) {
        const auto sub_offset = static_cast<int32_t>(variable_part_offset_and_len >> 32);
        const auto len = static_cast<int32_t>(variable_part_offset_and_len);
        return MemorySegmentUtils::CopyToBytes({segment}, base_offset + sub_offset, len, pool);
    } else {
        auto len = static_cast<int32_t>(
            (variable_part_offset_and_len & BinarySection::HIGHEST_SECOND_TO_EIGHTH_BIT) >> 56);
        if ((SystemByteOrder() == ByteOrder::PAIMON_LITTLE_ENDIAN)) {
            return MemorySegmentUtils::CopyToBytes({segment}, field_offset, len, pool);
        } else {
            // field_offset + 1 to skip header.
            return MemorySegmentUtils::CopyToBytes({segment}, field_offset + 1, len, pool);
        }
    }
}

}  // namespace paimon
