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

#include "paimon/common/data/abstract_binary_writer.h"

#include <algorithm>
#include <cassert>
#include <memory>
#include <optional>

#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_map.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_section.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/io/byte_order.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

void AbstractBinaryWriter::WriteBytes(int32_t pos, const Bytes& bytes) {
    int32_t len = bytes.size();
    if (len <= BinarySection::MAX_FIX_PART_DATA_SIZE) {
        WriteBytesToFixLenPart(&segment_, GetFieldOffset(pos), bytes, len);
    } else {
        WriteBytesToVarLenPart(pos, bytes, len);
    }
}

void AbstractBinaryWriter::WriteString(int32_t pos, const BinaryString& input) {
    int32_t len = input.GetSizeInBytes();
    if (len <= BinarySection::MAX_FIX_PART_DATA_SIZE) {
        auto bytes = Bytes::AllocateBytes(len, pool_);
        MemorySegmentUtils::CopyToBytes(input.GetSegments(), input.GetOffset(), bytes.get(), 0,
                                        len);
        WriteBytesToFixLenPart(&segment_, GetFieldOffset(pos), *bytes, len);
    } else {
        WriteSegmentsToVarLenPart(pos, input.GetSegments(), input.GetOffset(), len);
    }
}

void AbstractBinaryWriter::WriteBinary(int32_t pos, const Bytes& bytes) {
    int32_t len = bytes.size();
    if (len <= BinarySection::MAX_FIX_PART_DATA_SIZE) {
        WriteBytesToFixLenPart(&segment_, GetFieldOffset(pos), bytes, len);
    } else {
        WriteBytesToVarLenPart(pos, bytes, len);
    }
}

void AbstractBinaryWriter::WriteStringView(int32_t pos, const std::string_view& view) {
    int32_t len = view.size();
    if (len <= BinarySection::MAX_FIX_PART_DATA_SIZE) {
        WriteBytesToFixLenPart(&segment_, GetFieldOffset(pos), view, len);
    } else {
        WriteBytesToVarLenPart(pos, view, len);
    }
}

void AbstractBinaryWriter::WriteRow(int32_t pos, const BinaryRow& input) {
    return WriteSegmentsToVarLenPart(pos, input.GetSegments(), input.GetOffset(),
                                     input.GetSizeInBytes());
}

void AbstractBinaryWriter::WriteArray(int32_t pos, const BinaryArray& input) {
    return WriteSegmentsToVarLenPart(pos, input.GetSegments(), input.GetOffset(),
                                     input.GetSizeInBytes());
}

void AbstractBinaryWriter::WriteMap(int32_t pos, const BinaryMap& input) {
    return WriteSegmentsToVarLenPart(pos, input.GetSegments(), input.GetOffset(),
                                     input.GetSizeInBytes());
}

void AbstractBinaryWriter::WriteDecimal(int32_t pos, const std::optional<Decimal>& value,
                                        int32_t precision) {
    assert(value == std::nullopt || precision == value.value().Precision());
    if (Decimal::IsCompact(precision)) {
        assert(value != std::nullopt);
        WriteLong(pos, value.value().ToUnscaledLong());
    } else {
        // grow the global buffer before writing data.
        EnsureCapacity(16);
        // zero-out 16 bytes
        segment_.PutValue<int64_t>(cursor_, 0ll);
        segment_.PutValue<int64_t>(cursor_ + 8, 0ll);

        // Make sure Decimal object has the same scale as DecimalType.
        // Note that we may pass in null Decimal object to set null for it.
        if (value == std::nullopt) {
            SetNullBit(pos);
            SetOffsetAndSize(pos, cursor_, 0l);
        } else {
            auto bytes = value.value().ToUnscaledBytes();
            segment_.Put(cursor_, bytes, 0, bytes.size());
            SetOffsetAndSize(pos, cursor_, bytes.size());
        }
        // move the cursor forward.
        cursor_ += 16;
    }
}

void AbstractBinaryWriter::WriteTimestamp(int32_t pos, const std::optional<Timestamp>& value,
                                          int32_t precision) {
    if (Timestamp::IsCompact(precision)) {
        assert(value != std::nullopt);
        WriteLong(pos, value.value().GetMillisecond());
    } else {
        // store the nanoOfMillisecond in fixed-length part as offset and nanoOfMillisecond
        EnsureCapacity(8);

        if (value == std::nullopt) {
            SetNullBit(pos);
            // zero-out the bytes
            segment_.PutValue<int64_t>(cursor_, 0l);
            SetOffsetAndSize(pos, cursor_, 0l);
        } else {
            segment_.PutValue<int64_t>(cursor_, value.value().GetMillisecond());
            SetOffsetAndSize(pos, cursor_, value.value().GetNanoOfMillisecond());
        }
        cursor_ += 8;
    }
}

void AbstractBinaryWriter::ZeroOutPaddingBytes(int32_t num_bytes) {
    if ((num_bytes & 0x07) > 0) {
        segment_.PutValue<int64_t>(cursor_ + ((num_bytes >> 3) << 3), 0L);
    }
}

void AbstractBinaryWriter::EnsureCapacity(int32_t needed_size) {
    const int32_t length = cursor_ + needed_size;
    if (segment_.Size() < length) {
        Grow(length);
    }
}

void AbstractBinaryWriter::WriteSegmentsToVarLenPart(int32_t pos,
                                                     const std::vector<MemorySegment>& segments,
                                                     int32_t offset, int32_t size) {
    const int32_t rounded_size = RoundNumberOfBytesToNearestWord(size);
    // grow the global buffer before writing data.
    EnsureCapacity(rounded_size);
    ZeroOutPaddingBytes(size);

    if (segments.size() == 1) {
        segments[0].CopyTo(offset, &segment_, cursor_, size);
    } else {
        WriteMultiSegmentsToVarLenPart(segments, offset, size);
    }
    SetOffsetAndSize(pos, cursor_, size);
    // move the cursor forward.
    cursor_ += rounded_size;
}

void AbstractBinaryWriter::WriteMultiSegmentsToVarLenPart(
    const std::vector<MemorySegment>& segments, int32_t offset, int32_t size) {
    // Write the bytes to the variable length portion.
    int32_t need_copy = size;
    int32_t from_offset = offset;
    int32_t to_offset = cursor_;
    for (const auto& source_segment : segments) {
        int32_t remain = source_segment.Size() - from_offset;
        if (remain > 0) {
            int32_t copy_size = std::min(remain, need_copy);
            source_segment.CopyTo(from_offset, &segment_, to_offset, copy_size);
            need_copy -= copy_size;
            to_offset += copy_size;
            from_offset = 0;
        } else {
            from_offset -= source_segment.Size();
        }
    }
}

template <typename T>
void AbstractBinaryWriter::WriteBytesToVarLenPart(int32_t pos, const T& bytes, int32_t len) {
    const int32_t rounded_size = RoundNumberOfBytesToNearestWord(len);
    // grow the global buffer before writing data.
    EnsureCapacity(rounded_size);
    ZeroOutPaddingBytes(len);
    // Write the bytes to the variable length portion.
    segment_.Put(cursor_, bytes, 0, len);
    SetOffsetAndSize(pos, cursor_, len);
    // move the cursor forward.
    cursor_ += rounded_size;
}

void AbstractBinaryWriter::Grow(int32_t min_capacity) {
    int32_t old_capacity = segment_.Size();
    int32_t new_capacity = old_capacity + (old_capacity >> 1);
    if (new_capacity - min_capacity < 0) {
        new_capacity = min_capacity;
    }
    std::shared_ptr<Bytes> new_bytes = Bytes::CopyOf(*(segment_.GetArray()), new_capacity, pool_);
    segment_ = MemorySegment::Wrap(new_bytes);
    AfterGrow();
}

int32_t AbstractBinaryWriter::RoundNumberOfBytesToNearestWord(int32_t num_bytes) {
    int32_t remainder = num_bytes & 0x07;
    if (remainder == 0) {
        return num_bytes;
    } else {
        return num_bytes + (8 - remainder);
    }
}
template <typename T>
void AbstractBinaryWriter::WriteBytesToFixLenPart(MemorySegment* segment, int32_t field_offset,
                                                  const T& bytes, int32_t len) {
    int64_t first_byte = len | 0x80;  // first bit is 1, other bits is len
    int64_t seven_bytes = 0L;         // real data
    if ((SystemByteOrder() == ByteOrder::PAIMON_LITTLE_ENDIAN)) {
        for (int32_t i = 0; i < len; i++) {
            seven_bytes |= ((0x00000000000000FFL & bytes[i]) << (i * 8L));
        }
    } else {
        for (int32_t i = 0; i < len; i++) {
            seven_bytes |= ((0x00000000000000FFL & bytes[i]) << ((6 - i) * 8L));
        }
    }
    const int64_t offset_and_size =
        (first_byte << 56) |  // NOLINT(clang-analyzer-core.UndefinedBinaryOperatorResult)
        seven_bytes;
    segment->PutValue<int64_t>(field_offset, offset_and_size);
}

}  // namespace paimon
