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
#include <vector>

#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/type_fwd.h"

namespace paimon {
/// Utils for `MemorySegment`.
class BinaryDataReadUtils {
 public:
    BinaryDataReadUtils() = delete;
    ~BinaryDataReadUtils() = delete;

    /// Gets an instance of `Timestamp` from underlying `MemorySegment`.
    /// @param segments the underlying `MemorySegment`s
    /// @param base_offset the base offset of current instance of TimestampData
    /// @param offset_and_nanos the offset of milli-seconds part and nanoseconds
    /// @return an instance of `Timestamp`

    static Timestamp ReadTimestampData(const std::vector<MemorySegment>& segments,
                                       int32_t base_offset, int64_t offset_and_nanos) {
        auto nano_of_millisecond = static_cast<int32_t>(offset_and_nanos & LOW_BYTES_MASK);
        auto sub_offset = static_cast<int32_t>(offset_and_nanos >> 32);
        auto millisecond =
            MemorySegmentUtils::GetValue<int64_t>(segments, base_offset + sub_offset);
        return Timestamp::FromEpochMillis(millisecond, nano_of_millisecond);
    }

    /// Gets an instance of `Decimal` from underlying `MemorySegment`.
    static Decimal ReadDecimal(const std::vector<MemorySegment>& segments, int32_t base_offset,
                               int64_t offset_and_size, int32_t precision, int32_t scale) {
        auto size = static_cast<int32_t>(offset_and_size & LOW_BYTES_MASK);
        auto sub_offset = static_cast<int32_t>(offset_and_size >> 32);
        auto bytes = Bytes::AllocateBytes(size, GetDefaultPool().get());
        std::memset(bytes->data(), 0, bytes->size());

        MemorySegmentUtils::CopyToBytes<Bytes>(segments, base_offset + sub_offset, bytes.get(), 0,
                                               size);
        return Decimal::FromUnscaledBytes(precision, scale, bytes.get());
    }

    /// Get binary string, if len less than 8, will be include in variable_part_offset_and_len.
    /// @note Need to consider the ByteOrder.
    /// @param base_offset base offset of composite binary format.
    /// @param field_offset absolute start offset of variable_part_offset_and_len.
    /// @param variable_part_offset_and_len a long value, real data or offset and len.

    static BinaryString ReadBinaryString(const std::vector<MemorySegment>& segments,
                                         int32_t base_offset, int64_t field_offset,
                                         int64_t variable_part_offset_and_len) {
        int64_t mark = variable_part_offset_and_len & BinaryString::HIGHEST_FIRST_BIT;
        if (mark == 0) {
            const auto sub_offset = static_cast<int32_t>(variable_part_offset_and_len >> 32);
            const auto len = static_cast<int32_t>(variable_part_offset_and_len & LOW_BYTES_MASK);
            return BinaryString::FromAddress(segments, base_offset + sub_offset, len);
        } else {
            auto len = static_cast<int32_t>(
                (static_cast<uint64_t>(variable_part_offset_and_len &
                                       BinaryString::HIGHEST_SECOND_TO_EIGHTH_BIT)) >>
                56);
            if (SystemByteOrder() == ByteOrder::PAIMON_LITTLE_ENDIAN) {
                return BinaryString::FromAddress(segments, field_offset, len);
            } else {
                // field_offset + 1 to skip header.
                return BinaryString::FromAddress(segments, field_offset + 1, len);
            }
        }
    }

    static std::shared_ptr<InternalArray> ReadArrayData(const std::vector<MemorySegment>& segments,
                                                        int32_t base_offset,
                                                        int64_t offset_and_size) {
        auto size = static_cast<int32_t>(offset_and_size & LOW_BYTES_MASK);
        auto offset = static_cast<int32_t>(offset_and_size >> 32);
        auto binary_array = std::make_shared<BinaryArray>();
        binary_array->PointTo(segments, offset + base_offset, size);
        return binary_array;
    }

    /// Gets an instance of `InternalRow` from underlying `MemorySegment`.
    static std::shared_ptr<InternalRow> ReadRowData(const std::vector<MemorySegment>& segments,
                                                    int32_t num_fields, int32_t base_offset,
                                                    int64_t offset_and_size) {
        auto size = static_cast<int32_t>(offset_and_size & LOW_BYTES_MASK);
        auto offset = static_cast<int32_t>(offset_and_size >> 32);
        auto row = std::make_shared<BinaryRow>(num_fields);
        row->PointTo(segments, offset + base_offset, size);
        return row;
    }

    static std::shared_ptr<InternalMap> ReadMapData(const std::vector<MemorySegment>& segments,
                                                    int32_t base_offset, int64_t offset_and_size) {
        return nullptr;
    }

 private:
    static constexpr uint64_t LOW_BYTES_MASK = 0xFFFFFFFF;
};
}  // namespace paimon
