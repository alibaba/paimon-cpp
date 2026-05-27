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
#include <functional>
#include <memory>

#include "paimon/common/data/abstract_binary_writer.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
class MemoryPool;
/// Writer for `BinaryRow`.
class BinaryRowWriter : public AbstractBinaryWriter {
 public:
    BinaryRowWriter(BinaryRow* row, int32_t initial_size, MemoryPool* pool);

    /// First, call `Reset()` before set/write value.
    void Reset() override {
        cursor_ = fixed_size_;
        for (int32_t i = 0; i < null_bits_size_in_bytes_; i += 8) {
            segment_.PutValue<int64_t>(i, 0L);
        }
    }

    /// Default not null.
    /// @note If type is decimal or timestamp with no compact, use
    /// `WriteTimestamp(pos, null, precision)` to set null rather than `SetNullAt(pos)`
    void SetNullAt(int32_t pos) override {
        SetNullBit(pos);
        segment_.PutValue<int64_t>(GetFieldOffset(pos), 0L);
    }

    void SetNullBit(int32_t pos) override {
        MemorySegmentUtils::BitSet(&segment_, 0, pos + BinaryRow::HEADER_SIZE_IN_BITS);
    }

    void WriteRowKind(const RowKind* kind) {
        segment_.Put(0, kind->ToByteValue());
    }

    void WriteBoolean(int32_t pos, bool value) override {
        segment_.PutValue<bool>(GetFieldOffset(pos), value);
    }

    void WriteByte(int32_t pos, int8_t value) override {
        segment_.PutValue<int8_t>(GetFieldOffset(pos), value);
    }

    void WriteShort(int32_t pos, int16_t value) override {
        segment_.PutValue<int16_t>(GetFieldOffset(pos), value);
    }

    void WriteInt(int32_t pos, int32_t value) override {
        segment_.PutValue<int32_t>(GetFieldOffset(pos), value);
    }

    void WriteLong(int32_t pos, int64_t value) override {
        segment_.PutValue<int64_t>(GetFieldOffset(pos), value);
    }

    void WriteFloat(int32_t pos, float value) override {
        segment_.PutValue<float>(GetFieldOffset(pos), value);
    }

    void WriteDouble(int32_t pos, double value) override {
        segment_.PutValue<double>(GetFieldOffset(pos), value);
    }

    void Complete() override {
        row_->SetTotalSize(cursor_);
    }

    int32_t GetFieldOffset(int32_t pos) const override {
        return null_bits_size_in_bytes_ + 8 * pos;
    }

    void SetOffsetAndSize(int32_t pos, int32_t offset, int64_t size) override {
        const auto offset_and_size = static_cast<int64_t>((static_cast<uint64_t>(offset) << 32) |
                                                          static_cast<uint64_t>(size));
        segment_.PutValue<int64_t>(GetFieldOffset(pos), offset_and_size);
    }

    void AfterGrow() override {
        row_->PointTo(segment_, 0, segment_.Size());
    }

    /// only support non-nested type
    using FieldSetterFunc = std::function<void(const VariantType& field, BinaryRowWriter* writer)>;
    static Result<FieldSetterFunc> CreateFieldSetter(
        int32_t field_idx, const std::shared_ptr<arrow::DataType>& field_type);

 private:
    int32_t null_bits_size_in_bytes_;
    int32_t fixed_size_;
    BinaryRow* row_;
};

}  // namespace paimon
