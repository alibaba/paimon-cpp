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

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>

#include "fmt/format.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/utils/math.h"
#include "paimon/common/utils/var_length_int_utils.h"
#include "paimon/io/byte_order.h"
#include "paimon/status.h"
#include "paimon/visibility.h"
namespace paimon {
class MemoryPool;

///  Input stream over a MemorySlice with inline hot-path methods.
class PAIMON_EXPORT MemorySliceInput {
 public:
    explicit MemorySliceInput(const MemorySlice& slice)
        : slice_(slice), data_(slice.Data()), length_(slice.Length()) {}

    inline int32_t Position() const {
        return position_;
    }

    inline Status SetPosition(int32_t position) {
        if (position < 0 || position > length_) {
            return Status::IndexError(fmt::format("position {} index out of bounds", position));
        }
        position_ = position;
        return Status::OK();
    }

    inline bool IsReadable() const {
        return position_ < length_;
    }

    inline int32_t Available() const {
        return length_ - position_;
    }

    inline int8_t ReadByte() {
        int8_t value;
        std::memcpy(&value, data_ + position_, 1);
        position_++;
        return value;
    }

    inline int8_t ReadUnsignedByte() {
        return static_cast<int8_t>(static_cast<uint8_t>(data_[position_++]));
    }

    inline int32_t ReadInt() {
        int32_t v;
        std::memcpy(&v, data_ + position_, sizeof(v));
        position_ += 4;
        if (NeedSwap()) {
            return EndianSwapValue(v);
        }
        return v;
    }

    inline int64_t ReadLong() {
        int64_t v;
        std::memcpy(&v, data_ + position_, sizeof(v));
        position_ += 8;
        if (NeedSwap()) {
            return EndianSwapValue(v);
        }
        return v;
    }

    /// Reads a varint32 from the current position.
    inline Result<int32_t> ReadVarLenInt() {
        return VarLengthIntUtils::DecodeInt(data_, &position_);
    }

    /// Reads a varint64 from the current position.
    inline Result<int64_t> ReadVarLenLong() {
        return VarLengthIntUtils::DecodeLong(data_, &position_);
    }

    inline MemorySlice ReadSliceView(int32_t length) {
        auto view_segment = MemorySegment::WrapView(data_ + position_, length);
        position_ += length;
        return MemorySlice(view_segment, 0, length);
    }

    void SetOrder(ByteOrder order) {
        byte_order_ = order;
    }

 private:
    inline bool NeedSwap() const {
        return SystemByteOrder() != byte_order_;
    }

 private:
    MemorySlice slice_;
    const char* data_;  // Cached raw pointer for fast access.
    int32_t length_;    // Cached length.
    int32_t position_ = 0;

    ByteOrder byte_order_ = SystemByteOrder();
};

}  // namespace paimon
