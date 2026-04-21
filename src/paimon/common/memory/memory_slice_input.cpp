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

#include "paimon/common/memory/memory_slice_input.h"

#include "fmt/format.h"
#include "paimon/common/utils/math.h"

namespace paimon {

MemorySliceInput::MemorySliceInput(const MemorySlice& slice) : slice_(slice), position_(0) {}

int32_t MemorySliceInput::Position() const {
    return position_;
}

Status MemorySliceInput::SetPosition(int32_t position) {
    if (position < 0 || position > slice_.Length()) {
        return Status::IndexError(fmt::format("position {} index out of bounds", position));
    }
    position_ = position;
    return Status::OK();
}

bool MemorySliceInput::IsReadable() const {
    return Available() > 0;
}

int32_t MemorySliceInput::Available() const {
    return slice_.Length() - position_;
}

int8_t MemorySliceInput::ReadByte() {
    return slice_.ReadByte(position_++);
}

int8_t MemorySliceInput::ReadUnsignedByte() {
    return static_cast<int8_t>(ReadByte() & 0xFF);
}

int32_t MemorySliceInput::ReadInt() {
    int32_t v = slice_.ReadInt(position_);
    position_ += 4;
    if (NeedSwap()) {
        return EndianSwapValue(v);
    }
    return v;
}

int64_t MemorySliceInput::ReadLong() {
    int64_t v = slice_.ReadLong(position_);
    position_ += 8;
    if (NeedSwap()) {
        return EndianSwapValue(v);
    }
    return v;
}

Result<int32_t> MemorySliceInput::ReadVarLenInt() {
    for (int32_t offset = 0, result = 0; offset < 32; offset += 7) {
        int32_t b = ReadUnsignedByte();
        result |= (b & 0x7F) << offset;
        if ((b & 0x80) == 0) {
            return result;
        }
    }
    return Status::Invalid("Malformed integer.");
}

Result<int64_t> MemorySliceInput::ReadVarLenLong() {
    int64_t result = 0;
    for (int32_t offset = 0; offset < 64; offset += 7) {
        int64_t b = ReadUnsignedByte();
        result |= (b & 0x7F) << offset;
        if ((b & 0x80) == 0) {
            return result;
        }
    }
    return Status::Invalid("Malformed long.");
}

void MemorySliceInput::SetOrder(ByteOrder order) {
    byte_order_ = order;
}

bool MemorySliceInput::NeedSwap() const {
    return SystemByteOrder() != byte_order_;
}

MemorySlice MemorySliceInput::ReadSlice(int32_t length) {
    auto slice = slice_.Slice(position_, length);
    position_ += length;
    return slice;
}

}  // namespace paimon
