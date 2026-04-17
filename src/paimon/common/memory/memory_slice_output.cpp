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

#include "paimon/common/memory/memory_slice_output.h"

#include "fmt/format.h"
#include "paimon/common/utils/math.h"
namespace paimon {

MemorySliceOutput::MemorySliceOutput(int32_t estimated_size, MemoryPool* pool) {
    size_ = 0;
    pool_ = pool;
    segment_ = MemorySegment::Wrap(Bytes::AllocateBytes(estimated_size, pool));
}

int32_t MemorySliceOutput::Size() const {
    return size_;
}

void MemorySliceOutput::Reset() {
    size_ = 0;
}

MemorySlice MemorySliceOutput::ToSlice() {
    return MemorySlice(segment_, 0, size_);
}

template <typename T>
void MemorySliceOutput::WriteValue(T value) {
    int32_t write_length = sizeof(T);
    EnsureSize(size_ + write_length);
    T write_value = value;
    if (NeedSwap()) {
        write_value = EndianSwapValue(value);
    }
    segment_.PutValue(size_, write_value);
    size_ += write_length;
}

Status MemorySliceOutput::WriteVarLenInt(int32_t value) {
    if (value < 0) {
        return Status::Invalid(fmt::format("negative value: v={}", value));
    }
    while ((value & ~0x7F) != 0) {
        WriteValue(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    WriteValue(static_cast<char>(value));
    return Status::OK();
}

Status MemorySliceOutput::WriteVarLenLong(int64_t value) {
    if (value < 0) {
        return Status::Invalid(fmt::format("negative value: v={}", value));
    }
    while ((value & ~0x7F) != 0) {
        WriteValue(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    WriteValue(static_cast<char>(value));
    return Status::OK();
}

void MemorySliceOutput::WriteBytes(const std::shared_ptr<Bytes>& source) {
    WriteBytes(source, 0, source->size());
}

void MemorySliceOutput::WriteBytes(const std::shared_ptr<Bytes>& source, int32_t source_index,
                                   int32_t length) {
    EnsureSize(size_ + length);
    std::string_view sv{source->data(), source->size()};
    segment_.Put(size_, sv, source_index, length);
    size_ += length;
}

void MemorySliceOutput::SetOrder(ByteOrder order) {
    byte_order_ = order;
}

bool MemorySliceOutput::NeedSwap() const {
    return SystemByteOrder() != byte_order_;
}

void MemorySliceOutput::EnsureSize(int32_t size) {
    if (size <= segment_.Size()) {
        return;
    }
    int32_t capacity = segment_.Size();
    int32_t min_capacity = segment_.Size() + size;
    while (capacity < min_capacity) {
        capacity <<= 1;
    }

    auto bytes = std::make_shared<Bytes>(capacity, pool_);
    MemorySegment new_segment = MemorySegment::Wrap(bytes);

    segment_.CopyTo(0, &new_segment, 0, segment_.Size());
    segment_ = new_segment;
}

template void MemorySliceOutput::WriteValue(bool);
template void MemorySliceOutput::WriteValue(char);
template void MemorySliceOutput::WriteValue(int8_t);
template void MemorySliceOutput::WriteValue(int16_t);
template void MemorySliceOutput::WriteValue(int32_t);
template void MemorySliceOutput::WriteValue(int64_t);

}  // namespace paimon
