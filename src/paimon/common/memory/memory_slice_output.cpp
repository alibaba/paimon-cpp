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

#include "paimon/common/memory/memory_slice_output.h"

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

std::unique_ptr<MemorySlice> MemorySliceOutput::ToSlice() {
    auto segment = std::make_shared<MemorySegment>(segment_);
    return std::make_unique<MemorySlice>(segment, 0, size_);
}

void MemorySliceOutput::WriteByte(int8_t value) {
    EnsureSize(size_ + 1);
    segment_.Put(size_++, static_cast<char>(value));
}

void MemorySliceOutput::WriteShort(int16_t value) {
    EnsureSize(size_ + 2);
    segment_.PutValue(size_, value);
    size_ += 2;
}

void MemorySliceOutput::WriteInt(int32_t value) {
    EnsureSize(size_ + 4);
    segment_.PutValue(size_, value);
    size_ += 4;
}

void MemorySliceOutput::WriteLong(int64_t value) {
    EnsureSize(size_ + 8);
    segment_.PutValue(size_, value);
    size_ += 8;
}

void MemorySliceOutput::WriteVarLenInt(int32_t value) {
    if (value < 0) {
        throw std::invalid_argument("negative value: v=" + std::to_string(value));
    }
    while ((value & ~0x7F) != 0) {
        WriteByte(((value & 0x7F) | 0x80));
        value >>= 7;
    }
    WriteByte(static_cast<int8_t>(value));
}

void MemorySliceOutput::WriteVarLenLong(int64_t value) {
    if (value < 0) {
        throw std::invalid_argument("negative value: v=" + std::to_string(value));
    }
    while ((value & ~0x7F) != 0) {
        WriteByte(((value & 0x7F) | 0x80));
        value >>= 7;
    }
    WriteByte(static_cast<int8_t>(value));
}

void MemorySliceOutput::WriteBytes(const std::shared_ptr<Bytes>& source) {
    WriteBytes(source, 0, source->size());
}

void MemorySliceOutput::WriteBytes(const std::shared_ptr<Bytes>& source, int source_index,
                                   int length) {
    EnsureSize(size_ + length);
    std::string_view sv{source->data(), source->size()};
    segment_.Put(size_, sv, source_index, length);
    size_ += length;
}

void MemorySliceOutput::EnsureSize(int size) {
    if (size <= segment_.Size()) {
        return;
    }
    int32_t capacity = segment_.Size();
    int min_capacity = segment_.Size() + size;
    while (capacity < min_capacity) {
        capacity <<= 1;
    }

    auto bytes = std::make_shared<Bytes>(capacity, pool_);
    MemorySegment new_segment = MemorySegment::Wrap(bytes);

    segment_.CopyTo(0, &new_segment, 0, segment_.Size());
    segment_ = new_segment;
}

}  // namespace paimon
