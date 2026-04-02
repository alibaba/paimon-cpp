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

#include "paimon/common/memory/memory_slice.h"

#include "paimon/common/memory/memory_slice_input.h"

namespace paimon {
MemorySlice MemorySlice::Wrap(const std::shared_ptr<Bytes>& bytes) {
    auto segment = MemorySegment::Wrap(bytes);
    return MemorySlice(segment, 0, segment.Size());
}

MemorySlice MemorySlice::Wrap(const MemorySegment& segment) {
    return MemorySlice(segment, 0, segment.Size());
}

MemorySlice::MemorySlice(const MemorySegment& segment, int32_t offset, int32_t length)
    : segment_(segment), offset_(offset), length_(length) {}

MemorySlice MemorySlice::Slice(int32_t index, int32_t length) const {
    if (index == 0 && length == length_) {
        return *this;
    }
    return MemorySlice(segment_, offset_ + index, length);
}

int32_t MemorySlice::Length() const {
    return length_;
}

int32_t MemorySlice::Offset() const {
    return offset_;
}

std::shared_ptr<Bytes> MemorySlice::GetHeapMemory() const {
    return segment_.GetHeapMemory();
}

const MemorySegment& MemorySlice::GetSegment() const {
    return segment_;
}

int8_t MemorySlice::ReadByte(int32_t position) const {
    return segment_.GetValue<int8_t>(offset_ + position);
}

int32_t MemorySlice::ReadInt(int32_t position) const {
    return segment_.GetValue<int32_t>(offset_ + position);
}

int16_t MemorySlice::ReadShort(int32_t position) const {
    return segment_.GetValue<int16_t>(offset_ + position);
}

int64_t MemorySlice::ReadLong(int32_t position) const {
    return segment_.GetValue<int64_t>(offset_ + position);
}

std::string_view MemorySlice::ReadStringView() const {
    auto array = segment_.GetArray();
    return {array->data() + offset_, static_cast<size_t>(length_)};
}

std::shared_ptr<Bytes> MemorySlice::CopyBytes(MemoryPool* pool) const {
    auto bytes = std::make_shared<Bytes>(length_, pool);
    auto target = MemorySegment::Wrap(bytes);
    segment_.CopyTo(offset_, &target, 0, length_);
    return bytes;
}

MemorySliceInput MemorySlice::ToInput() const {
    return MemorySliceInput(*this);
}

}  // namespace paimon
