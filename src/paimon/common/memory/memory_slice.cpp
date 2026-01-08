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

#include "paimon/common/memory/memory_slice.h"

#include "paimon/common/memory/memory_slice_input.h"

namespace paimon {
std::shared_ptr<MemorySlice> MemorySlice::Wrap(std::shared_ptr<Bytes>& bytes) {
    auto segment = MemorySegment::Wrap(bytes);
    auto ptr = std::make_shared<MemorySegment>(segment);
    return std::make_shared<MemorySlice>(ptr, 0, ptr->Size());
}

std::shared_ptr<MemorySlice> MemorySlice::Wrap(std::shared_ptr<MemorySegment>& segment) {
    return std::make_shared<MemorySlice>(segment, 0, segment->Size());
}

MemorySlice::MemorySlice(std::shared_ptr<MemorySegment>& segment, int32_t offset, int32_t length)
    : segment_(segment), offset_(offset), length_(length) {}

std::shared_ptr<MemorySlice> MemorySlice::Slice(int32_t index, int32_t length) {
    if (index == 0 && length == length_) {
        return shared_from_this();
    }
    return std::make_shared<MemorySlice>(segment_, offset_ + index, length);
}

int32_t MemorySlice::Length() const {
    return length_;
}

int32_t MemorySlice::Offset() const {
    return offset_;
}

std::shared_ptr<Bytes> MemorySlice::GetHeapMemory() {
    return segment_->GetHeapMemory();
}

int8_t MemorySlice::ReadByte(int position) {
    return segment_->GetValue<int8_t>(offset_ + position);
}

int32_t MemorySlice::ReadInt(int position) {
    return segment_->GetValue<int32_t>(offset_ + position);
}

int16_t MemorySlice::ReadShort(int position) {
    return segment_->GetValue<int16_t>(offset_ + position);
}

int64_t MemorySlice::ReadLong(int position) {
    return segment_->GetValue<int64_t>(offset_ + position);
}

std::string_view MemorySlice::ReadStringView() {
    auto array = segment_->GetArray();
    return {array->data() + offset_, static_cast<size_t>(length_)};
}

std::shared_ptr<Bytes> MemorySlice::CopyBytes(MemoryPool* pool) {
    auto bytes = std::make_shared<Bytes>(length_, pool);
    auto target = MemorySegment::Wrap(bytes);
    segment_->CopyTo(offset_, &target, 0, length_);
    return bytes;
}

bool MemorySlice::operator<(const MemorySlice& other) const {
    return Compare(other) < 0;
}
bool MemorySlice::operator>(const MemorySlice& other) const {
    return Compare(other) > 0;
}
bool MemorySlice::operator==(const MemorySlice& other) const {
    return Compare(other) == 0;
}
bool MemorySlice::operator!=(const MemorySlice& other) const {
    return !(*this == other);
}
bool MemorySlice::operator<=(const MemorySlice& other) const {
    return Compare(other) <= 0;
}
bool MemorySlice::operator>=(const MemorySlice& other) const {
    return Compare(other) >= 0;
}
std::shared_ptr<MemorySliceInput> MemorySlice::ToInput() {
    auto self = shared_from_this();
    return std::make_shared<MemorySliceInput>(self);
}

int32_t MemorySlice::Compare(const MemorySlice& other) const {
    int32_t len = std::min(length_, other.length_);
    for (int32_t i = 0; i < len; ++i) {
        auto byte1 = static_cast<unsigned char>(segment_->Get(offset_ + i));
        auto byte2 = static_cast<unsigned char>(other.segment_->Get(other.offset_ + i));
        if (byte1 != byte2) {
            return static_cast<int>(byte1) - static_cast<int>(byte2);
        }
    }
    return length_ - other.length_;
}

}  // namespace paimon
