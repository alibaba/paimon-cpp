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

#include "paimon/common/utils/bit_set.h"
namespace paimon {

Status BitSet::SetMemorySegment(MemorySegment segment, int32_t offset) {
    if (offset < 0) {
        return Status::Invalid("Offset should be positive integer.");
    }
    if (offset + byte_length_ > segment.Size()) {
        return Status::Invalid("Could not set MemorySegment, the remain buffers is not enough.");
    }
    segment_ = segment;
    offset_ = offset;
    return Status::OK();
}

void BitSet::UnsetMemorySegment() {
    segment_ = MemorySegment();
}

Status BitSet::Set(uint32_t index) {
    if (index >= bit_size_) {
        return Status::IndexError("Index out of bound");
    }
    uint32_t byte_index = index >> 3;
    auto val = segment_.Get(offset_ + byte_index);
    val |= (1 << (index & BYTE_INDEX_MASK));
    segment_.PutValue(offset_ + byte_index, val);
    return Status::OK();
}

bool BitSet::Get(uint32_t index) {
    if (index >= bit_size_) {
        return false;
    }
    uint32_t byte_index = index >> 3;
    auto val = segment_.Get(offset_ + byte_index);
    return (val & (1 << (index & BYTE_INDEX_MASK))) != 0;
}

void BitSet::Clear() {
    int32_t index = 0;
    while (index + 8 <= byte_length_) {
        segment_.PutValue(offset_ + index, 0L);
        index += 8;
    }
    while (index < byte_length_) {
        segment_.PutValue(offset_ + index, static_cast<char>(0));
        index += 1;
    }
}

}  // namespace paimon
