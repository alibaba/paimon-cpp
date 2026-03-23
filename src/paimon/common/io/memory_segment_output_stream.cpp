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

#include "paimon/common/io/memory_segment_output_stream.h"

#include <algorithm>

#include "paimon/memory/bytes.h"

namespace paimon {
class MemoryPool;

MemorySegmentOutputStream::MemorySegmentOutputStream(int32_t segment_size,
                                                     const std::shared_ptr<MemoryPool>& pool)
    : segment_size_(segment_size), pool_(pool) {
    Advance();
}

void MemorySegmentOutputStream::Advance() {
    current_segment_ = NextSegment();
    position_in_segment_ = 0;
}

MemorySegment MemorySegmentOutputStream::NextSegment() {
    MemorySegment memory_segment = MemorySegment::AllocateHeapMemory(segment_size_, pool_.get());
    memory_segments_.push_back(memory_segment);
    return memory_segment;
}

void MemorySegmentOutputStream::WriteBytes(const std::shared_ptr<Bytes>& bytes) {
    auto segment = MemorySegment::Wrap(bytes);
    Write(segment, 0, segment.Size());
}

void MemorySegmentOutputStream::WriteString(const std::string& str) {
    WriteValue<int16_t>(str.length());
    Write(str.data(), str.size());
}

void MemorySegmentOutputStream::Write(const char* data, uint32_t size) {
    auto bytes = std::make_shared<Bytes>(size, pool_.get());
    memcpy(bytes->data(), data, size);
    auto segment = MemorySegment::Wrap(bytes);
    Write(segment, 0, segment.Size());
}

void MemorySegmentOutputStream::Write(const MemorySegment& segment, int32_t offset, int32_t len) {
    int32_t remaining = segment_size_ - position_in_segment_;
    if (remaining >= len) {
        segment.CopyTo(offset, &current_segment_, position_in_segment_, len);
        position_in_segment_ += len;
    } else {
        if (remaining == 0) {
            Advance();
            remaining = segment_size_ - position_in_segment_;
        }
        while (true) {
            int32_t to_put = std::min(remaining, len);
            segment.CopyTo(offset, &current_segment_, position_in_segment_, to_put);
            offset += to_put;
            len -= to_put;

            if (len > 0) {
                position_in_segment_ = segment_size_;
                Advance();
                remaining = segment_size_ - position_in_segment_;
            } else {
                position_in_segment_ += to_put;
                break;
            }
        }
    }
}

int64_t MemorySegmentOutputStream::CurrentSize() const {
    return segment_size_ * (memory_segments_.size() - 1) + CurrentPositionInSegment();
}

bool MemorySegmentOutputStream::NeedSwap() const {
    return SystemByteOrder() != byte_order_;
}

}  // namespace paimon
