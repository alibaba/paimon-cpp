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

#include "paimon/common/memory/memory_segment_utils.h"

#include <cassert>

#include "paimon/common/utils/murmurhash_utils.h"

namespace paimon {
std::shared_ptr<Bytes> MemorySegmentUtils::AllocateBytes(int32_t length, MemoryPool* pool) {
    return Bytes::AllocateBytes(length, pool);
}

void MemorySegmentUtils::CopyFromBytes(std::vector<MemorySegment>* segments, int32_t offset,
                                       const Bytes& bytes, int32_t bytes_offset,
                                       int32_t num_bytes) {
    if (segments->size() == 1) {
        (*segments)[0].Put(offset, bytes, bytes_offset, num_bytes);
    } else {
        CopyMultiSegmentsFromBytes(segments, offset, bytes, bytes_offset, num_bytes);
    }
}

void MemorySegmentUtils::CopyMultiSegmentsFromBytes(std::vector<MemorySegment>* segments,
                                                    int32_t offset, const Bytes& bytes,
                                                    int32_t bytes_offset, int32_t num_bytes) {
    int32_t remain_size = num_bytes;
    for (auto& segment : (*segments)) {
        int32_t remain = segment.Size() - offset;
        if (remain > 0) {
            int32_t n_copy = std::min(remain, remain_size);
            segment.Put(offset, bytes, num_bytes - remain_size + bytes_offset, n_copy);
            remain_size -= n_copy;
            // next new segment.
            offset = 0;
            if (remain_size == 0) {
                return;
            }
        } else {
            // remain is negative, let's advance to next segment
            // now the offset = offset - segmentSize (-remain)
            offset = -remain;
        }
    }
}

PAIMON_UNIQUE_PTR<Bytes> MemorySegmentUtils::CopyToBytes(const std::vector<MemorySegment>& segments,
                                                         int32_t offset, int32_t num_bytes,
                                                         MemoryPool* pool) {
    assert(pool);
    auto bytes = Bytes::AllocateBytes(num_bytes, pool);
    CopyToBytes(segments, offset, bytes.get(), 0, num_bytes);
    return bytes;
}

void MemorySegmentUtils::CopyToUnsafe(const std::vector<MemorySegment>& segments, int32_t offset,
                                      void* target, int32_t num_bytes) {
    if (InFirstSegment(segments, offset, num_bytes)) {
        segments[0].CopyToUnsafe(offset, target, 0, num_bytes);
    } else {
        CopyMultiSegmentsToUnsafe(segments, offset, target, num_bytes);
    }
}

void MemorySegmentUtils::CopyMultiSegmentsToUnsafe(const std::vector<MemorySegment>& segments,
                                                   int32_t offset, void* target,
                                                   int32_t num_bytes) {
    int32_t remain_size = num_bytes;
    for (const auto& segment : segments) {
        int32_t remain = segment.Size() - offset;
        if (remain > 0) {
            int32_t n_copy = std::min(remain, remain_size);
            segment.CopyToUnsafe(offset, target, num_bytes - remain_size, n_copy);
            remain_size -= n_copy;
            // next new segment.
            offset = 0;
            if (remain_size == 0) {
                return;
            }
        } else {
            // remain is negative, let's advance to next segment
            // now the offset = offset - segmentSize (-remain)
            offset = -remain;
        }
    }
}

std::shared_ptr<Bytes> MemorySegmentUtils::GetBytes(const std::vector<MemorySegment>& segments,
                                                    int32_t base_offset, int32_t size_in_bytes,
                                                    MemoryPool* pool) {
    // avoid copy if `base` is `byte[]`
    if (segments.size() == 1) {
        std::shared_ptr<Bytes> heap_memory = segments[0].GetHeapMemory();
        if (base_offset == 0 && heap_memory != nullptr &&
            static_cast<int32_t>(heap_memory->size()) == size_in_bytes) {
            return heap_memory;
        } else {
            std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(size_in_bytes, pool);
            segments[0].Get(base_offset, bytes.get(), 0, size_in_bytes);
            return bytes;
        }
    } else {
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(size_in_bytes, pool);
        CopyMultiSegmentsToBytes(segments, base_offset, bytes.get(), 0, size_in_bytes);
        return bytes;
    }
}

bool MemorySegmentUtils::InFirstSegment(const std::vector<MemorySegment>& segments, int32_t offset,
                                        int32_t num_bytes) {
    return num_bytes + offset <= segments[0].Size();
}

int32_t MemorySegmentUtils::ByteIndex(int32_t bit_index) {
    return (static_cast<uint32_t>(bit_index)) >> ADDRESS_BITS_PER_WORD;
}

void MemorySegmentUtils::BitUnSet(MemorySegment* segment, int32_t base_offset, int32_t index) {
    int32_t offset = base_offset + ByteIndex(index);
    char current = segment->Get(offset);
    current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
    segment->Put(offset, current);
}

void MemorySegmentUtils::BitSet(MemorySegment* segment, int32_t base_offset, int32_t index) {
    int32_t offset = base_offset + ByteIndex(index);
    char current = segment->Get(offset);
    current |= (1 << (index & BIT_BYTE_INDEX_MASK));
    segment->Put(offset, current);
}

bool MemorySegmentUtils::BitGet(const MemorySegment& segment, int32_t base_offset, int32_t index) {
    int32_t offset = base_offset + ByteIndex(index);
    char current = segment.Get(offset);
    return (current & (1 << (index & BIT_BYTE_INDEX_MASK))) != 0;
}

void MemorySegmentUtils::BitSet(std::vector<MemorySegment>* segments, int32_t base_offset,
                                int32_t index) {
    if (segments->size() == 1) {
        int32_t offset = base_offset + ByteIndex(index);
        MemorySegment& segment = (*segments)[0];
        char current = segment.Get(offset);
        current |= (1 << (index & BIT_BYTE_INDEX_MASK));
        segment.Put(offset, current);
    } else {
        BitSetMultiSegments(segments, base_offset, index);
    }
}

void MemorySegmentUtils::BitSetMultiSegments(std::vector<MemorySegment>* segments,
                                             int32_t base_offset, int32_t index) {
    int32_t offset = base_offset + ByteIndex(index);
    int32_t seg_size = (*segments)[0].Size();
    int32_t seg_index = offset / seg_size;
    int32_t seg_offset = offset - seg_index * seg_size;  // equal to %
    MemorySegment& segment = (*segments)[seg_index];

    char current = segment.Get(seg_offset);
    current |= (1 << (index & BIT_BYTE_INDEX_MASK));
    segment.Put(seg_offset, current);
}

bool MemorySegmentUtils::BitGet(const std::vector<MemorySegment>& segments, int32_t base_offset,
                                int32_t index) {
    int32_t offset = base_offset + ByteIndex(index);
    char current = GetValue<char>(segments, offset);
    return (current & (1 << (index & BIT_BYTE_INDEX_MASK))) != 0;
}

void MemorySegmentUtils::BitUnSet(std::vector<MemorySegment>* segments, int32_t base_offset,
                                  int32_t index) {
    if (segments->size() == 1) {
        MemorySegment& segment = (*segments)[0];
        int32_t offset = base_offset + ByteIndex(index);
        char current = segment.Get(offset);
        current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
        segment.Put(offset, current);
    } else {
        BitUnSetMultiSegments(segments, base_offset, index);
    }
}

void MemorySegmentUtils::BitUnSetMultiSegments(std::vector<MemorySegment>* segments,
                                               int32_t base_offset, int32_t index) {
    int32_t offset = base_offset + ByteIndex(index);
    int32_t seg_size = (*segments)[0].Size();
    int32_t seg_index = offset / seg_size;
    int32_t seg_offset = offset - seg_index * seg_size;  // equal to %
    MemorySegment& segment = (*segments)[seg_index];

    char current = segment.Get(seg_offset);
    current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
    segment.Put(seg_offset, current);
}

bool MemorySegmentUtils::Equals(const std::vector<MemorySegment>& segments1, int32_t offset1,
                                const std::vector<MemorySegment>& segments2, int32_t offset2,
                                int32_t len) {
    if (InFirstSegment(segments1, offset1, len) && InFirstSegment(segments2, offset2, len)) {
        return segments1[0].EqualTo(segments2[0], offset1, offset2, len);
    } else {
        return EqualsMultiSegments(segments1, offset1, segments2, offset2, len);
    }
}

bool MemorySegmentUtils::EqualsMultiSegments(const std::vector<MemorySegment>& segments1,
                                             int32_t offset1,
                                             const std::vector<MemorySegment>& segments2,
                                             int32_t offset2, int32_t len) {
    if (len == 0) {
        // quick way and avoid seg_size is zero.
        return true;
    }

    int32_t seg_size1 = segments1[0].Size();
    int32_t seg_size2 = segments2[0].Size();

    // find first seg_index and seg_offset of segments.
    int32_t seg_index1 = offset1 / seg_size1;
    int32_t seg_index2 = offset2 / seg_size2;
    int32_t seg_offset1 = offset1 - seg_size1 * seg_index1;  // equal to %
    int32_t seg_offset2 = offset2 - seg_size2 * seg_index2;  // equal to %

    while (len > 0) {
        int32_t equal_len =
            std::min(std::min(len, seg_size1 - seg_offset1), seg_size2 - seg_offset2);
        if (!segments1[seg_index1].EqualTo(segments2[seg_index2], seg_offset1, seg_offset2,
                                           equal_len)) {
            return false;
        }
        len -= equal_len;
        seg_offset1 += equal_len;
        if (seg_offset1 == seg_size1) {
            seg_offset1 = 0;
            seg_index1++;
        }
        seg_offset2 += equal_len;
        if (seg_offset2 == seg_size2) {
            seg_offset2 = 0;
            seg_index2++;
        }
    }
    return true;
}

int32_t MemorySegmentUtils::Find(const std::vector<MemorySegment>& segments1, int32_t offset1,
                                 int32_t num_bytes1, const std::vector<MemorySegment>& segments2,
                                 int32_t offset2, int32_t num_bytes2) {
    if (num_bytes2 == 0) {  // quick way 1.
        return offset1;
    }
    if (InFirstSegment(segments1, offset1, num_bytes1) &&
        InFirstSegment(segments2, offset2, num_bytes2)) {
        char first = segments2[0].Get(offset2);
        int32_t end = num_bytes1 - num_bytes2 + offset1;
        for (int32_t i = offset1; i <= end; i++) {
            // quick way 2: equal first byte.
            if (segments1[0].Get(i) == first &&
                segments1[0].EqualTo(segments2[0], i, offset2, num_bytes2)) {
                return i;
            }
        }
        return -1;
    } else {
        return FindInMultiSegments(segments1, offset1, num_bytes1, segments2, offset2, num_bytes2);
    }
}
int32_t MemorySegmentUtils::FindInMultiSegments(const std::vector<MemorySegment>& segments1,
                                                int32_t offset1, int32_t num_bytes1,
                                                const std::vector<MemorySegment>& segments2,
                                                int32_t offset2, int32_t num_bytes2) {
    int32_t end = num_bytes1 - num_bytes2 + offset1;
    for (int32_t i = offset1; i <= end; i++) {
        if (EqualsMultiSegments(segments1, i, segments2, offset2, num_bytes2)) {
            return i;
        }
    }
    return -1;
}

int32_t MemorySegmentUtils::Hash(const std::vector<MemorySegment>& segments, int32_t offset,
                                 int32_t num_bytes, MemoryPool* pool) {
    if (InFirstSegment(segments, offset, num_bytes)) {
        return MurmurHashUtils::HashBytes(segments[0], offset, num_bytes);
    } else {
        return HashMultiSeg(segments, offset, num_bytes, pool);
    }
}

int32_t MemorySegmentUtils::HashByWords(const std::vector<MemorySegment>& segments, int32_t offset,
                                        int32_t num_bytes, MemoryPool* pool) {
    if (InFirstSegment(segments, offset, num_bytes)) {
        return MurmurHashUtils::HashBytesByWords(segments[0], offset, num_bytes);
    } else {
        return HashMultiSegByWords(segments, offset, num_bytes, pool);
    }
}

int32_t MemorySegmentUtils::HashMultiSegByWords(const std::vector<MemorySegment>& segments,
                                                int32_t offset, int32_t num_bytes,
                                                MemoryPool* pool) {
    std::shared_ptr<Bytes> bytes = AllocateBytes(num_bytes, pool);
    CopyMultiSegmentsToBytes(segments, offset, bytes.get(), 0, num_bytes);
    return MurmurHashUtils::HashUnsafeBytesByWords(reinterpret_cast<void*>(bytes->data()), 0,
                                                   num_bytes);
}

int32_t MemorySegmentUtils::HashMultiSeg(const std::vector<MemorySegment>& segments, int32_t offset,
                                         int32_t num_bytes, MemoryPool* pool) {
    std::shared_ptr<Bytes> bytes = AllocateBytes(num_bytes, pool);
    CopyMultiSegmentsToBytes(segments, offset, bytes.get(), 0, num_bytes);

    return MurmurHashUtils::HashUnsafeBytes(reinterpret_cast<void*>(bytes->data()), 0, num_bytes);
}

}  // namespace paimon
