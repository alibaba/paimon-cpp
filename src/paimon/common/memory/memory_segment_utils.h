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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/io/byte_order.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {

class PAIMON_EXPORT MemorySegmentUtils {
 public:
    MemorySegmentUtils() = delete;
    ~MemorySegmentUtils() = delete;

    /// Allocate bytes in pool
    static std::shared_ptr<Bytes> AllocateBytes(int32_t length, MemoryPool* pool);

    /// Copy target segments from source byte[].
    ///
    /// @param segments target segments.
    /// @param offset target segments offset.
    /// @param bytes source byte[].
    /// @param bytes_offset source byte[] offset.
    /// @param num_bytes the number bytes to copy.
    static void CopyFromBytes(std::vector<MemorySegment>* segments, int32_t offset,
                              const Bytes& bytes, int32_t bytes_offset, int32_t num_bytes);

    /// Copy segments to a new byte[].
    ///
    /// @param segments Source segments.
    /// @param offset Source segments offset.
    /// @param num_bytes the number bytes to copy.
    static PAIMON_UNIQUE_PTR<Bytes> CopyToBytes(const std::vector<MemorySegment>& segments,
                                                int32_t offset, int32_t num_bytes,
                                                MemoryPool* pool);

    /// Copy segments to target byte[].
    ///
    /// @param segments Source segments.
    /// @param offset Source segments offset.
    /// @param bytes target byte[].
    /// @param bytes_offset target byte[] offset.
    /// @param num_bytes the number bytes to copy.
    template <typename T>
    static void CopyToBytes(const std::vector<MemorySegment>& segments, int32_t offset, T* bytes,
                            int32_t bytes_offset, int32_t num_bytes);

    /// Copy bytes of segments to output stream.
    ///
    /// @note It just copies the data in, not include the length.
    ///
    /// @param segments source segments
    /// @param offset offset for segments
    /// @param size_in_bytes size in bytes
    /// @param target target output stream
    static Status CopyToStream(const std::vector<MemorySegment>& segments, int32_t offset,
                               int32_t size_in_bytes, MemorySegmentOutputStream* target);

    /// Copy segments to target unsafe pointer.
    ///
    /// @param segments Source segments.
    /// @param offset The position where the bytes are started to be read from these memory
    /// segments.
    /// @param target The unsafe memory to copy the bytes to.
    /// @param num_bytes the number bytes to copy.
    static void CopyToUnsafe(const std::vector<MemorySegment>& segments, int32_t offset,
                             void* target, int32_t num_bytes);

    template <typename T>
    static void CopyMultiSegmentsToBytes(const std::vector<MemorySegment>& segments, int32_t offset,
                                         T* bytes, int32_t bytes_offset, int32_t num_bytes);

    static std::shared_ptr<Bytes> GetBytes(const std::vector<MemorySegment>& segments,
                                           int32_t base_offset, int32_t size_in_bytes,
                                           MemoryPool* pool);

    /// Is it just in first MemorySegment, we use quick way to do something.
    static bool InFirstSegment(const std::vector<MemorySegment>& segments, int32_t offset,
                               int32_t num_bytes);

    /// unset bit.
    ///
    /// @param segment target segment.
    /// @param base_offset bits base offset.
    /// @param index bit index from base offset.
    static void BitUnSet(MemorySegment* segment, int32_t base_offset, int32_t index);

    /// set bit.
    ///
    /// @param segment target segment.
    /// @param base_offset bits base offset.
    /// @param index bit index from base offset.
    static void BitSet(MemorySegment* segment, int32_t base_offset, int32_t index);

    /// read bit.
    ///
    /// @param segment target segment.
    /// @param base_offset bits base offset.
    /// @param index bit index from base offset.
    static bool BitGet(const MemorySegment& segment, int32_t base_offset, int32_t index);

    /// set bit from segments.
    ///
    /// @param segments target segments.
    /// @param base_offset bits base offset.
    /// @param index bit index from base offset.
    static void BitSet(std::vector<MemorySegment>* segments, int32_t base_offset, int32_t index);

    /// read bit from segments.
    ///
    /// @param segments target segments.
    /// @param base_offset bits base offset.
    /// @param index bit index from base offset.
    static bool BitGet(const std::vector<MemorySegment>& segments, int32_t base_offset,
                       int32_t index);

    /// unset bit from segments.
    ///
    /// @param segments target segments.
    /// @param base_offset bits base offset.
    /// @param index bit index from base offset.
    static void BitUnSet(std::vector<MemorySegment>* segments, int32_t base_offset, int32_t index);

    /// get value from segments. Only support: bool, char, int16_t, int32_t, int64_t, double, float
    ///
    /// @param segments target segments.
    /// @param offset value offset.
    template <typename T>
    static T GetValue(const std::vector<MemorySegment>& segments, int32_t offset);

    /// set value to segments. Only support: bool, char, int16_t, int32_t, int64_t, double, float
    ///
    /// @param segments target segments.
    /// @param offset value offset.
    template <typename T>
    static void SetValue(std::vector<MemorySegment>* segments, int32_t offset, const T& value);

    /*
     * Equals two memory segments regions.
     *
     * @param segments1 Segments 1
     * @param offset1 Offset of segments1 to start equaling
     * @param segments2 Segments 2
     * @param offset2 Offset of segments2 to start equaling
     * @param len Length of the equaled memory region
     * @return true if equal, false otherwise
     */
    static bool Equals(const std::vector<MemorySegment>& segments1, int32_t offset1,
                       const std::vector<MemorySegment>& segments2, int32_t offset2, int32_t len);

    static bool EqualsMultiSegments(const std::vector<MemorySegment>& segments1, int32_t offset1,
                                    const std::vector<MemorySegment>& segments2, int32_t offset2,
                                    int32_t len);

    /// Find equal segments2 in segments1.
    ///
    /// @param segments1 segs to find.
    /// @param segments2 sub segs.
    /// @return Return the found offset, return -1 if not find.
    static int32_t Find(const std::vector<MemorySegment>& segments1, int32_t offset1,
                        int32_t num_bytes1, const std::vector<MemorySegment>& segments2,
                        int32_t offset2, int32_t num_bytes2);

    /// hash segments to int, num_bytes must be aligned to 4 bytes.
    ///
    /// @param segments Source segments.
    /// @param offset Source segments offset.
    /// @param num_bytes the number bytes to hash.
    static int32_t HashByWords(const std::vector<MemorySegment>& segments, int32_t offset,
                               int32_t num_bytes, MemoryPool* pool);

    /// hash segments to int.
    ///
    /// @param segments Source segments.
    /// @param offset Source segments offset.
    /// @param num_bytes the number bytes to hash.
    static int32_t Hash(const std::vector<MemorySegment>& segments, int32_t offset,
                        int32_t num_bytes, MemoryPool* pool);

 private:
    static constexpr int32_t ADDRESS_BITS_PER_WORD = 3;
    static constexpr int32_t BIT_BYTE_INDEX_MASK = 7;
    static constexpr int32_t MAX_BYTES_LENGTH = 1024 * 64;
    static constexpr int32_t MAX_CHARS_LENGTH = 1024 * 32;

 private:
    template <typename T>
    static void SetValueToMultiSegments(std::vector<MemorySegment>* segments, int32_t offset,
                                        const T& value);

    template <typename T>
    static void SetValueSlowly(std::vector<MemorySegment>* segments, int32_t seg_size,
                               int32_t seg_num, int32_t seg_offset, const T& value);
    template <typename T>
    static T GetValueFromMultiSegments(const std::vector<MemorySegment>& segments, int32_t offset);

    template <typename T>
    static T GetValueSlowly(const std::vector<MemorySegment>& segments, int32_t seg_size,
                            int32_t seg_num, int32_t seg_offset);

    static void CopyMultiSegmentsFromBytes(std::vector<MemorySegment>* segments, int32_t offset,
                                           const Bytes& bytes, int32_t bytes_offset,
                                           int32_t num_bytes);

    static void CopyMultiSegmentsToUnsafe(const std::vector<MemorySegment>& segments,
                                          int32_t offset, void* target, int32_t num_bytes);

    static int32_t FindInMultiSegments(const std::vector<MemorySegment>& segments1, int32_t offset1,
                                       int32_t num_bytes1,
                                       const std::vector<MemorySegment>& segments2, int32_t offset2,
                                       int32_t num_bytes2);

    static int32_t HashMultiSeg(const std::vector<MemorySegment>& segments, int32_t offset,
                                int32_t num_bytes, MemoryPool* pool);

    static int32_t HashMultiSegByWords(const std::vector<MemorySegment>& segments, int32_t offset,
                                       int32_t num_bytes, MemoryPool* pool);

    /// Given a bit index, return the byte index containing it.
    ///
    /// @param bit_index the bit index.
    /// @return the byte index.
    static int32_t ByteIndex(int32_t bit_index);

    static void BitSetMultiSegments(std::vector<MemorySegment>* segments, int32_t base_offset,
                                    int32_t index);

    static void BitUnSetMultiSegments(std::vector<MemorySegment>* segments, int32_t base_offset,
                                      int32_t index);
};

template <typename T>
inline void MemorySegmentUtils::CopyToBytes(const std::vector<MemorySegment>& segments,
                                            int32_t offset, T* bytes, int32_t bytes_offset,
                                            int32_t num_bytes) {
    if (InFirstSegment(segments, offset, num_bytes)) {
        segments[0].Get(offset, bytes, bytes_offset, num_bytes);
    } else {
        CopyMultiSegmentsToBytes(segments, offset, bytes, bytes_offset, num_bytes);
    }
}

template <typename T>
inline void MemorySegmentUtils::CopyMultiSegmentsToBytes(const std::vector<MemorySegment>& segments,
                                                         int32_t offset, T* bytes,
                                                         int32_t bytes_offset, int32_t num_bytes) {
    int32_t remain_size = num_bytes;
    for (const auto& segment : segments) {
        int32_t remain = segment.Size() - offset;
        if (remain > 0) {
            int32_t n_copy = std::min(remain, remain_size);
            segment.Get(offset, bytes, num_bytes - remain_size + bytes_offset, n_copy);
            remain_size -= n_copy;
            // next new segment.
            offset = 0;
            if (remain_size == 0) {
                return;
            }
        } else {
            // remain is negative, let's advance to next segment
            // now the offset = offset - segment_size (-remain)
            offset = -remain;
        }
    }
}

template <typename T>
inline T MemorySegmentUtils::GetValue(const std::vector<MemorySegment>& segments, int32_t offset) {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    if (InFirstSegment(segments, offset, sizeof(T))) {
        return segments[0].GetValue<T>(offset);
    } else {
        return GetValueFromMultiSegments<T>(segments, offset);
    }
}

template <typename T>
inline void MemorySegmentUtils::SetValue(std::vector<MemorySegment>* segments, int32_t offset,
                                         const T& value) {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    if (InFirstSegment(*segments, offset, sizeof(T))) {
        (*segments)[0].PutValue<T>(offset, value);
    } else {
        SetValueToMultiSegments<T>(segments, offset, value);
    }
}

template <typename T>
inline void MemorySegmentUtils::SetValueToMultiSegments(std::vector<MemorySegment>* segments,
                                                        int32_t offset, const T& value) {
    int32_t seg_size = (*segments)[0].Size();
    int32_t seg_index = offset / seg_size;
    int32_t seg_offset = offset - seg_index * seg_size;  // equal to %

    if (seg_offset <= seg_size - static_cast<int32_t>(sizeof(T))) {
        (*segments)[seg_index].PutValue<T>(seg_offset, value);
    } else {
        SetValueSlowly<T>(segments, seg_size, seg_index, seg_offset, value);
    }
}

template <>
inline void MemorySegmentUtils::SetValueToMultiSegments(std::vector<MemorySegment>* segments,
                                                        int32_t offset, const float& value) {
    int32_t seg_size = (*segments)[0].Size();
    int32_t seg_index = offset / seg_size;
    int32_t seg_offset = offset - seg_index * seg_size;  // equal to %

    if (seg_offset <= seg_size - static_cast<int32_t>(sizeof(float))) {
        (*segments)[seg_index].PutValue<float>(seg_offset, value);
    } else {
        int32_t int_value;
        std::memcpy(&int_value, &value, sizeof(float));
        SetValueSlowly<int32_t>(segments, seg_size, seg_index, seg_offset, int_value);
    }
}

template <>
inline void MemorySegmentUtils::SetValueToMultiSegments(std::vector<MemorySegment>* segments,
                                                        int32_t offset, const double& value) {
    int32_t seg_size = (*segments)[0].Size();
    int32_t seg_index = offset / seg_size;
    int32_t seg_offset = offset - seg_index * seg_size;  // equal to %

    if (seg_offset <= seg_size - static_cast<int32_t>(sizeof(double))) {
        (*segments)[seg_index].PutValue<double>(seg_offset, value);
    } else {
        int64_t long_value;
        std::memcpy(&long_value, &value, sizeof(double));
        SetValueSlowly<int64_t>(segments, seg_size, seg_index, seg_offset, long_value);
    }
}

template <typename T>
inline void MemorySegmentUtils::SetValueSlowly(std::vector<MemorySegment>* segments,
                                               int32_t seg_size, int32_t seg_num,
                                               int32_t seg_offset, const T& value) {
    MemorySegment segment = (*segments)[seg_num];
    for (size_t i = 0; i < sizeof(T); i++) {
        if (seg_offset == seg_size) {
            segment = (*segments)[++seg_num];
            seg_offset = 0;
        }
        T unsigned_byte;
        if (SystemByteOrder() == ByteOrder::PAIMON_LITTLE_ENDIAN) {
            unsigned_byte = value >> (i * 8);
        } else {
            int32_t shift_count = sizeof(T) - 1;
            unsigned_byte = value >> ((shift_count - i) * 8);
        }
        segment.Put(seg_offset, static_cast<char>(unsigned_byte));
        seg_offset++;
    }
}

template <typename T>
inline T MemorySegmentUtils::GetValueFromMultiSegments(const std::vector<MemorySegment>& segments,
                                                       int32_t offset) {
    int32_t seg_size = segments[0].Size();
    int32_t seg_index = offset / seg_size;
    int32_t seg_offset = offset - seg_index * seg_size;  // equal to %
    if (seg_offset <= seg_size - static_cast<int32_t>(sizeof(T))) {
        return segments[seg_index].GetValue<T>(seg_offset);
    } else {
        return GetValueSlowly<T>(segments, seg_size, seg_index, seg_offset);
    }
}

template <>
inline float MemorySegmentUtils::GetValueFromMultiSegments(
    const std::vector<MemorySegment>& segments, int32_t offset) {
    int32_t seg_size = segments[0].Size();
    int32_t seg_index = offset / seg_size;
    int32_t seg_offset = offset - seg_index * seg_size;  // equal to %
    if (seg_offset <= seg_size - static_cast<int32_t>(sizeof(float))) {
        return segments[seg_index].GetValue<float>(seg_offset);
    } else {
        auto int_value = GetValueSlowly<int32_t>(segments, seg_size, seg_index, seg_offset);
        float float_value;
        std::memcpy(&float_value, &int_value, sizeof(float));
        return float_value;
    }
}

template <>
inline double MemorySegmentUtils::GetValueFromMultiSegments(
    const std::vector<MemorySegment>& segments, int32_t offset) {
    int32_t seg_size = segments[0].Size();
    int32_t seg_index = offset / seg_size;
    int32_t seg_offset = offset - seg_index * seg_size;  // equal to %
    if (seg_offset <= seg_size - static_cast<int32_t>(sizeof(double))) {
        return segments[seg_index].GetValue<double>(seg_offset);
    } else {
        auto long_value = GetValueSlowly<int64_t>(segments, seg_size, seg_index, seg_offset);
        double double_value;
        std::memcpy(&double_value, &long_value, sizeof(double));
        return double_value;
    }
}

template <typename T>
inline T MemorySegmentUtils::GetValueSlowly(const std::vector<MemorySegment>& segments,
                                            int32_t seg_size, int32_t seg_num, int32_t seg_offset) {
    MemorySegment segment = segments[seg_num];
    if constexpr (std::is_same_v<T, bool>) {
        if (seg_offset == seg_size) {
            segment = segments[++seg_num];
            seg_offset = 0;
        }
        return static_cast<bool>(static_cast<uint8_t>(segment.Get(seg_offset)));
    } else {
        using UnsignedT = std::make_unsigned_t<T>;
        UnsignedT ret = 0;
        for (size_t i = 0; i < sizeof(T); i++) {
            if (seg_offset == seg_size) {
                segment = segments[++seg_num];
                seg_offset = 0;
            }
            UnsignedT unsigned_byte = static_cast<uint8_t>(segment.Get(seg_offset));
            if (SystemByteOrder() == ByteOrder::PAIMON_LITTLE_ENDIAN) {
                ret |= (unsigned_byte << (i * 8));
            } else {
                int32_t shift_count = sizeof(T) - 1;
                ret |= (unsigned_byte << ((shift_count - i) * 8));
            }
            seg_offset++;
        }
        return static_cast<T>(ret);
    }
}

inline Status MemorySegmentUtils::CopyToStream(const std::vector<MemorySegment>& segments,
                                               int32_t offset, int32_t size_in_bytes,
                                               MemorySegmentOutputStream* target) {
    for (const auto& source_segment : segments) {
        int32_t cur_seg_remain = source_segment.Size() - offset;
        if (cur_seg_remain > 0) {
            int32_t copy_size = std::min(cur_seg_remain, size_in_bytes);
            target->Write(source_segment, offset, copy_size);
            size_in_bytes -= copy_size;
            offset = 0;
        } else {
            offset -= source_segment.Size();
        }

        if (size_in_bytes == 0) {
            return Status::OK();
        }
    }
    if (size_in_bytes != 0) {
        return Status::Invalid(
            fmt::format("No copy finished, this should be a bug, "
                        "The remaining length is: {}",
                        size_in_bytes));
    }
    return Status::OK();
}

}  // namespace paimon
