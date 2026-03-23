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

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "paimon/common/memory/memory_segment.h"
#include "paimon/common/utils/math.h"
#include "paimon/io/byte_order.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class Bytes;
class MemoryPool;

// TODO(xinyu.lxy): use DataOutputStream to do big-endian conversion
class PAIMON_EXPORT MemorySegmentOutputStream {
 public:
    MemorySegmentOutputStream(int32_t segment_size, const std::shared_ptr<MemoryPool>& pool);

    static constexpr int32_t DEFAULT_SEGMENT_SIZE = 64 * 1024;

    void Write(const MemorySegment& segment, int32_t offset, int32_t len);

    template <typename T>
    void WriteValue(T v);

    /// First write length (int16), then write string bytes.
    void WriteString(const std::string& str);

    void WriteBytes(const std::shared_ptr<Bytes>& bytes);

    void Write(const char* data, uint32_t size);

    int32_t CurrentPositionInSegment() const {
        return position_in_segment_;
    }

    int64_t CurrentSize() const;

    const std::vector<MemorySegment>& Segments() const {
        return memory_segments_;
    }

    void SetOrder(ByteOrder order) {
        byte_order_ = order;
    }

 private:
    template <typename T>
    void WriteValueImpl(T v);

    void Advance();
    MemorySegment NextSegment();

    bool NeedSwap() const;

 private:
    int32_t segment_size_;
    int32_t position_in_segment_;
    std::shared_ptr<MemoryPool> pool_;
    MemorySegment current_segment_;
    std::vector<MemorySegment> memory_segments_;

    ByteOrder byte_order_ = ByteOrder::PAIMON_BIG_ENDIAN;
};

template <typename T>
void MemorySegmentOutputStream::WriteValueImpl(T v) {
    if (position_in_segment_ <= segment_size_ - static_cast<int32_t>(sizeof(T))) {
        current_segment_.PutValue<T>(position_in_segment_, v);
        position_in_segment_ += sizeof(T);
    } else if (position_in_segment_ == segment_size_) {
        Advance();
        WriteValueImpl<T>(v);
    } else {
        for (size_t i = 0; i < sizeof(T); i++) {
            // because of endian swap, just copy the bytes of input v
            uint8_t onebyte = 0;
            memcpy(&onebyte, (reinterpret_cast<uint8_t*>(&v)) + i, sizeof(onebyte));
            WriteValueImpl<uint8_t>(onebyte);
        }
    }
}

template <typename T>
void MemorySegmentOutputStream::WriteValue(T v) {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    if (NeedSwap()) {
        v = EndianSwapValue(v);
    }
    return WriteValueImpl(v);
}

}  // namespace paimon
