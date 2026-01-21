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

#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>

#include "paimon/common/memory/memory_segment.h"
#include "paimon/memory/bytes.h"
#include "paimon/visibility.h"

namespace paimon {
class MemoryPool;
class MemorySliceInput;

///  Slice of a MemorySegment.
class PAIMON_EXPORT MemorySlice : public std::enable_shared_from_this<MemorySlice> {
 public:
    static std::shared_ptr<MemorySlice> Wrap(const std::shared_ptr<Bytes>& bytes);
    static std::shared_ptr<MemorySlice> Wrap(const std::shared_ptr<MemorySegment>& segment);

 public:
    MemorySlice() = default;

    MemorySlice(const std::shared_ptr<MemorySegment>& segment, int32_t offset, int32_t length);
    std::shared_ptr<MemorySlice> Slice(int32_t index, int32_t length);

    int32_t Length() const;
    int32_t Offset() const;
    std::shared_ptr<Bytes> GetHeapMemory();

    int8_t ReadByte(int32_t position);
    int32_t ReadInt(int32_t position);
    int16_t ReadShort(int32_t position);
    int64_t ReadLong(int32_t position);
    std::string_view ReadStringView();

    std::shared_ptr<Bytes> CopyBytes(MemoryPool* pool);

    bool operator<(const MemorySlice& other) const;
    bool operator>(const MemorySlice& other) const;
    bool operator==(const MemorySlice& other) const;
    bool operator!=(const MemorySlice& other) const;
    bool operator<=(const MemorySlice& other) const;
    bool operator>=(const MemorySlice& other) const;

    std::shared_ptr<MemorySliceInput> ToInput();

 private:
    int32_t Compare(const MemorySlice& other) const;

 private:
    std::shared_ptr<MemorySegment> segment_;
    int32_t offset_;
    int32_t length_;
};

}  // namespace paimon
