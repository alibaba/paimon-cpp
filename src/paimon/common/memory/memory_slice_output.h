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

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <type_traits>

#include "paimon/common/memory/memory_slice.h"
#include "paimon/visibility.h"

namespace paimon {
class MemoryPool;

///  Slice of a MemorySegment.
class PAIMON_EXPORT MemorySliceOutput {
 public:
    MemorySliceOutput() = default;

    MemorySliceOutput(int32_t size, std::shared_ptr<MemoryPool>& pool);

    int32_t Size() const;
    void Reset();
    std::unique_ptr<MemorySlice> ToSlice();

    void WriteByte(int8_t value);
    void WriteShort(int16_t value);
    void WriteInt(int32_t value);
    void WriteVarLenInt(int32_t value);
    void WriteLong(int64_t value);
    void RwiteVarLenLong(int64_t value);
    void WriteBytes(std::shared_ptr<Bytes>& source);
    void WriteBytes(std::shared_ptr<Bytes>& source, int source_index, int length);

 private:
    void EnsureSize(int bytes);

 private:
 std::shared_ptr<MemoryPool> pool_;
    MemorySegment segment_;
    int32_t size_;
};

}  // namespace paimon
