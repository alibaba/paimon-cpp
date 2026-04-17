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

#include "paimon/common/memory/memory_slice.h"
#include "paimon/io/byte_order.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {
class MemoryPool;

///  Slice of a MemorySegment.
class PAIMON_EXPORT MemorySliceOutput {
 public:
    MemorySliceOutput() = default;

    MemorySliceOutput(int32_t estimated_size, MemoryPool* pool);

    int32_t Size() const;
    void Reset();
    MemorySlice ToSlice();

    template <typename T>
    void WriteValue(T value);

    Status WriteVarLenInt(int32_t value);
    Status WriteVarLenLong(int64_t value);

    void WriteBytes(const std::shared_ptr<Bytes>& source);
    void WriteBytes(const std::shared_ptr<Bytes>& source, int32_t source_index, int32_t length);

    void SetOrder(ByteOrder order);

 private:
    void EnsureSize(int32_t bytes);
    bool NeedSwap() const;

 private:
    MemoryPool* pool_;
    MemorySegment segment_;
    int32_t size_;

    ByteOrder byte_order_ = SystemByteOrder();
};

}  // namespace paimon
