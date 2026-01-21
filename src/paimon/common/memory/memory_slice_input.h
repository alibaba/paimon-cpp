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
#include <stdexcept>
#include <type_traits>

#include "paimon/common/memory/memory_slice.h"
#include "paimon/io/byte_order.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {
class MemoryPool;

///  Slice of a MemorySegment.
class PAIMON_EXPORT MemorySliceInput {
 public:
    MemorySliceInput() = default;

    explicit MemorySliceInput(const std::shared_ptr<MemorySlice>& slice);

    int32_t Position() const;
    Status SetPosition(int32_t position);

    bool IsReadable();
    int32_t Available();

    int8_t ReadByte();
    int8_t ReadUnsignedByte();
    int32_t ReadInt();
    int64_t ReadLong();
    int32_t ReadVarLenInt();
    int64_t ReadVarLenLong();
    std::shared_ptr<MemorySlice> ReadSlice(int length);

    void SetOrder(ByteOrder order);

 private:
    bool NeedSwap() const;

 private:
    std::shared_ptr<MemorySlice> slice_;
    int32_t position_;

    ByteOrder byte_order_ = SystemByteOrder();
};

}  // namespace paimon
