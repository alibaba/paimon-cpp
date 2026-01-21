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

#include <memory>

#include "paimon/common/memory/memory_segment.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"

namespace paimon {

class BlockHandle {
 public:
    static std::shared_ptr<BlockHandle> ReadBlockHandle(std::shared_ptr<MemorySliceInput>& input);

 public:
    BlockHandle(int64_t offset, int32_t size);
    ~BlockHandle() = default;

    int64_t Offset() const;
    int32_t Size() const;
    int32_t GetFullBlockSize() const;

    std::string ToString() const;
    std::shared_ptr<MemorySlice> WriteBlockHandle(MemoryPool* pool);

 public:
    // max len for varlong is 9 bytes, max len for varint is 5 bytes
    static constexpr int32_t MAX_ENCODED_LENGTH = 9 + 5;

 private:
    int64_t offset_;
    int32_t size_;
};
}  // namespace paimon
