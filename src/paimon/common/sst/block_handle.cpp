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

#include "paimon/common/sst/block_handle.h"

#include "paimon/common/memory/memory_slice_output.h"

namespace paimon {

Result<BlockHandle> BlockHandle::ReadBlockHandle(MemorySliceInput* input) {
    PAIMON_ASSIGN_OR_RAISE(int64_t offset, input->ReadVarLenLong());
    PAIMON_ASSIGN_OR_RAISE(int32_t size, input->ReadVarLenInt());
    return BlockHandle(offset, size);
}

BlockHandle::BlockHandle(int64_t offset, int32_t size) : offset_(offset), size_(size) {}

int64_t BlockHandle::Offset() const {
    return offset_;
}

int32_t BlockHandle::Size() const {
    return size_;
}

int32_t BlockHandle::GetFullBlockSize() const {
    return size_ + MAX_ENCODED_LENGTH;
}

std::string BlockHandle::ToString() const {
    return "BlockHandle{offset=" + std::to_string(offset_) + ", size=" + std::to_string(size_) +
           "}";
}

Result<MemorySlice> BlockHandle::WriteBlockHandle(MemoryPool* pool) {
    MemorySliceOutput output(MAX_ENCODED_LENGTH, pool);
    PAIMON_RETURN_NOT_OK(output.WriteVarLenLong(offset_));
    PAIMON_RETURN_NOT_OK(output.WriteVarLenInt(size_));
    return output.ToSlice();
}
}  // namespace paimon
