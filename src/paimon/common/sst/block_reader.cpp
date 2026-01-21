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

#include "paimon/common/sst/block_reader.h"

#include "paimon/common/sst/block_trailer.h"

namespace paimon {

std::shared_ptr<BlockReader> BlockReader::Create(
    std::shared_ptr<MemorySlice> block,
    std::function<int32_t(const std::shared_ptr<MemorySlice>&,
                          const std::shared_ptr<MemorySlice>&)>& comparator) {
    auto ret = From(block->ReadByte(block->Length() - 1));
    if (!ret.ok()) {
        return nullptr;
    }
    BlockAlignedType type = ret.value();
    const auto trailer_len = BlockTrailer::ENCODED_LENGTH;
    int size = block->ReadInt(block->Length() - trailer_len);
    if (type == BlockAlignedType::ALIGNED) {
        auto data = block->Slice(0, block->Length() - trailer_len);
        return std::make_shared<AlignedBlockReader>(data, size, comparator);
    } else {
        int index_length = size * 4;
        int index_offset = block->Length() - trailer_len - index_length;
        auto data = block->Slice(0, index_offset);
        auto index = block->Slice(index_offset, index_length);
        return std::make_shared<UnAlignedBlockReader>(data, index, comparator);
    }
}

std::unique_ptr<BlockIterator> BlockReader::Iterator() {
    std::shared_ptr<BlockReader> ptr = shared_from_this();
    return std::make_unique<BlockIterator>(ptr);
}

std::shared_ptr<MemorySliceInput> BlockReader::BlockInput() {
    return block_->ToInput();
}

int32_t BlockReader::RecordCount() const {
    return record_count_;
}

std::function<int32_t(const std::shared_ptr<MemorySlice>&, const std::shared_ptr<MemorySlice>&)>&
BlockReader::Comparator() {
    return comparator_;
}

}  // namespace paimon
