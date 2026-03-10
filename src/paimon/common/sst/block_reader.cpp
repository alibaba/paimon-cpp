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

Result<std::shared_ptr<BlockReader>> BlockReader::Create(const std::shared_ptr<MemorySlice>& block,
                                                         MemorySlice::SliceComparator comparator) {
    PAIMON_ASSIGN_OR_RAISE(BlockAlignedType type, From(block->ReadByte(block->Length() - 1)));
    const auto trailer_len = BlockTrailer::ENCODED_LENGTH;
    int32_t size = block->ReadInt(block->Length() - trailer_len);
    if (type == BlockAlignedType::ALIGNED) {
        auto data = block->Slice(0, block->Length() - trailer_len);
        return std::make_shared<AlignedBlockReader>(data, size, std::move(comparator));
    } else {
        int32_t index_length = size * 4;
        int32_t index_offset = block->Length() - trailer_len - index_length;
        auto data = block->Slice(0, index_offset);
        auto index = block->Slice(index_offset, index_length);
        return std::make_shared<UnAlignedBlockReader>(data, index, std::move(comparator));
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

MemorySlice::SliceComparator BlockReader::Comparator() const {
    return comparator_;
}

}  // namespace paimon
