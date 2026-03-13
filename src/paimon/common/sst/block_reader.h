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

#include <functional>
#include <memory>

#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/sst/block_aligned_type.h"
#include "paimon/common/sst/block_iterator.h"
#include "paimon/memory/bytes.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
namespace paimon {
class BlockIterator;

/// Reader for a block.
class BlockReader : public std::enable_shared_from_this<BlockReader> {
 public:
    virtual ~BlockReader() = default;

    static Result<std::shared_ptr<BlockReader>> Create(const std::shared_ptr<MemorySlice>& block,
                                                       MemorySlice::SliceComparator comparator);

    virtual int32_t SeekTo(int32_t record_position) = 0;

    int32_t RecordCount() const;
    MemorySlice::SliceComparator Comparator() const;

    std::unique_ptr<BlockIterator> Iterator();
    std::shared_ptr<MemorySliceInput> BlockInput();

 protected:
    BlockReader(const std::shared_ptr<MemorySlice>& block, int32_t record_count,
                MemorySlice::SliceComparator comparator)
        : block_(block), comparator_(std::move(comparator)), record_count_(record_count) {}

 private:
    std::shared_ptr<MemorySlice> block_;
    MemorySlice::SliceComparator comparator_;
    int32_t record_count_;
};

class AlignedBlockReader : public BlockReader {
 public:
    AlignedBlockReader(const std::shared_ptr<MemorySlice>& block, int32_t record_size,
                       MemorySlice::SliceComparator comparator)
        : BlockReader(block, block->Length() / record_size, std::move(comparator)),
          record_size_(record_size) {}

    int32_t SeekTo(int32_t record_position) override {
        return record_size_ * record_position;
    }

 private:
    int32_t record_size_;
};

class UnAlignedBlockReader : public BlockReader {
 public:
    UnAlignedBlockReader(const std::shared_ptr<MemorySlice>& data,
                         std::shared_ptr<MemorySlice>& index,
                         MemorySlice::SliceComparator comparator)
        : BlockReader(data, index->Length() / 4, std::move(comparator)), index_(index) {}

    int32_t SeekTo(int32_t record_position) override {
        return index_->ReadInt(record_position * 4);
    }

 private:
    std::shared_ptr<MemorySlice> index_;
};

}  // namespace paimon
