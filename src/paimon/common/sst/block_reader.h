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
    static std::shared_ptr<BlockReader> Create(
        std::shared_ptr<MemorySlice> block,
        std::function<int32_t(const std::shared_ptr<MemorySlice>&,
                              const std::shared_ptr<MemorySlice>&)>& comparator);

    virtual ~BlockReader() = default;

    std::unique_ptr<BlockIterator> Iterator();
    virtual int32_t SeekTo(int32_t record_position) = 0;

    std::shared_ptr<MemorySliceInput> BlockInput();

    int32_t RecordCount() const;

    std::function<int32_t(const std::shared_ptr<MemorySlice>&,
                          const std::shared_ptr<MemorySlice>&)>&
    Comparator();

 protected:
    BlockReader(std::shared_ptr<MemorySlice>& block, int32_t record_count,
                std::function<int32_t(const std::shared_ptr<MemorySlice>&,
                                      const std::shared_ptr<MemorySlice>&)>& comparator)
        : block_(block), comparator_(comparator), record_count_(record_count) {}

 private:
    std::shared_ptr<MemorySlice> block_;
    std::function<int32_t(const std::shared_ptr<MemorySlice>&,
                          const std::shared_ptr<MemorySlice>&)>& comparator_;
    int32_t record_count_;
};

class AlignedBlockReader : public BlockReader {
 public:
    AlignedBlockReader(std::shared_ptr<MemorySlice>& block, int32_t record_size,
                       std::function<int32_t(const std::shared_ptr<MemorySlice>&,
                                             const std::shared_ptr<MemorySlice>&)>& comparator)
        : BlockReader(block, block->Length() / record_size, comparator),
          record_size_(record_size) {}

    int32_t SeekTo(int32_t record_position) override {
        return record_size_ * record_position;
    }

 private:
    int32_t record_size_;
};

class UnAlignedBlockReader : public BlockReader {
 public:
    UnAlignedBlockReader(std::shared_ptr<MemorySlice>& data, std::shared_ptr<MemorySlice>& index,
                         std::function<int32_t(const std::shared_ptr<MemorySlice>&,
                                               const std::shared_ptr<MemorySlice>&)>& comparator)
        : BlockReader(data, index->Length() / 4, comparator), index_(index) {}

    int32_t SeekTo(int32_t record_position) override {
        return index_->ReadInt(record_position * 4);
    }

 private:
    std::shared_ptr<MemorySlice> index_;
};

}  // namespace paimon
