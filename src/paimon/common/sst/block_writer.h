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

#include "paimon/common/memory/memory_slice_output.h"
#include "paimon/memory/bytes.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace paimon {

///
/// Writer to build a Block. A block is designed for storing and random-accessing k-v pairs. The
/// layout is as below:
///
/// <pre>
///     +---------------+
///     | Block Trailer |
///     +------------------------------------------------+
///     |       Block CRC32C      |     Compression      |
///     +------------------------------------------------+
///     +---------------+
///     |  Block Data   |
///     +---------------+--------------------------------+----+
///     | key len | key bytes | value len | value bytes  |    |
///     +------------------------------------------------+    |
///     | key len | key bytes | value len | value bytes  |    +-> Key-Value pairs
///     +------------------------------------------------+    |
///     |                  ... ...                       |    |
///     +------------------------------------------------+----+
///     | entry pos | entry pos |     ...    | entry pos |    +-> optional, for unaligned block
///     +------------------------------------------------+----+
///     |   entry num  /  entry size   |   aligned type  |
///     +------------------------------------------------+
/// </pre>
///
class BlockWriter {
 public:
    BlockWriter(int32_t size, const std::shared_ptr<MemoryPool>& pool, bool aligned = true)
        : size_(size), pool_(pool), aligned_(aligned) {
        block_ = std::make_shared<MemorySliceOutput>(size, pool_.get());
        aligned_size_ = 0;
    }

    ~BlockWriter() = default;

    void Write(std::shared_ptr<Bytes>& key, std::shared_ptr<Bytes>& value);

    void Reset();

    int32_t Size() const {
        return positions_.size();
    }

    int32_t Memory() const {
        int memory = block_->Size() + 5;
        if (!aligned_) {
            memory += positions_.size() * 4;
        }
        return memory;
    }

    Result<std::unique_ptr<paimon::MemorySlice>> Finish();

 private:
    const int32_t size_;
    const std::shared_ptr<MemoryPool> pool_;

    std::vector<int32_t> positions_;
    std::shared_ptr<MemorySliceOutput> block_;
    bool aligned_;
    int32_t aligned_size_;
};
}  // namespace paimon
