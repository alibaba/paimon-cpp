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

#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/sst/block_handle.h"
#include "paimon/common/sst/bloom_filter_handle.h"
#include "paimon/core/key_value.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace paimon {

/// Footer of a block.
class BlockFooter {
 public:
    static Result<std::unique_ptr<BlockFooter>> ReadBlockFooter(
        std::shared_ptr<MemorySliceInput>& input);

 public:
    BlockFooter(const std::shared_ptr<BlockHandle>& index_block_handle,
                const std::shared_ptr<BloomFilterHandle>& bloom_filter_handle)
        : index_block_handle_(index_block_handle), bloom_filter_handle_(bloom_filter_handle) {}

    ~BlockFooter() = default;

    std::shared_ptr<BlockHandle> GetIndexBlockHandle() const {
        return index_block_handle_;
    }
    std::shared_ptr<BloomFilterHandle> GetBloomFilterHandle() const {
        return bloom_filter_handle_;
    }

    std::shared_ptr<MemorySlice> WriteBlockFooter(MemoryPool* pool);

 public:
    // 20 bytes for bloom filter handle, 12 bytes for index block handle, 4 bytes for magic number
    static constexpr int32_t ENCODED_LENGTH = 36;
    static constexpr int32_t MAGIC_NUMBER = 1481571681;

 private:
    std::shared_ptr<BlockHandle> index_block_handle_;
    std::shared_ptr<BloomFilterHandle> bloom_filter_handle_;
};
}  // namespace paimon
