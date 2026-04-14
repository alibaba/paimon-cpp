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
#include "paimon/common/memory/memory_slice_output.h"
#include "paimon/common/sst/block_handle.h"
#include "paimon/common/sst/bloom_filter_handle.h"

namespace paimon {
/// The Footer for BTree file.
class BTreeFileFooter {
 public:
    static Result<std::shared_ptr<BTreeFileFooter>> Read(MemorySliceInput* input);
    static MemorySlice Write(const std::shared_ptr<BTreeFileFooter>& footer, MemoryPool* pool);
    static MemorySlice Write(const std::shared_ptr<BTreeFileFooter>& footer,
                             MemorySliceOutput& output);

 public:
    BTreeFileFooter(const std::shared_ptr<BloomFilterHandle>& bloom_filter_handle,
                    const std::shared_ptr<BlockHandle>& index_block_handle,
                    const std::shared_ptr<BlockHandle>& null_bitmap_handle)
        : version_(CURRENT_VERSION),
          bloom_filter_handle_(bloom_filter_handle),
          index_block_handle_(index_block_handle),
          null_bitmap_handle_(null_bitmap_handle) {}

    BTreeFileFooter(int32_t version, const std::shared_ptr<BloomFilterHandle>& bloom_filter_handle,
                    const std::shared_ptr<BlockHandle>& index_block_handle,
                    const std::shared_ptr<BlockHandle>& null_bitmap_handle)
        : version_(version),
          bloom_filter_handle_(bloom_filter_handle),
          index_block_handle_(index_block_handle),
          null_bitmap_handle_(null_bitmap_handle) {}

    int32_t GetVersion() const {
        return version_;
    }

    std::shared_ptr<BloomFilterHandle> GetBloomFilterHandle() const {
        return bloom_filter_handle_;
    }

    std::shared_ptr<BlockHandle> GetIndexBlockHandle() const {
        return index_block_handle_;
    }

    std::shared_ptr<BlockHandle> GetNullBitmapHandle() const {
        return null_bitmap_handle_;
    }

 public:
    static constexpr int32_t kMagicNumber = 0x50425449;
    static constexpr int32_t CURRENT_VERSION = 1;
    static constexpr int32_t ENCODED_LENGTH = 52;

 private:
    int32_t version_;
    std::shared_ptr<BloomFilterHandle> bloom_filter_handle_;
    std::shared_ptr<BlockHandle> index_block_handle_;
    std::shared_ptr<BlockHandle> null_bitmap_handle_;
};

}  // namespace paimon
