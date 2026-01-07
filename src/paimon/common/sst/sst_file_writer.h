/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/common/sst/block_handle.h"
#include "paimon/common/sst/block_writer.h"
#include "paimon/common/sst/bloom_filter_handle.h"
#include "paimon/common/utils/bit_set.h"
#include "paimon/common/utils/bloom_filter.h"
#include "paimon/common/utils/murmurhash_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon {
class MemoryPool;

/**
 * The writer for writing SST Files. SST Files are row-oriented and designed to serve frequent point
 * queries and range queries by key.
 */
class SstFileWriter {
 public:
    SstFileWriter(std::shared_ptr<OutputStream> out, std::shared_ptr<MemoryPool>& pool,
                  std::shared_ptr<BloomFilter> bloom_filter, int32_t block_size)
        : out_(out), pool_(pool), bloom_filter_(bloom_filter), block_size_(block_size) {
        data_block_writer_ = std::make_unique<BlockWriter>((int32_t)(block_size * 1.1), pool);
        index_block_writer_ =
            std::make_unique<BlockWriter>(BlockHandle::MAX_ENCODED_LENGTH * 1024, pool);
    }

    ~SstFileWriter() = default;

    Status Write(std::shared_ptr<Bytes>& key, std::shared_ptr<Bytes>& value);

    Status Write(std::shared_ptr<Bytes>&& key, std::shared_ptr<Bytes>&& value);

    Status Write(std::shared_ptr<MemorySlice>& slice);

    Status Flush();

    Result<std::shared_ptr<BlockHandle>> WriteIndexBlock();

    Result<std::shared_ptr<BloomFilterHandle>> WriteBloomFilter();

    // For testing
    BlockWriter* IndexWriter() const {
        return index_block_writer_.get();
    }

 private:
    Result<std::shared_ptr<BlockHandle>> FlushBlockWriter(std::unique_ptr<BlockWriter>& writer);

    Status WriteBytes(const char* data, size_t size);

 private:
    const std::shared_ptr<OutputStream> out_;

    std::shared_ptr<Bytes> last_key_;

    const std::shared_ptr<MemoryPool>& pool_;

    std::shared_ptr<BloomFilter> bloom_filter_;

    int32_t block_size_;
    std::unique_ptr<BlockWriter> data_block_writer_;
    std::unique_ptr<BlockWriter> index_block_writer_;
};
}  // namespace paimon
