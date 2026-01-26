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

#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/sst/block_footer.h"
#include "paimon/common/sst/block_handle.h"
#include "paimon/common/sst/block_trailer.h"
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

/// The writer for writing SST Files. SST Files are row-oriented and designed to serve frequent
/// point queries and range queries by key.
class SstFileWriter {
 public:
    SstFileWriter(const std::shared_ptr<OutputStream>& out, const std::shared_ptr<MemoryPool>& pool,
                  const std::shared_ptr<BloomFilter>& bloom_filter, int32_t block_size,
                  const std::shared_ptr<BlockCompressionFactory>& factory);

    ~SstFileWriter() = default;

    Status Write(std::shared_ptr<Bytes>&& key, std::shared_ptr<Bytes>&& value);

    Status Write(std::shared_ptr<MemorySlice>& slice);

    Status Flush();

    Result<std::shared_ptr<BlockHandle>> WriteIndexBlock();

    Result<std::shared_ptr<BloomFilterHandle>> WriteBloomFilter();

    Status WriteFooter(const std::shared_ptr<BlockHandle>& index_block_handle,
                       const std::shared_ptr<BloomFilterHandle>& bloom_filter_handle);

 private:
    Result<std::shared_ptr<BlockHandle>> FlushBlockWriter(std::unique_ptr<BlockWriter>& writer);

    Status WriteBytes(const char* data, size_t size);

    Result<int32_t> WriteVarLenInt(char* bytes, int32_t value);

    // api for testing
    BlockWriter* IndexWriter() const {
        return index_block_writer_.get();
    }

 private:
    const std::shared_ptr<OutputStream> out_;

    const std::shared_ptr<MemoryPool> pool_;

    std::shared_ptr<BloomFilter> bloom_filter_;

    BlockCompressionType compression_type_;
    std::shared_ptr<BlockCompressor> compressor_;

    std::shared_ptr<Bytes> last_key_;

    int32_t block_size_;
    std::unique_ptr<BlockWriter> data_block_writer_;
    std::unique_ptr<BlockWriter> index_block_writer_;
};
}  // namespace paimon
