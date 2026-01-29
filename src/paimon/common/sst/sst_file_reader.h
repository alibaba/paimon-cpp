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
#include "paimon/common/sst/block_cache.h"
#include "paimon/common/sst/block_footer.h"
#include "paimon/common/sst/block_handle.h"
#include "paimon/common/sst/block_iterator.h"
#include "paimon/common/sst/block_reader.h"
#include "paimon/common/sst/block_trailer.h"
#include "paimon/common/sst/bloom_filter_handle.h"
#include "paimon/common/utils/bit_set.h"
#include "paimon/common/utils/bloom_filter.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"

namespace paimon {
class SstFileIterator;

/// An SST File Reader which serves point queries and range queries. Users can call
/// CreateIterator() to create a file iterator and then use seek and read methods to do range
/// queries. Note that this class is NOT thread-safe.
class SstFileReader {
 public:
    static Result<std::shared_ptr<SstFileReader>> Create(
        const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<paimon::FileSystem>& fs,
        const std::string& file_path,
        std::function<int32_t(const std::shared_ptr<MemorySlice>&,
                              const std::shared_ptr<MemorySlice>&)>
            comparator);

    SstFileReader(const std::shared_ptr<MemoryPool>& pool,
                  const std::shared_ptr<BlockCache>& block_cache,
                  const std::shared_ptr<BloomFilter>& bloom_filter,
                  const std::shared_ptr<paimon::BlockReader>& index_block_reader,
                  std::function<int32_t(const std::shared_ptr<MemorySlice>&,
                                        const std::shared_ptr<MemorySlice>&)>
                      comparator);
    ~SstFileReader() = default;

    std::unique_ptr<SstFileIterator> CreateIterator();

    /**
     * Lookup the specified key in the file.
     *
     * @param key serialized key
     * @return corresponding serialized value, nullptr if not found.
     */
    std::shared_ptr<Bytes> Lookup(std::shared_ptr<Bytes> key);

    Result<std::unique_ptr<BlockIterator>> GetNextBlock(
        std::unique_ptr<BlockIterator>& index_iterator);

    /**
     * @param handle The block handle.
     * @param index Whether read the block as an index.
     * @return The reader of the target block.
     */
    Result<std::shared_ptr<BlockReader>> ReadBlock(std::shared_ptr<BlockHandle>&& handle,
                                                   bool index);

    /**
     * @param handle The block handle.
     * @param index Whether read the block as an index.
     * @return The reader of the target block.
     */
    Result<std::shared_ptr<BlockReader>> ReadBlock(const std::shared_ptr<BlockHandle>& handle,
                                                   bool index);

 private:
    static Result<std::shared_ptr<paimon::MemorySegment>> DecompressBlock(
        const std::shared_ptr<paimon::MemorySegment>& compressed_data,
        const std::unique_ptr<BlockTrailer>& trailer, const std::shared_ptr<MemoryPool>& pool);

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<BlockCache> block_cache_;
    std::shared_ptr<BloomFilter> bloom_filter_;
    std::shared_ptr<BlockReader> index_block_reader_;
    std::function<int32_t(const std::shared_ptr<MemorySlice>&, const std::shared_ptr<MemorySlice>&)>
        comparator_;
};

class SstFileIterator {
 public:
    SstFileIterator() = default;
    SstFileIterator(SstFileReader* reader, std::unique_ptr<BlockIterator> index_iterator);

    /**
     * Seek to the position of the record whose key is exactly equal to or greater than the
     * specified key.
     */
    Status SeekTo(std::shared_ptr<Bytes>& key);

 private:
    SstFileReader* reader_;
    std::unique_ptr<BlockIterator> index_iterator_;
    std::unique_ptr<BlockIterator> data_iterator_;
};
}  // namespace paimon
