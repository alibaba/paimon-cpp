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

#include "paimon/common/sst/block_cache.h"
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

class SstFileReader {
 public:
    SstFileReader(const std::shared_ptr<MemoryPool>& pool, std::unique_ptr<BlockCache> block_cache,
                  std::shared_ptr<BlockHandle> index_block_handle,
                  std::unique_ptr<BloomFilter> bloom_filter,
                  std::function<int32_t(const std::shared_ptr<MemorySlice>&,
                                        const std::shared_ptr<MemorySlice>&)>
                      comparator);
    ~SstFileReader() = default;

    std::unique_ptr<SstFileIterator> CreateIterator();

    std::shared_ptr<Bytes> Lookup(std::shared_ptr<Bytes>& key);

    std::unique_ptr<BlockIterator> GetNextBlock(std::unique_ptr<BlockIterator>& index_iterator);

    std::unique_ptr<BlockReader> ReadBlock(std::shared_ptr<BlockHandle>&& handle, bool index);

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<BlockCache> block_cache_;
    std::unique_ptr<BloomFilter> bloom_filter_;
    std::unique_ptr<BlockReader> index_block_reader_;
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
    void SeekTo(std::shared_ptr<Bytes>& key);

 private:
    SstFileReader* reader_;
    std::unique_ptr<BlockIterator> index_iterator_;
    std::unique_ptr<BlockIterator> data_iterator_;
};
}  // namespace paimon
