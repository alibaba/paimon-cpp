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
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon {
class MemoryPool;

class SstFileWriter {
 public:
    SstFileWriter(std::shared_ptr<OutputStream> out, int32_t block_size,
                  const std::shared_ptr<MemoryPool>& pool)
        : out_(out), block_size_(block_size), pool_(pool) {}

    ~SstFileWriter() = default;

    Status Write(std::shared_ptr<Bytes>& key, std::shared_ptr<Bytes>& value);

    Status Write(std::shared_ptr<Bytes>&& key, std::shared_ptr<Bytes>&& value);

    Status Flush();

    Result<std::shared_ptr<BlockHandle>> WriteIndexBlock();

    Result<std::shared_ptr<BloomFilterHandle>> WriteBloomFilter();

 private:
    Result<std::shared_ptr<BlockHandle>> FlushBlockWriter(std::unique_ptr<BlockWriter>& writer);

    Status WriteBytes(std::shared_ptr<Bytes>& bytes);

 private:
    const std::shared_ptr<OutputStream> out_;
    const std::shared_ptr<MemoryPool>& pool_;

    std::shared_ptr<Bytes> last_key_;

    // Nullable
    std::shared_ptr<BloomFilter> bloom_filter_;

    int32_t block_size_;
    int64_t record_count_;

    std::unique_ptr<BlockWriter> data_block_writer_;
    std::unique_ptr<BlockWriter> index_block_writer_;
};
}  // namespace paimon
