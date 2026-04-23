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

#include "paimon/common/lookup/lookup_store_factory.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/sst/sst_file_reader.h"
#include "paimon/common/sst/sst_file_writer.h"
#include "paimon/common/utils/bloom_filter.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
namespace paimon {
/// Reader, lookup value by key bytes.
class SortLookupStoreReader : public LookupStoreReader {
 public:
    SortLookupStoreReader(const std::shared_ptr<InputStream>& in,
                          const std::shared_ptr<SstFileReader>& reader)
        : in_(in), reader_(reader) {}

    Result<std::shared_ptr<Bytes>> Lookup(const std::shared_ptr<Bytes>& key) const override {
        return reader_->Lookup(key);
    }

    Status Close() override;

 private:
    std::shared_ptr<InputStream> in_;
    std::shared_ptr<SstFileReader> reader_;
};

/// Writer to prepare binary file.
class SortLookupStoreWriter : public LookupStoreWriter {
 public:
    SortLookupStoreWriter(const std::shared_ptr<OutputStream>& out,
                          const std::shared_ptr<SstFileWriter>& writer,
                          const std::shared_ptr<MemoryPool>& pool)
        : out_(out), writer_(writer), pool_(pool) {}

    Status Put(std::shared_ptr<Bytes>&& key, std::shared_ptr<Bytes>&& value) override {
        return writer_->Write(std::move(key), std::move(value));
    }

    Status Close() override;

 private:
    std::shared_ptr<OutputStream> out_;
    std::shared_ptr<SstFileWriter> writer_;
    std::shared_ptr<MemoryPool> pool_;
};

/// A `LookupStoreFactory` which uses hash to lookup records on disk.
class SortLookupStoreFactory : public LookupStoreFactory {
 public:
    SortLookupStoreFactory(MemorySlice::SliceComparator comparator,
                           const std::shared_ptr<CacheManager>& cache_manager, int32_t block_size,
                           const std::shared_ptr<BlockCompressionFactory>& compression_factory)
        : block_size_(block_size),
          comparator_(std::move(comparator)),
          cache_manager_(cache_manager),
          compression_factory_(compression_factory) {}

    Result<std::unique_ptr<LookupStoreWriter>> CreateWriter(
        const std::shared_ptr<paimon::FileSystem>& fs, const std::string& file_path,
        const std::shared_ptr<BloomFilter>& bloom_filter,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::unique_ptr<LookupStoreReader>> CreateReader(
        const std::shared_ptr<paimon::FileSystem>& fs, const std::string& file_path,
        const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    int32_t block_size_;
    MemorySlice::SliceComparator comparator_;
    std::shared_ptr<CacheManager> cache_manager_;
    std::shared_ptr<BlockCompressionFactory> compression_factory_;
};
}  // namespace paimon
