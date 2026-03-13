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
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/utils/bloom_filter.h"
#include "paimon/core/core_options.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
namespace paimon {
/// Reader, lookup value by key bytes.
class LookupStoreReader {
 public:
    virtual ~LookupStoreReader() = default;
    /// Lookup value by key.
    virtual Result<std::shared_ptr<Bytes>> Lookup(const std::shared_ptr<Bytes>& key) const = 0;
    virtual Status Close() = 0;
};

/// Writer to prepare binary file.
class LookupStoreWriter {
 public:
    virtual ~LookupStoreWriter() = default;
    virtual Status Put(std::shared_ptr<Bytes>&& key, std::shared_ptr<Bytes>&& value) = 0;
    virtual Status Close() = 0;
};

/// A key-value store for lookup, key-value store should be single binary file written once and
/// ready to be used. This factory provide two interfaces:
///
///  Writer: written once to prepare binary file.
///  Reader: lookup value by key bytes.
class LookupStoreFactory {
 public:
    virtual ~LookupStoreFactory() = default;

    virtual Result<std::unique_ptr<LookupStoreWriter>> CreateWriter(
        const std::shared_ptr<paimon::FileSystem>& fs, const std::string& file_path,
        const std::shared_ptr<BloomFilter>& bloom_filter,
        const std::shared_ptr<MemoryPool>& pool) const = 0;

    virtual Result<std::unique_ptr<LookupStoreReader>> CreateReader(
        const std::shared_ptr<paimon::FileSystem>& fs, const std::string& file_path,
        const std::shared_ptr<MemoryPool>& pool) const = 0;

    static Result<std::shared_ptr<LookupStoreFactory>> Create(
        MemorySlice::SliceComparator comparator, const CoreOptions& options);

    static Result<std::shared_ptr<BloomFilter>> BfGenerator(int64_t row_count,
                                                            const CoreOptions& options,
                                                            MemoryPool* pool);
};
}  // namespace paimon
