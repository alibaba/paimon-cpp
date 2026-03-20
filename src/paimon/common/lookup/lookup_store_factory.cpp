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

#include "paimon/common/lookup/lookup_store_factory.h"

#include "paimon/common/lookup/sort/sort_lookup_store_factory.h"
namespace paimon {
Result<std::shared_ptr<LookupStoreFactory>> LookupStoreFactory::Create(
    MemorySlice::SliceComparator comparator, const CoreOptions& options) {
    const auto& compress_options = options.GetLookupCompressOptions();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BlockCompressionFactory> compression_factory,
                           BlockCompressionFactory::Create(compress_options));
    return std::make_shared<SortLookupStoreFactory>(
        std::move(comparator), options.GetCachePageSize(), compression_factory);
}

Result<std::shared_ptr<BloomFilter>> LookupStoreFactory::BfGenerator(int64_t row_count,
                                                                     const CoreOptions& options,
                                                                     MemoryPool* pool) {
    if (row_count <= 0 || !options.LookupCacheBloomFilterEnabled()) {
        return std::shared_ptr<BloomFilter>();
    }
    auto bloom_filter = BloomFilter::Create(row_count, options.GetLookupCacheBloomFilterFpp());
    MemorySegment memory_segment =
        MemorySegment::AllocateHeapMemory(bloom_filter->ByteLength(), pool);
    PAIMON_RETURN_NOT_OK(bloom_filter->SetMemorySegment(memory_segment));
    return bloom_filter;
}
}  // namespace paimon
