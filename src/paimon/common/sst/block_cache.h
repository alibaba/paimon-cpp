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

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "paimon/common/io/cache/cache_manager.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/fs/file_system.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
namespace paimon {

class BlockCache {
 public:
    BlockCache(const std::string& file_path, const std::shared_ptr<InputStream>& in,
               const std::shared_ptr<CacheManager>& cache_manager,
               const std::shared_ptr<MemoryPool>& pool)
        : pool_(pool), file_path_(file_path), in_(in), cache_manager_(cache_manager) {}

    Result<MemorySegment> GetBlock(
        int64_t position, int32_t length, bool is_index,
        std::function<Result<MemorySegment>(const MemorySegment&)> decompress_func) {
        auto key = CacheKey::ForPosition(file_path_, position, length, is_index);
        auto it = blocks_.find(key);
        if (it == blocks_.end() || it->second.GetAccessCount() == CacheManager::REFRESH_COUNT) {
            PAIMON_ASSIGN_OR_RAISE(
                MemorySegment segment,
                cache_manager_->GetPage(
                    key,
                    [&](const std::shared_ptr<paimon::CacheKey>&) -> Result<MemorySegment> {
                        PAIMON_ASSIGN_OR_RAISE(MemorySegment compress_data,
                                               ReadFrom(position, length));
                        if (!decompress_func) {
                            return compress_data;
                        }
                        return decompress_func(compress_data);
                    },
                    [this](const std::shared_ptr<CacheKey>& evicted_key) {
                        blocks_.erase(evicted_key);
                    }));
            auto container = CacheManager::SegmentContainer(segment);
            const auto& result_segment = container.Access();
            blocks_.insert_or_assign(key, container);
            return result_segment;
        }
        return it->second.Access();
    }

    /// Returns the number of entries in the local blocks_ cache.
    size_t BlocksSize() const {
        return blocks_.size();
    }

    /// Returns true if the local blocks_ cache contains an entry for the given position/length.
    bool ContainsBlock(int64_t position, int32_t length, bool is_index) const {
        auto key = CacheKey::ForPosition(file_path_, position, length, is_index);
        return blocks_.find(key) != blocks_.end();
    }

    void Close() {
        // Snapshot blocks_ to avoid iterator invalidation from `InvalidPage` callback.
        auto copied_blocks = blocks_;
        for (const auto& [key, _] : copied_blocks) {
            cache_manager_->InvalidPage(key);
        }
        assert(blocks_.empty());
    }

 private:
    Result<MemorySegment> ReadFrom(int64_t offset, int length) {
        PAIMON_RETURN_NOT_OK(in_->Seek(offset, SeekOrigin::FS_SEEK_SET));
        auto segment = MemorySegment::AllocateHeapMemory(length, pool_.get());
        PAIMON_RETURN_NOT_OK(in_->Read(segment.GetHeapMemory()->data(), length));
        return segment;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::string file_path_;
    std::shared_ptr<InputStream> in_;

    std::shared_ptr<CacheManager> cache_manager_;
    std::unordered_map<std::shared_ptr<CacheKey>, CacheManager::SegmentContainer, CacheKeyHash,
                       CacheKeyEqual>
        blocks_;
};
}  // namespace paimon
