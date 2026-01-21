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
#include <unordered_map>

#include "paimon/common/io/cache/cache_manager.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/fs/file_system.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace paimon {

class BlockCache {
 public:
    BlockCache(std::string& file_path, const std::shared_ptr<InputStream>& in,
               const std::shared_ptr<MemoryPool>& pool,
               std::unique_ptr<CacheManager>&& cache_manager)
        : file_path_(file_path), in_(in), pool_(pool), cache_manager_(std::move(cache_manager)) {}

    ~BlockCache() = default;

    std::shared_ptr<MemorySegment> GetBlock(int64_t position, int32_t length, bool is_index) {
        auto key = CacheKey::ForPosition(file_path_, position, length, is_index);

        auto it = blocks_.find(key);
        if (it == blocks_.end()) {
            auto segment = cache_manager_->GetPage(
                key, [&](const std::shared_ptr<paimon::CacheKey>&) -> Result<MemorySegment> {
                    return ReadFrom(position, length);
                });
            if (!segment.get()) {
                blocks_.insert({key, std::make_shared<CacheValue>(segment)});
            }
            return segment;
        }
        return it->second->GetSegment();
    }

 private:
    Result<MemorySegment> ReadFrom(int64_t offset, int length) {
        PAIMON_RETURN_NOT_OK(in_->Seek(offset, SeekOrigin::FS_SEEK_SET));
        auto segment = MemorySegment::AllocateHeapMemory(length, pool_.get());
        PAIMON_RETURN_NOT_OK(in_->Read(segment.GetHeapMemory()->data(), length));
        return segment;
    }

 private:
    std::string file_path_;
    std::shared_ptr<InputStream> in_;
    std::shared_ptr<MemoryPool> pool_;

    std::unique_ptr<CacheManager> cache_manager_;
    std::unordered_map<std::shared_ptr<CacheKey>, std::shared_ptr<CacheValue>> blocks_;
};
}  // namespace paimon
