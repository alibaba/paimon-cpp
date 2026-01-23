/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Adapted from Apache ORC
// https://github.com/apache/orc/blob/main/c%2B%2B/src/io/Cache.cc

#include "paimon/utils/read_ahead_cache.h"

#include <algorithm>
#include <cassert>
#include <future>
#include <shared_mutex>

#include "paimon/common/utils/byte_range_combiner.h"

namespace paimon {

struct RangeCacheEntry {
    ByteRange range;
    std::shared_ptr<Bytes> buffer;
    std::shared_future<Status> future;  // use shared_future in case of multiple get calls

    RangeCacheEntry() = default;
    RangeCacheEntry(const ByteRange& range, std::shared_ptr<Bytes> buffer,
                    std::future<Status> future)
        : range(range), buffer(std::move(buffer)), future(std::move(future).share()) {}

    friend bool operator<(const RangeCacheEntry& left, const RangeCacheEntry& right) {
        return left.range.offset < right.range.offset;
    }
};

CacheConfig::CacheConfig(uint64_t buffer_size_limit, uint64_t range_size_limit,
                         uint64_t hole_size_limit, uint32_t pre_buffer_range_count)
    : buffer_size_limit_(buffer_size_limit),
      range_size_limit_(range_size_limit),
      hole_size_limit_(hole_size_limit),
      pre_buffer_range_count_(pre_buffer_range_count) {}

CacheConfig::CacheConfig()
    : CacheConfig(/*buffer_size_limit=*/512 * 1024 * 1024,
                  /*range_size_limit=*/16 * 1024 * 1024,
                  /*hole_size_limit=*/8 * 1024,
                  /*pre_buffer_range_count=*/6) {}

class ReadAheadCache::Impl {
 public:
    Impl(const std::shared_ptr<InputStream>& stream, const CacheConfig& config,
         const std::shared_ptr<MemoryPool>& memory_pool);
    ~Impl();

    Status Init(std::vector<ByteRange>&& ranges);
    Result<ByteSlice> Read(const ByteRange& range);
    void Reset();

 private:
    std::vector<RangeCacheEntry> MakeCacheEntries(const std::vector<ByteRange>& ranges) const;
    void PreBuffer(uint64_t offset, size_t n_extra);

    /// Cache the given ranges in the background.
    ///
    /// The caller must ensure that the ranges do not overlap with each other,
    /// nor with previously cached ranges.  Otherwise, behaviour will be undefined.
    void Cache(std::vector<ByteRange> ranges);

    std::shared_ptr<InputStream> stream_;
    CacheConfig config_;
    // Ordered by offset (so as to find a matching region by binary search)
    std::vector<RangeCacheEntry> entries_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_mutex rw_mutex_;
    std::vector<std::atomic<bool>> is_cached_;
    std::vector<ByteRange> pending_ranges_;
    bool is_initialized_ = false;
};

void ReadAheadCache::Impl::Cache(std::vector<ByteRange> ranges) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    std::sort(ranges.begin(), ranges.end(),
              [](const ByteRange& a, const ByteRange& b) { return a.offset < b.offset; });
    std::vector<RangeCacheEntry> new_entries = MakeCacheEntries(ranges);
    // Add new entries, themselves ordered by offset
    if (entries_.size() > 0) {
        size_t new_entries_size = 0;
        for (const auto& e : new_entries) {
            new_entries_size += e.range.length;
        }

        size_t total_size = 0;
        for (const auto& e : entries_) {
            total_size += e.range.length;
        }
        size_t limit = config_.GetBufferSizeLimit();
        while (!entries_.empty() && total_size + new_entries_size > limit) {
            auto iter = entries_.begin();
            total_size -= entries_.front().range.length;
            entries_.erase(iter);
        }

        std::vector<RangeCacheEntry> merged(entries_.size() + new_entries.size());
        std::merge(entries_.begin(), entries_.end(), new_entries.begin(), new_entries.end(),
                   merged.begin());
        entries_ = std::move(merged);
    } else {
        entries_ = std::move(new_entries);
    }
}

Status ReadAheadCache::Impl::Init(std::vector<ByteRange>&& ranges) {
    if (is_initialized_) {
        return Status::Invalid("Cache has already been initialized");
    }
    if (config_.GetRangeSizeLimit() > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
        return Status::Invalid("CacheConfig range_size_limit exceeds uint32_t max");
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::vector<ByteRange> pending_ranges,
        ByteRangeCombiner::CoalesceByteRanges(std::move(ranges), config_.GetHoleSizeLimit(),
                                              config_.GetRangeSizeLimit()));
    for (const auto& pending_range : pending_ranges) {
        if (pending_range.length > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
            return Status::Invalid("range length should not be larger than uint32_t max");
        }
    }
    pending_ranges_ = pending_ranges;
    is_cached_ = std::vector<std::atomic<bool>>(pending_ranges_.size());
    for (auto& is_cached : is_cached_) {
        is_cached.store(false);
    }
    is_initialized_ = true;
    return Status::OK();
}

void ReadAheadCache::Impl::PreBuffer(uint64_t offset, size_t n_extra) {
    auto it = std::lower_bound(pending_ranges_.begin(), pending_ranges_.end(), offset,
                               [](const ByteRange& range, uint64_t offset) {
                                   return range.offset + range.length <= offset;
                               });
    if (it == pending_ranges_.end() || it->offset > offset) {
        return;
    }

    size_t start_idx = std::distance(pending_ranges_.begin(), it);
    size_t end_idx = std::min(pending_ranges_.size(), start_idx + 1 + n_extra);

    std::vector<ByteRange> ranges;
    for (size_t i = start_idx; i < end_idx; ++i) {
        if (!is_cached_[i].exchange(true)) {
            ranges.emplace_back(pending_ranges_[i]);
        }
    }

    if (!ranges.empty()) {
        Cache(std::move(ranges));
    }
}

ReadAheadCache::Impl::Impl(const std::shared_ptr<InputStream>& stream, const CacheConfig& config,
                           const std::shared_ptr<MemoryPool>& memory_pool)
    : stream_(stream), config_(config), memory_pool_(memory_pool) {}

ReadAheadCache::Impl::~Impl() {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    for (auto& entry : entries_) {
        entry.future.wait();
    }
}

void ReadAheadCache::Impl::Reset() {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    for (auto& entry : entries_) {
        entry.future.wait();
    }
    entries_.clear();
    is_cached_.clear();
    pending_ranges_.clear();
    is_initialized_ = false;
}

Result<ByteSlice> ReadAheadCache::Impl::Read(const ByteRange& range) {
    if (range.length == 0) {
        return ByteSlice{std::make_shared<Bytes>(0, memory_pool_.get()), 0, 0};
    }
    PreBuffer(range.offset, config_.GetPreBufferRangeCount());
    ByteSlice result{};
    {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        auto it = std::lower_bound(entries_.begin(), entries_.end(), range.offset,
                                   [](const RangeCacheEntry& e, uint64_t offset) {
                                       return e.range.offset + e.range.length <= offset;
                                   });
        if (it != entries_.end() && it->range.Contains(range)) {
            PAIMON_RETURN_NOT_OK(it->future.get());
            result = ByteSlice{it->buffer, range.offset - it->range.offset, range.length};
            return result;
        }
    }
    return result;
}

std::vector<RangeCacheEntry> ReadAheadCache::Impl::MakeCacheEntries(
    const std::vector<ByteRange>& ranges) const {
    std::vector<RangeCacheEntry> new_entries;
    new_entries.reserve(ranges.size());
    for (const auto& range : ranges) {
        auto promise = std::make_shared<std::promise<Status>>();
        auto future = promise->get_future();
        auto buffer = std::make_shared<Bytes>(range.length, memory_pool_.get());
        stream_->ReadAsync(
            buffer->data(), static_cast<uint32_t>(buffer->size()), range.offset,
            [promise, buffer](Status status) mutable { promise->set_value(status); });
        new_entries.emplace_back(range, std::move(buffer), std::move(future));
    }
    return new_entries;
}

ReadAheadCache::ReadAheadCache(const std::shared_ptr<InputStream>& stream,
                               const CacheConfig& config,
                               const std::shared_ptr<MemoryPool>& memory_pool)
    : impl_(std::make_unique<Impl>(stream, config, memory_pool)) {}

ReadAheadCache::~ReadAheadCache() = default;

Status ReadAheadCache::Init(std::vector<ByteRange>&& ranges) {
    return impl_->Init(std::move(ranges));
}

Result<ByteSlice> ReadAheadCache::Read(const ByteRange& range) {
    return impl_->Read(range);
}

void ReadAheadCache::Reset() {
    return impl_->Reset();
}

}  // namespace paimon
