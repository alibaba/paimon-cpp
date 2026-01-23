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
// https://github.com/apache/orc/blob/main/c%2B%2B/src/io/Cache.hh

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {

/// Configuration parameters for the read-ahead cache behavior.
///
/// This struct controls various limits and prefetching strategies used by
/// ReadAheadCache to balance memory usage, I/O efficiency, and latency hiding.
class PAIMON_EXPORT CacheConfig {
 public:
    CacheConfig();
    CacheConfig(uint64_t buffer_size_limit, uint64_t range_size_limit, uint64_t hole_size_limit,
                uint32_t pre_buffer_range_count);

    /// Returns the maximum total size (in bytes) of cached data.
    uint64_t GetBufferSizeLimit() const {
        return buffer_size_limit_;
    }

    /// Returns the maximum allowed size (in bytes) for a single cached range.
    uint64_t GetRangeSizeLimit() const {
        return range_size_limit_;
    }

    /// Returns the maximum gap size (in bytes) considered mergeable between adjacent ranges.
    uint64_t GetHoleSizeLimit() const {
        return hole_size_limit_;
    }

    /// Returns the number of ranges to pre-buffer ahead of the current read position.
    uint32_t GetPreBufferRangeCount() const {
        return pre_buffer_range_count_;
    }

 private:
    uint64_t buffer_size_limit_;
    uint64_t range_size_limit_;
    uint64_t hole_size_limit_;
    uint32_t pre_buffer_range_count_;
};

/// A byte range with offset and length.
struct PAIMON_EXPORT ByteRange {
    uint64_t offset;
    uint64_t length;

    ByteRange() = default;
    ByteRange(uint64_t offset, uint64_t length) : offset(offset), length(length) {}

    friend bool operator==(const ByteRange& left, const ByteRange& right) {
        return (left.offset == right.offset && left.length == right.length);
    }
    friend bool operator!=(const ByteRange& left, const ByteRange& right) {
        return !(left == right);
    }

    /// @param other The other byte range to check.
    /// @return true if this range contains the other range
    bool Contains(const ByteRange& other) const {
        return (offset <= other.offset && offset + length >= other.offset + other.length);
    }
};

/// A byte slice with buffer, offset and length.
struct PAIMON_EXPORT ByteSlice {
    std::shared_ptr<Bytes> buffer = nullptr;
    uint64_t offset = 0;
    uint64_t length = 0;
};

/// A read cache designed to hide IO latencies when reading.
/// Prefetching strategy: When a range is read, the cache will prefetch up to
/// `pre_buffer_range_count` additional adjacent ranges ahead of the requested offset. This helps
/// hide I/O latency for sequential access. Example: If you read range [0, 100), and
/// pre_buffer_range_count=2, the next two configured ranges will also be prefetched.
///
/// Eviction policy: The cache uses a simple FIFO eviction policy based on total cached byte size.
/// When adding new ranges would exceed `buffer_size_limit`, the oldest cached ranges are evicted
/// first until there is enough space for the new data.
class PAIMON_EXPORT ReadAheadCache {
 public:
    /// Construct a read cache with given options
    ReadAheadCache(const std::shared_ptr<InputStream>& stream, const CacheConfig& config,
                   const std::shared_ptr<MemoryPool>& memory_pool);
    ~ReadAheadCache();

    /// Initialize the cache with given byte ranges to be cached.
    /// @param ranges The byte ranges to be cached.
    /// @return Status of the operation.
    /// @note This method must be called before any Read() calls. Ranges will be coalesced based
    /// on the cache configuration.
    Status Init(std::vector<ByteRange>&& ranges);

    /// Read a range previously provided to Init().
    /// @param range The byte range to read.
    /// @return The byte slice containing the requested data. If the data is not yet cached
    /// (cache miss), the returned `ByteSlice` will have a null buffer (`buffer == nullptr`)
    Result<ByteSlice> Read(const ByteRange& range);

    /// Reset the cache to its initial state, clearing all cached data and configuration.
    ///
    /// This method waits for all ongoing asynchronous read operations to complete,
    /// clears all cached entries, and resets the internal state so that Init() can be called again.
    /// After calling Reset, the cache can be safely re-initialized with new ranges.
    void Reset();

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
