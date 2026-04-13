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
#include "paimon/common/data/binary_row.h"
#include "paimon/common/lookup/lookup_store_factory.h"
#include "paimon/common/utils/generic_lru_cache.h"
#include "paimon/fs/file_system.h"

namespace paimon {
/// Lookup file for cache remote file to local.
class LookupFile {
 public:
    using Callback = std::function<void()>;
    /// Type alias for the global lookup file cache.
    /// Key: data file name (string), Value: shared_ptr<LookupFile>
    /// Weight is measured in bytes (file size on disk).
    using LookupFileCache = GenericLruCache<std::string, std::shared_ptr<LookupFile>>;

    LookupFile(const std::shared_ptr<FileSystem>& fs, const std::string& local_file,
               int64_t file_size_bytes, int32_t level, int64_t schema_id,
               const std::string& ser_version, std::unique_ptr<LookupStoreReader>&& reader,
               Callback callback);

    ~LookupFile();

    const std::string& LocalFile() const {
        return local_file_;
    }

    int64_t SchemaId() const {
        return schema_id_;
    }

    const std::string& SerVersion() const {
        return ser_version_;
    }

    int32_t Level() const {
        return level_;
    }

    bool IsClosed() const {
        return closed_;
    }

    bool operator==(const LookupFile& other) const {
        if (this == &other) {
            return true;
        }
        return local_file_ == other.local_file_;
    }

    Result<std::shared_ptr<Bytes>> GetResult(const std::shared_ptr<Bytes>& key);

    Status Close();

    /// Create a global LookupFileCache with the given retention and max disk size (in bytes).
    static std::shared_ptr<LookupFileCache> CreateLookupFileCache(int64_t file_retention_ms,
                                                                  int64_t max_disk_size);

    static Result<std::string> LocalFilePrefix(const std::shared_ptr<arrow::Schema>& partition_type,
                                               const BinaryRow& partition, int32_t bucket,
                                               const std::string& remote_file_name);

 private:
    /// Compute the weight of a lookup file in bytes for cache eviction.
    static int64_t FileWeigh(const std::string& file_name,
                             const std::shared_ptr<LookupFile>& lookup_file);

    /// Removal callback for the global LookupFileCache.
    static void RemovalCallback(const std::string& file_name,
                                const std::shared_ptr<LookupFile>& lookup_file,
                                LookupFileCache::RemovalCause cause);

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string local_file_;
    int64_t file_size_bytes_ = 0;
    int32_t level_;
    int64_t schema_id_;
    std::string ser_version_;
    std::unique_ptr<LookupStoreReader> reader_;
    Callback callback_;
    int64_t request_count_ = 0;
    int64_t hit_count_ = 0;
    bool closed_ = false;
};

}  // namespace paimon
