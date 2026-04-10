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
#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/lookup/lookup_store_factory.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
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

    LookupFile(const std::shared_ptr<FileSystem>& fs, const std::string& local_file, int32_t level,
               int64_t schema_id, const std::string& ser_version,
               std::unique_ptr<LookupStoreReader>&& reader, int64_t file_size_bytes = 0,
               Callback callback = nullptr)
        : fs_(fs),
          local_file_(local_file),
          level_(level),
          schema_id_(schema_id),
          ser_version_(ser_version),
          reader_(std::move(reader)),
          file_size_bytes_(file_size_bytes),
          callback_(std::move(callback)) {}

    ~LookupFile() {
        if (!closed_) {
            [[maybe_unused]] auto status = Close();
        }
    }

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
        return local_file_ == other.local_file_;
    }

    Result<std::shared_ptr<Bytes>> GetResult(const std::shared_ptr<Bytes>& key) {
        if (closed_) {
            return Status::Invalid("GetResult failed in LookupFile, reader is closed");
        }
        request_count_++;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> res, reader_->Lookup(key));
        if (res) {
            hit_count_++;
        }
        return res;
    }

    Status Close() {
        closed_ = true;
        if (callback_) {
            callback_();
        }
        PAIMON_RETURN_NOT_OK(reader_->Close());
        return fs_->Delete(local_file_, /*recursive=*/false);
    }

    /// Compute the weight of a lookup file in bytes for cache eviction.
    static int64_t FileWeigh(const std::string& /*file_name*/,
                             const std::shared_ptr<LookupFile>& lookup_file) {
        if (!lookup_file || lookup_file->IsClosed()) {
            return 0;
        }
        return lookup_file->file_size_bytes_;
    }

    /// Removal callback for the global LookupFileCache.
    static void RemovalCallback(const std::string& /*file_name*/,
                                const std::shared_ptr<LookupFile>& lookup_file,
                                LookupFileCache::RemovalCause /*cause*/) {
        if (lookup_file && !lookup_file->IsClosed()) {
            [[maybe_unused]] auto status = lookup_file->Close();
        }
    }

    /// Create a global LookupFileCache with the given retention and max disk size (in bytes).
    static std::shared_ptr<LookupFileCache> CreateLookupFileCache(int64_t file_retention_ms,
                                                                  int64_t max_disk_size) {
        LookupFileCache::Options options;
        options.expire_after_access_ms = file_retention_ms;
        options.max_weight = max_disk_size;
        options.weigh_func = &LookupFile::FileWeigh;
        options.removal_callback = &LookupFile::RemovalCallback;
        return std::make_shared<LookupFileCache>(std::move(options));
    }

    static Result<std::string> LocalFilePrefix(const std::shared_ptr<arrow::Schema>& partition_type,
                                               const BinaryRow& partition, int32_t bucket,
                                               const std::string& remote_file_name) {
        if (partition.GetFieldCount() == 0) {
            return fmt::format("{}-{}", std::to_string(bucket), remote_file_name);
        } else {
            PAIMON_ASSIGN_OR_RAISE(
                std::string part_str,
                BinaryRowPartitionComputer::PartToSimpleString(
                    partition_type, partition, /*delimiter=*/"-", /*max_length=*/20));
            return fmt::format("{}-{}-{}", part_str, bucket, remote_file_name);
        }
    }

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string local_file_;
    int32_t level_;
    int64_t schema_id_;
    std::string ser_version_;
    std::unique_ptr<LookupStoreReader> reader_;
    int64_t file_size_bytes_ = 0;
    Callback callback_;
    int64_t request_count_ = 0;
    int64_t hit_count_ = 0;
    bool closed_ = false;
};

}  // namespace paimon
