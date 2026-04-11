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

#include "paimon/core/mergetree/lookup_file.h"

#include "fmt/format.h"
#include "paimon/common/utils/binary_row_partition_computer.h"

namespace paimon {
LookupFile::LookupFile(const std::shared_ptr<FileSystem>& fs, const std::string& local_file,
                       int64_t file_size_bytes, int32_t level, int64_t schema_id,
                       const std::string& ser_version, std::unique_ptr<LookupStoreReader>&& reader,
                       Callback callback)
    : fs_(fs),
      local_file_(local_file),
      file_size_bytes_(file_size_bytes),
      level_(level),
      schema_id_(schema_id),
      ser_version_(ser_version),
      reader_(std::move(reader)),
      callback_(std::move(callback)) {}

LookupFile::~LookupFile() {
    if (!closed_) {
        [[maybe_unused]] auto status = Close();
    }
}

Result<std::shared_ptr<Bytes>> LookupFile::GetResult(const std::shared_ptr<Bytes>& key) {
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

Status LookupFile::Close() {
    closed_ = true;
    if (callback_) {
        callback_();
    }
    PAIMON_RETURN_NOT_OK(reader_->Close());
    return fs_->Delete(local_file_, /*recursive=*/false);
}

int64_t LookupFile::FileWeigh(const std::string& /*file_name*/,
                              const std::shared_ptr<LookupFile>& lookup_file) {
    if (!lookup_file || lookup_file->IsClosed()) {
        return 0;
    }
    return lookup_file->file_size_bytes_;
}

void LookupFile::RemovalCallback(const std::string& /*file_name*/,
                                 const std::shared_ptr<LookupFile>& lookup_file,
                                 LookupFile::LookupFileCache::RemovalCause /*cause*/) {
    if (lookup_file && !lookup_file->IsClosed()) {
        [[maybe_unused]] auto status = lookup_file->Close();
    }
}

std::shared_ptr<LookupFile::LookupFileCache> LookupFile::CreateLookupFileCache(
    int64_t file_retention_ms, int64_t max_disk_size) {
    LookupFile::LookupFileCache::Options options;
    options.expire_after_access_ms = file_retention_ms;
    options.max_weight = max_disk_size;
    options.weigh_func = &LookupFile::FileWeigh;
    options.removal_callback = &LookupFile::RemovalCallback;
    return std::make_shared<LookupFile::LookupFileCache>(std::move(options));
}

Result<std::string> LookupFile::LocalFilePrefix(
    const std::shared_ptr<arrow::Schema>& partition_type, const BinaryRow& partition,
    int32_t bucket, const std::string& remote_file_name) {
    if (partition.GetFieldCount() == 0) {
        return fmt::format("{}-{}", std::to_string(bucket), remote_file_name);
    } else {
        PAIMON_ASSIGN_OR_RAISE(std::string part_str, BinaryRowPartitionComputer::PartToSimpleString(
                                                         partition_type, partition,
                                                         /*delimiter=*/"-", /*max_length=*/20));
        return fmt::format("{}-{}-{}", part_str, bucket, remote_file_name);
    }
}

}  // namespace paimon
