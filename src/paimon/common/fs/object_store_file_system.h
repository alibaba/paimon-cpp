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
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "paimon/fs/file_system.h"
#include "paimon/visibility.h"

namespace paimon {

struct ObjectStorePath {
    std::string bucket;
    std::string key;
};

struct ObjectMetadata {
    std::string key;
    int64_t size = 0;
    int64_t modification_time = 0;
};

struct ListObjectsResult {
    std::vector<ObjectMetadata> objects;
    std::vector<std::string> common_prefixes;
    std::string continuation_token;
    bool is_truncated = false;
};

class PAIMON_EXPORT ObjectStoreClient {
 public:
    virtual ~ObjectStoreClient() = default;

    virtual Result<ObjectMetadata> HeadObject(const ObjectStorePath& path) const = 0;
    virtual Result<ListObjectsResult> ListObjects(const ObjectStorePath& path,
                                                  const std::string& continuation_token,
                                                  int32_t max_keys) const = 0;
    virtual Result<int64_t> GetObjectRange(const ObjectStorePath& path, int64_t offset,
                                           int64_t size, char* buffer) const = 0;
    virtual void GetObjectRangeAsync(const ObjectStorePath& path, int64_t offset, int64_t size,
                                     char* buffer,
                                     std::function<void(Status)>&& callback) const = 0;
};

class ReadAheadMemoryLimiter {
 public:
    explicit ReadAheadMemoryLimiter(int64_t limit);

    int64_t ReserveUpTo(int64_t min_size, int64_t max_size);
    void Release(int64_t size);

 private:
    int64_t limit_;
    int64_t used_ = 0;
    std::mutex mutex_;
};

class PAIMON_EXPORT ObjectStoreFileSystem : public FileSystem {
 public:
    ObjectStoreFileSystem(std::string scheme, std::shared_ptr<ObjectStoreClient> client);
    ObjectStoreFileSystem(std::string scheme, std::shared_ptr<ObjectStoreClient> client,
                          int64_t read_ahead_memory_limit);
    ~ObjectStoreFileSystem() override = default;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override;
    Result<std::unique_ptr<FileStatus>> GetFileStatus(const std::string& path) const override;
    Status ListDir(const std::string& directory,
                   std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) const override;
    Status ListFileStatus(
        const std::string& path,
        std::vector<std::unique_ptr<FileStatus>>* file_status_list) const override;
    Result<bool> Exists(const std::string& path) const override;

    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override;
    Status Mkdirs(const std::string& path) const override;
    Status Rename(const std::string& src, const std::string& dst) const override;
    Status Delete(const std::string& path, bool recursive = true) const override;
    Status WriteFile(const std::string& path, const std::string& content, bool overwrite) override;
    Status AtomicStore(const std::string& path, const std::string& content) override;

 protected:
    Result<ObjectStorePath> ParsePath(const std::string& path) const;
    std::string ToUri(const ObjectStorePath& path, bool is_dir = false) const;

 private:
    Result<bool> DirectoryExists(const ObjectStorePath& path) const;
    Status ListDirectory(const ObjectStorePath& path,
                         std::vector<std::unique_ptr<BasicFileStatus>>* basic_statuses,
                         std::vector<std::unique_ptr<FileStatus>>* statuses) const;
    Status ReadOnlyStatus() const;

    std::string scheme_;
    std::shared_ptr<ObjectStoreClient> client_;
    std::shared_ptr<ReadAheadMemoryLimiter> read_ahead_limiter_;
};

}  // namespace paimon
