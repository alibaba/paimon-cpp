/*
 * Copyright 2024-present Alibaba Inc.
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

#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

// `FileSystem` for local file.
class LocalFileSystem : public FileSystem {
 public:
    LocalFileSystem() = default;
    ~LocalFileSystem() override = default;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override;
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override;

    Status Mkdirs(const std::string& path) const override;
    Status Rename(const std::string& src, const std::string& dst) const override;
    Status Delete(const std::string& path, bool recursive = true) const override;
    Result<std::unique_ptr<FileStatus>> GetFileStatus(const std::string& path) const override;
    Status ListDir(const std::string& directory,
                   std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) const override;
    Status ListFileStatus(
        const std::string& path,
        std::vector<std::unique_ptr<FileStatus>>* file_status_list) const override;
    Result<bool> Exists(const std::string& path) const override;

 private:
    // the lock to ensure atomic renaming
    static const std::mutex RENAME_LOCK;

    Status Delete(std::unique_ptr<LocalFile>&& file, bool recursive = true) const;
    std::string GetParentPath(const std::string& path) const;
    Status MkdirsInternal(std::unique_ptr<LocalFile>&& file) const;
};

class LocalInputStream : public InputStream {
 public:
    static Result<std::unique_ptr<LocalInputStream>> Create(std::unique_ptr<LocalFile> file);

    Status Seek(int64_t offset, SeekOrigin origin) override;
    Result<int64_t> GetPos() const override;
    Result<int64_t> Read(char* buffer, int64_t size) override;
    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override;
    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override;

    Status Close() override;
    Result<std::string> GetUri() const override {
        return file_->GetPath();
    }
    Result<int64_t> Length() const override;

 private:
    explicit LocalInputStream(std::unique_ptr<LocalFile>&& file);

    std::unique_ptr<LocalFile> file_;
};

class LocalOutputStream : public OutputStream {
 public:
    static Result<std::unique_ptr<LocalOutputStream>> Create(std::unique_ptr<LocalFile> file);

    Result<int64_t> GetPos() const override;
    Result<int64_t> Write(const char* buffer, int64_t size) override;
    Status Flush() override;
    Status Close() override;
    Result<std::string> GetUri() const override {
        return file_->GetPath();
    }

 private:
    explicit LocalOutputStream(std::unique_ptr<LocalFile>&& file);

    std::unique_ptr<LocalFile> file_;
};

}  // namespace paimon
