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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "JdoFileSystem.hpp"  // NOLINT(build/include_subdir)
#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon::jindo {

class JindoFileSystemImpl;

class JindoFileSystem : public FileSystem {
 public:
    explicit JindoFileSystem(std::unique_ptr<JdoFileSystem>&& fs);
    ~JindoFileSystem() override = default;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override;
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override;
    Status Mkdirs(const std::string& path) const override;
    // noted that oss file system does not support atomic rename, rename func will copy src to dst
    // and remove src
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
    std::shared_ptr<JindoFileSystemImpl> impl_;
};

class JindoInputStream : public InputStream {
 public:
    JindoInputStream(const std::shared_ptr<JindoFileSystemImpl>& fs,
                     std::unique_ptr<JdoReader>&& reader);
    Status Seek(int64_t offset, SeekOrigin origin) override;
    Result<int64_t> GetPos() const override;
    Result<int32_t> Read(char* buffer, uint32_t size) override;
    Result<int32_t> Read(char* buffer, uint32_t size, uint64_t offset) override;
    void ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                   std::function<void(Status)>&& callback) override;
    Status Close() override;
    Result<std::string> GetUri() const override;
    Result<uint64_t> Length() const override;

 private:
    // The lifecycle of the fs used to create the Jindo Reader must be longer than the lifecycle of
    // the Jindo Reader.
    std::shared_ptr<JindoFileSystemImpl> fs_;
    std::unique_ptr<JdoReader> reader_;
    std::string_view result_;
};

class JindoOutputStream : public OutputStream {
 public:
    JindoOutputStream(const std::shared_ptr<JindoFileSystemImpl>& fs,
                      std::unique_ptr<JdoWriter>&& writer);

    Result<int64_t> GetPos() const override;
    Result<int32_t> Write(const char* buffer, uint32_t size) override;
    Status Flush() override;
    Status Close() override;
    Result<std::string> GetUri() const override;

 private:
    // The lifecycle of the fs used to create the Jindo Writer must be longer than the lifecycle of
    // the Jindo Writer.
    std::shared_ptr<JindoFileSystemImpl> fs_;
    std::unique_ptr<JdoWriter> writer_;
};

}  // namespace paimon::jindo
