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

#include <memory>
#include <string>
#include <vector>

#include "paimon/fs/file_system.h"

namespace paimon::test {

class MockInputStream : public InputStream {
 public:
    MockInputStream() = default;
    ~MockInputStream() override = default;

    Status Seek(int64_t offset, SeekOrigin origin) override {
        return Status::OK();
    }
    Result<int64_t> GetPos() const override {
        return 0;
    }
    Result<int32_t> Read(char* buffer, uint32_t size) override {
        return 0;
    }
    Result<int32_t> Read(char* buffer, uint32_t size, uint64_t offset) override {
        return 0;
    }
    void ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                   std::function<void(Status)>&& callback) override {}

    Status Close() override {
        return Status::OK();
    }
    Result<std::string> GetUri() const override {
        return std::string();
    }
    Result<uint64_t> Length() const override {
        return 0;
    }
};

class MockOutputStream : public OutputStream {
 public:
    MockOutputStream() = default;
    ~MockOutputStream() override = default;

    Result<int64_t> GetPos() const override {
        return 0;
    }
    Result<int32_t> Write(const char* buffer, uint32_t size) override {
        return 0;
    }
    Status Flush() override {
        return Status::OK();
    }
    Status Close() override {
        return Status::OK();
    }
    Result<std::string> GetUri() const override {
        return std::string();
    }
};

class MockFileStatus : public FileStatus {
 public:
    MockFileStatus() = default;
    ~MockFileStatus() override = default;

    std::string GetPath() const override {
        return "";
    }
    uint64_t GetLen() const override {
        return 0;
    }
    int64_t GetModificationTime() const override {
        return 0;
    }
    bool IsDir() const override {
        return false;
    }
};

class MockFileSystem : public FileSystem {
 public:
    MockFileSystem() = default;
    ~MockFileSystem() override = default;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        return std::make_unique<MockInputStream>();
    }
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        return std::make_unique<MockOutputStream>();
    }
    Status Mkdirs(const std::string& path) const override {
        return Status::OK();
    }
    Status Rename(const std::string& src, const std::string& dst) const override {
        return Status::OK();
    }
    Status Delete(const std::string& path, bool recursive = true) const override {
        return Status::OK();
    }
    Result<std::unique_ptr<FileStatus>> GetFileStatus(const std::string& path) const override {
        return std::make_unique<MockFileStatus>();
    }
    Status ListDir(const std::string& directory,
                   std::vector<std::unique_ptr<BasicFileStatus>>* status_list) const override {
        return Status::OK();
    }
    Status ListFileStatus(const std::string& path,
                          std::vector<std::unique_ptr<FileStatus>>* status_list) const override {
        return Status::OK();
    }
    Result<bool> Exists(const std::string& path) const override {
        return true;
    }
};

}  // namespace paimon::test
