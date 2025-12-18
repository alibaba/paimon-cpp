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
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class IOHook;
class LocalFileStatus;

class LocalFile {
 public:
    explicit LocalFile(const std::string& path);
    ~LocalFile() = default;

    Result<bool> Exists() const;
    Result<bool> IsFile() const;
    Result<bool> IsDir() const;
    Status List(std::vector<std::string>* file_list) const;
    Status ListFiles(std::vector<LocalFile>* file_list) const;
    Status Delete() const;
    const std::string& GetAbsolutePath() const;
    LocalFile GetParentFile() const;
    Result<bool> Mkdir() const;
    Result<std::unique_ptr<LocalFileStatus>> GetFileStatus() const;
    Result<uint64_t> Length() const;
    Result<int64_t> LastModifiedTimeMs() const;
    Status OpenFile(bool is_read_file);
    Result<int32_t> Read() {
        return Status::NotImplemented("");
    }
    Result<int32_t> Read(char* buffer, uint32_t length);
    Result<int32_t> Read(char* buffer, uint32_t length, uint64_t offset);
    Result<int32_t> Write(const char* buffer, uint32_t length);
    Status Flush();
    Status Close();
    Status Seek(int64_t offset, int32_t seek_origin);
    Result<int64_t> Tell() const;

    bool IsEmpty() const {
        return path_.empty();
    }

 private:
    const std::string path_;
    FILE* file_ = nullptr;
    IOHook* hook_;
};

}  // namespace paimon
