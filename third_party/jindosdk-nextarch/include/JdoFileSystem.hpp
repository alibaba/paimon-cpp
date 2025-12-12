/*
 * Copyright 2024-present Alibaba Cloud.
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

#include <atomic>
#include <functional>
#include <jdo_data_types.h>
#include <mutex>
#include <string>
#include <vector>
#include <tuple>
#include <memory>
#include <unordered_map>

#include "jdo_common.h"
#include "JdoConfig.hpp"
#include "JdoStatus.hpp"

class JdoFileInfo;
class JdoListResult;
class JdoContentSummary;
using JdoFileInfoPtr = std::shared_ptr<JdoFileInfo>;
using JdoListResultPtr = std::shared_ptr<JdoListResult>;
using JdoContentSummaryPtr = std::shared_ptr<JdoContentSummary>;

class JDO_EXPORT JdoTask {
public:
    JdoTask() = default;

    virtual ~JdoTask() = default;

    virtual JdoStatus perform() = 0;

    virtual JdoStatus wait() = 0;

    virtual JdoStatus cancel() = 0;
};

typedef std::shared_ptr<JdoTask> JdoTaskPtr;

class JDO_EXPORT JdoReader {
public:
    JdoReader() = default;

    virtual ~JdoReader() = default;

    virtual bool closed() const = 0;

    virtual JdoStatus name(std::string_view* result) const = 0;

    virtual JdoStatus read(size_t n, std::string_view* result, char* scratch) = 0;

    virtual JdoTaskPtr readAsync(size_t n, std::string_view* result, char* scratch,
                                 std::function<void(JdoStatus)> callback) = 0;

    virtual JdoStatus pread(int64_t offset, size_t n, std::string_view* result, char* scratch) = 0;

    virtual JdoTaskPtr preadAsync(int64_t offset, size_t n, std::string_view* result, char* scratch,
                                  std::function<void(JdoStatus)> callback) = 0;

    virtual JdoStatus preadv(std::vector<int64_t>& offsets, std::vector<size_t>& lengths, std::vector<char*>& scratches) = 0;

    virtual JdoTaskPtr preadvAsync(std::vector<int64_t>& offsets, std::vector<size_t>& lengths, std::vector<char*>& scratches,
                                  std::function<void(JdoStatus)> callback) = 0;

    virtual JdoStatus tell(int64_t &offset) = 0;

    virtual JdoTaskPtr tellAsync(std::function<void(JdoStatus, int64_t)> callback) = 0;

    virtual JdoStatus seek(int64_t offset) = 0;

    virtual JdoTaskPtr seekAsync(int64_t offset, std::function<void(JdoStatus, int64_t)> callback) = 0;

    virtual JdoStatus getFileLength(int64_t &length) = 0;

    virtual JdoTaskPtr getFileLengthAsync(std::function<void(JdoStatus, int64_t)> callback) = 0;

    virtual JdoStatus close() = 0;

    virtual JdoTaskPtr closeAsync(std::function<void(JdoStatus)> callback) = 0;
};

class JDO_EXPORT JdoWriter {
public:
    JdoWriter() = default;

    virtual ~JdoWriter() = default;

    virtual bool closed() const = 0;

    virtual JdoStatus name(std::string_view* result) const = 0;

    virtual JdoStatus write(std::string_view data) = 0;

    virtual JdoTaskPtr writeAsync(std::string_view data, std::function<void(JdoStatus)> callback) = 0;

    virtual JdoStatus flush() = 0;

    virtual JdoTaskPtr flushAsync(std::function<void(JdoStatus)> callback) = 0;

    virtual JdoStatus tell(int64_t &offset) = 0;

    virtual JdoTaskPtr tellAsync(std::function<void(JdoStatus, int64_t)> callback) = 0;

    virtual JdoStatus close() = 0;

    virtual JdoTaskPtr closeAsync(std::function<void(JdoStatus)> callback) = 0;
};


class JDO_EXPORT JdoFileSystem {
public:
    JdoFileSystem();

    virtual ~JdoFileSystem();

    JdoStatus init(const std::string &uri, const std::string &user, const std::shared_ptr<JdoConfig> &config);

    JdoStatus destroy();

    JdoStatus openReader(const std::string &path, std::unique_ptr<JdoReader>* result);

    JdoStatus openWriter(const std::string &path, std::unique_ptr<JdoWriter>* result);

    JdoStatus mkdir(const std::string &path, bool recursive);

    JdoTaskPtr mkdirAsync(const std::string &path, bool recursive, std::function<void(JdoStatus)> callback);

    JdoStatus rename(const std::string &oldpath, const std::string &newpath);

    JdoTaskPtr renameAsync(const std::string &oldpath, const std::string &newpath,
                           std::function<void(JdoStatus)> callback);

    JdoStatus remove(const std::string &path, bool recursive);

    JdoTaskPtr removeAsync(const std::string &path, bool recursive, std::function<void(JdoStatus)> callback);

    JdoStatus exists(const std::string &path);

    JdoTaskPtr existsAsync(const std::string &path, std::function<void(JdoStatus)> callback);

    JdoStatus getFileInfo(const std::string &path, JdoFileInfo* info);

    JdoTaskPtr getFileInfoAsync(const std::string &path, std::function<void(JdoStatus, JdoFileInfoPtr)> callback);

    JdoStatus listDir(const std::string &path, bool recursive, JdoListResult* result);

    JdoTaskPtr listDirAsync(const std::string &path, bool recursive,
                            std::function<void(JdoStatus, JdoListResultPtr)> callback);

    JdoStatus getContentSummary(const std::string &path, bool recursive, JdoContentSummary* result);

    JdoTaskPtr getContentSummaryAsync(const std::string &path, bool recursive,
                                      std::function<void(JdoStatus, JdoContentSummaryPtr)> callback);

    JdoStatus setPermission(const std::string &path, int16_t perm);

    JdoTaskPtr setPermissionAsync(const std::string &path, int16_t perm, std::function<void(JdoStatus)> callback);

    JdoStatus setOwner(const std::string &path, const std::string& user, const std::string& group);

    JdoTaskPtr setOwnerAsync(const std::string &path, const std::string& user, const std::string& group, std::function<void(JdoStatus)> callback);

private:
    JdoStatus getFileInfoInternal(JdoStore_t client,
                                  const std::string &path,
                                  JdoFileInfo* info);

    // return JdoStore_t, status
    std::tuple<JdoStore_t, JdoStatus> GetJdoStore(const std::string &uri);

private:
    JdoStore_t _store     = nullptr;
    JdoOptions_t _options = nullptr;
    bool _inited          = false;
};
