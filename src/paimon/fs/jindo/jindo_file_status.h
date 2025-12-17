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

#include <string>
#include <utility>

#include "JdoFileInfo.hpp"  // NOLINT(build/include_subdir)
#include "paimon/fs/file_system.h"

namespace paimon::jindo {
class JindoBasicFileStatus : public BasicFileStatus {
 public:
    explicit JindoBasicFileStatus(JdoFileInfo&& file_info) : file_info_(std::move(file_info)) {}

    std::string GetPath() const override {
        return file_info_.getPath();
    }

    bool IsDir() const override {
        return file_info_.isDir();
    }

 private:
    JdoFileInfo file_info_;
};

class JindoFileStatus : public FileStatus {
 public:
    explicit JindoFileStatus(JdoFileInfo&& file_info) : file_info_(std::move(file_info)) {}

    std::string GetPath() const override {
        return file_info_.getPath();
    }

    uint64_t GetLen() const override {
        return file_info_.getLength();
    }

    int64_t GetModificationTime() const override {
        return file_info_.getMtime();
    }

    bool IsDir() const override {
        return file_info_.isDir();
    }

 private:
    JdoFileInfo file_info_;
};
}  // namespace paimon::jindo
