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

#include "paimon/fs/file_system.h"

namespace paimon {

class LocalBasicFileStatus : public BasicFileStatus {
 public:
    LocalBasicFileStatus(const std::string& path, bool is_dir) : path_(path), is_dir_(is_dir) {}

    std::string GetPath() const override {
        return path_;
    }

    bool IsDir() const override {
        return is_dir_;
    }

 private:
    std::string path_;
    bool is_dir_;
};

class LocalFileStatus : public FileStatus {
 public:
    LocalFileStatus(const std::string& path, int64_t length, int64_t last_modification_time,
                    bool is_dir)
        : path_(path),
          length_(length),
          last_modification_time_(last_modification_time),
          is_dir_(is_dir) {}

    std::string GetPath() const override {
        return path_;
    }

    int64_t GetLen() const override {
        return length_;
    }

    int64_t GetModificationTime() const override {
        return last_modification_time_;
    }

    bool IsDir() const override {
        return is_dir_;
    }

 private:
    std::string path_;
    int64_t length_;
    int64_t last_modification_time_;
    bool is_dir_;
};

}  // namespace paimon
