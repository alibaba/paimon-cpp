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

#include <memory>
#include <string>
#include <vector>

#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon {

// Base index file
class IndexFile {
 public:
    IndexFile(const std::shared_ptr<FileSystem>& fs,
              const std::shared_ptr<IndexPathFactory>& path_factory)
        : fs_(fs), path_factory_(path_factory) {}
    virtual ~IndexFile() = default;

    virtual std::string Path(const std::shared_ptr<IndexFileMeta>& file) const {
        return path_factory_->ToPath(file);
    }

    virtual Result<uint64_t> FileSize(const std::shared_ptr<IndexFileMeta>& file) const {
        return FileSize(Path(file));
    }

    virtual Result<uint64_t> FileSize(const std::string& file) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStatus> file_status, fs_->GetFileStatus(file));
        return file_status->GetLen();
    }

    virtual void Delete(const std::shared_ptr<IndexFileMeta>& file) const {
        // Deletion is best-effort
        auto status = fs_->Delete(Path(file), /*recursive=*/false);
        (void)status;
    }

    virtual Result<bool> Exists(const std::shared_ptr<IndexFileMeta>& file) const {
        return fs_->Exists(Path(file));
    }

    virtual bool IsExternalPath() const {
        return path_factory_->IsExternalPath();
    }

 protected:
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<IndexPathFactory> path_factory_;
};

}  // namespace paimon
