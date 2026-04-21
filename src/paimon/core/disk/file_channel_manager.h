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
#include <mutex>
#include <random>
#include <string>

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/core/disk/file_io_channel.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/result.h"

namespace paimon {
class FileChannelManager {
 public:
    static Result<std::unique_ptr<FileChannelManager>> Create(
        const std::string& tmp_dir, const std::string& prefix,
        const std::shared_ptr<FileSystem>& file_system) {
        std::string uuid;
        if (!UUID::Generate(&uuid)) {
            return Status::Invalid("Failed to generate UUID for FileChannelManager.");
        }
        std::string spill_dir = PathUtil::JoinPath(tmp_dir, "paimon-" + prefix + "-" + uuid);

        PAIMON_RETURN_NOT_OK(file_system->Mkdirs(spill_dir));

        std::random_device rd;
        std::mt19937 random(rd());

        return std::unique_ptr<FileChannelManager>(
            new FileChannelManager(spill_dir, std::move(random), file_system));
    }

    ~FileChannelManager() {
        if (!spill_dir_.empty() && fs_ != nullptr) {
            [[maybe_unused]] auto status = fs_->Delete(spill_dir_, /*recursive=*/true);
        }
    }

    FileChannelManager(const FileChannelManager&) = delete;
    FileChannelManager& operator=(const FileChannelManager&) = delete;

    FileIOChannel::ID CreateChannel() {
        std::lock_guard<std::mutex> lock(mutex_);
        return FileIOChannel::ID(spill_dir_, &random_);
    }

    FileIOChannel::ID CreateChannel(const std::string& prefix) {
        std::lock_guard<std::mutex> lock(mutex_);
        return FileIOChannel::ID(spill_dir_, prefix, &random_);
    }

    std::shared_ptr<FileIOChannel::Enumerator> CreateChannelEnumerator() {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::make_shared<FileIOChannel::Enumerator>(spill_dir_, &random_);
    }

    const std::string& GetSpillDir() const {
        return spill_dir_;
    }

 private:
    FileChannelManager(const std::string& spill_dir, std::mt19937&& random,
                       const std::shared_ptr<FileSystem>& fs)
        : spill_dir_(spill_dir), random_(std::move(random)), fs_(fs) {}
    std::string spill_dir_;
    std::mutex mutex_;
    std::mt19937 random_;
    std::shared_ptr<FileSystem> fs_;
};

}  // namespace paimon
