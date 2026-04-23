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
#include <string>

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/core/disk/file_channel_manager.h"
#include "paimon/result.h"

namespace paimon {
class IOManager {
 public:
    IOManager(const std::string& tmp_dir, const std::shared_ptr<FileSystem>& file_system)
        : tmp_dir_(tmp_dir), file_system_(file_system) {}

    const std::string& GetTempDir() const {
        return tmp_dir_;
    }

    Result<std::string> GenerateTempFilePath(const std::string& prefix) const {
        std::string uuid;
        if (!UUID::Generate(&uuid)) {
            return Status::Invalid("generate uuid for io manager tmp path failed.");
        }
        return PathUtil::JoinPath(tmp_dir_, prefix + "-" + uuid + std::string(kSuffix));
    }

    Result<FileIOChannel::ID> CreateChannel() {
        PAIMON_ASSIGN_OR_RAISE(auto* manager, GetFileChannelManager());
        return manager->CreateChannel();
    }

    Result<FileIOChannel::ID> CreateChannel(const std::string& prefix) {
        PAIMON_ASSIGN_OR_RAISE(auto* manager, GetFileChannelManager());
        return manager->CreateChannel(prefix);
    }

    Result<std::shared_ptr<FileIOChannel::Enumerator>> CreateChannelEnumerator() {
        PAIMON_ASSIGN_OR_RAISE(auto* manager, GetFileChannelManager());
        return manager->CreateChannelEnumerator();
    }

    Result<std::string> GetSpillDir() {
        PAIMON_ASSIGN_OR_RAISE(auto* manager, GetFileChannelManager());
        return manager->GetSpillDir();
    }

 private:
    Result<FileChannelManager*> GetFileChannelManager() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (file_channel_manager_ == nullptr) {
            PAIMON_ASSIGN_OR_RAISE(
                file_channel_manager_,
                FileChannelManager::Create(tmp_dir_, kDirNamePrefix, file_system_));
        }
        return file_channel_manager_.get();
    }

    static constexpr char kSuffix[] = ".channel";
    static constexpr char kDirNamePrefix[] = "io";
    std::string tmp_dir_;
    std::shared_ptr<FileSystem> file_system_;
    std::mutex mutex_;
    std::unique_ptr<FileChannelManager> file_channel_manager_;
};

}  // namespace paimon
