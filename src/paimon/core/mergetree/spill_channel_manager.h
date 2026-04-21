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

#include <unordered_set>

#include "paimon/core/disk/file_io_channel.h"
#include "paimon/fs/file_system.h"
#include "paimon/status.h"

namespace paimon {

class SpillChannelManager {
 public:
    SpillChannelManager(const std::shared_ptr<FileSystem>& fs, size_t initial_capacity) : fs_(fs) {
        channels_.reserve(initial_capacity);
    }

    void AddChannel(const FileIOChannel::ID& channel_id) {
        channels_.emplace(channel_id);
    }

    Status DeleteChannel(const FileIOChannel::ID& channel_id) {
        PAIMON_RETURN_NOT_OK(fs_->Delete(channel_id.GetPath()));
        channels_.erase(channel_id);
        return Status::OK();
    }

    void Reset() {
        for (const auto& channel : channels_) {
            [[maybe_unused]] auto status = fs_->Delete(channel.GetPath());
        }
        channels_.clear();
    }

    const std::unordered_set<FileIOChannel::ID, FileIOChannel::ID::Hash>& GetChannels() const {
        return channels_;
    }

 private:
    std::unordered_set<FileIOChannel::ID, FileIOChannel::ID::Hash> channels_;
    std::shared_ptr<FileSystem> fs_;
};

}  // namespace paimon
