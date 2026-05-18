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

#include "paimon/core/utils/consumer_manager.h"

#include <algorithm>
#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/file_system.h"
#include "rapidjson/document.h"

namespace paimon {

ConsumerManager::ConsumerManager(std::shared_ptr<FileSystem> fs, std::string table_path,
                                 std::string branch)
    : fs_(std::move(fs)),
      table_path_(std::move(table_path)),
      branch_(BranchManager::NormalizeBranch(branch)) {}

std::string ConsumerManager::ConsumerDirectory() const {
    return PathUtil::JoinPath(BranchManager::BranchPath(table_path_, branch_), "consumer");
}

std::string ConsumerManager::ConsumerPath(const std::string& consumer_id) const {
    return PathUtil::JoinPath(ConsumerDirectory(), std::string(kConsumerPrefix) + consumer_id);
}

Result<std::vector<std::string>> ConsumerManager::ListConsumers() const {
    std::vector<std::string> consumers;
    std::string consumer_dir = ConsumerDirectory();
    PAIMON_ASSIGN_OR_RAISE(bool exists, fs_->Exists(consumer_dir));
    if (!exists) {
        return consumers;
    }

    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    PAIMON_RETURN_NOT_OK(fs_->ListDir(consumer_dir, &file_status_list));
    std::string prefix = kConsumerPrefix;
    for (const auto& file_status : file_status_list) {
        if (file_status->IsDir()) {
            continue;
        }
        std::string file_name = PathUtil::GetName(file_status->GetPath());
        if (StringUtils::StartsWith(file_name, prefix, /*start_pos=*/0)) {
            consumers.push_back(file_name.substr(prefix.length()));
        }
    }
    std::sort(consumers.begin(), consumers.end());
    return consumers;
}

Result<std::optional<int64_t>> ConsumerManager::GetNextSnapshotId(
    const std::string& consumer_id) const {
    constexpr int32_t kMaxRetryCount = 10;
    constexpr int32_t kRetryIntervalMillis = 200;
    Status last_error;
    for (int32_t i = 0; i < kMaxRetryCount; ++i) {
        std::string content;
        Status read_status = fs_->ReadFile(ConsumerPath(consumer_id), &content);
        if (!read_status.ok()) {
            if (read_status.IsNotExist()) {
                return std::optional<int64_t>();
            }
            return read_status;
        }
        std::optional<int64_t> snapshot_id = StringUtils::StringToValue<int64_t>(content);
        if (snapshot_id) {
            return snapshot_id;
        }

        rapidjson::Document document;
        document.Parse(content.c_str());
        if (!document.HasParseError() && document.IsObject() &&
            document.HasMember("nextSnapshot") && document["nextSnapshot"].IsInt64()) {
            return std::optional<int64_t>(document["nextSnapshot"].GetInt64());
        }

        last_error =
            Status::Invalid("failed to parse consumer metadata: ", ConsumerPath(consumer_id));
        std::this_thread::sleep_for(std::chrono::milliseconds(kRetryIntervalMillis));
    }
    return last_error;
}

Result<std::map<std::string, int64_t>> ConsumerManager::Consumers() const {
    std::map<std::string, int64_t> consumers;
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> consumer_ids, ListConsumers());
    for (const auto& id : consumer_ids) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> next_snapshot_id, GetNextSnapshotId(id));
        if (next_snapshot_id) {
            consumers[id] = next_snapshot_id.value();
        }
    }
    return consumers;
}

}  // namespace paimon
