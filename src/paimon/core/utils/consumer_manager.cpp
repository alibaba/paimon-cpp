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
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/file_system.h"

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
    return PathUtil::JoinPath(ConsumerDirectory(), std::string(CONSUMER_PREFIX) + consumer_id);
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
    std::string prefix = CONSUMER_PREFIX;
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
    std::string content;
    Status read_status = fs_->ReadFile(ConsumerPath(consumer_id), &content);
    if (!read_status.ok()) {
        return std::optional<int64_t>();
    }
    return StringUtils::StringToValue<int64_t>(content);
}

}  // namespace paimon
