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

#include "paimon/core/utils/branch_manager.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "paimon/fs/file_system.h"

namespace paimon {

Result<std::vector<std::string>> BranchManager::ListBranches(const std::shared_ptr<FileSystem>& fs,
                                                             const std::string& table_root) {
    std::vector<std::string> branches = {DEFAULT_MAIN_BRANCH};
    std::string branch_dir = PathUtil::JoinPath(table_root, "branch");
    PAIMON_ASSIGN_OR_RAISE(bool is_exist, fs->Exists(branch_dir));
    if (!is_exist) {
        return branches;
    }

    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    PAIMON_RETURN_NOT_OK(fs->ListDir(branch_dir, &file_status_list));
    std::string branch_prefix = BRANCH_PREFIX;
    for (const auto& file_status : file_status_list) {
        if (!file_status->IsDir()) {
            continue;
        }
        std::string dir_name = PathUtil::GetName(file_status->GetPath());
        if (StringUtils::StartsWith(dir_name, branch_prefix, /*start_pos=*/0)) {
            branches.push_back(dir_name.substr(branch_prefix.length()));
        }
    }
    std::sort(branches.begin(), branches.end());
    return branches;
}

}  // namespace paimon
