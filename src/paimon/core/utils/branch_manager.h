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
#include <memory>
#include <string>
#include <vector>

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/result.h"

namespace paimon {
class FileSystem;
}  // namespace paimon

namespace paimon {
/// Utility methods for table branch paths and branch discovery.
class BranchManager {
 public:
    BranchManager() = delete;
    ~BranchManager() = delete;

    static constexpr char DEFAULT_MAIN_BRANCH[] = "main";
    static constexpr char BRANCH_PREFIX[] = "branch-";

    /// Normalizes an empty branch name to `main`.
    static std::string NormalizeBranch(const std::string& branch) {
        return StringUtils::IsNullOrWhitespaceOnly(branch) ? DEFAULT_MAIN_BRANCH : branch;
    }

    /// Returns the table root path for the selected branch.
    static std::string BranchPath(const std::string& table_root, const std::string& branch) {
        return IsMainBranch(branch)
                   ? table_root
                   : PathUtil::JoinPath(table_root,
                                        "/branch/" + std::string(BRANCH_PREFIX) + branch);
    }

    /// Returns whether the branch is the default main branch.
    static bool IsMainBranch(const std::string& branch) {
        return branch == DEFAULT_MAIN_BRANCH;
    }

    /// Lists all branches for a table, including `main`.
    static Result<std::vector<std::string>> ListBranches(const std::shared_ptr<FileSystem>& fs,
                                                         const std::string& table_root);
};
}  // namespace paimon
