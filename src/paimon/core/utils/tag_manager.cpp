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

#include "paimon/core/utils/tag_manager.h"

#include <memory>
#include <stdexcept>
#include <string>

#include "fmt/format.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/tag/tag.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/file_system.h"

namespace paimon {

TagManager::TagManager(const std::shared_ptr<FileSystem>& fs, const std::string& root_path)
    : TagManager(fs, root_path, BranchManager::DEFAULT_MAIN_BRANCH) {}

TagManager::TagManager(const std::shared_ptr<FileSystem>& fs, const std::string& root_path,
                       const std::string& branch)
    : fs_(fs), root_path_(root_path), branch_(BranchManager::NormalizeBranch(branch)) {}

Result<Tag> TagManager::GetOrThrow(const std::string& tag_name) const {
    PAIMON_ASSIGN_OR_RAISE(std::optional<Tag> tag, Get(tag_name));
    if (tag == std::nullopt) {
        return Status::NotExist(fmt::format("Tag '{}' doesn't exist.", tag_name));
    }
    return tag.value();
}

Result<std::optional<Tag>> TagManager::Get(const std::string& tag_name) const {
    std::string tag_path = TagPath(tag_name);
    PAIMON_ASSIGN_OR_RAISE(bool is_exist, fs_->Exists(tag_path));
    if (!is_exist) {
        return std::optional<Tag>();
    }
    PAIMON_ASSIGN_OR_RAISE(Tag tag, Tag::FromPath(fs_, tag_path));
    return std::optional<Tag>(std::move(tag));
}

std::string TagManager::TagPath(const std::string& tag_name) const {
    return PathUtil::JoinPath(BranchManager::BranchPath(root_path_, branch_),
                              "/tag/" + std::string(TAG_PREFIX) + tag_name);
}
}  // namespace paimon
