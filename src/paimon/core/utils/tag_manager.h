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
#include <optional>
#include <string>

#include "paimon/core/tag/tag.h"

namespace paimon {

class FileSystem;

/// Manager for `Tag`.
class TagManager {
 public:
    static constexpr char TAG_PREFIX[] = "tag-";

    TagManager(const std::shared_ptr<FileSystem>& fs, const std::string& root_path);
    TagManager(const std::shared_ptr<FileSystem>& fs, const std::string& root_path,
               const std::string& branch);

    Result<Tag> GetOrThrow(const std::string& tag_name) const;

    Result<std::optional<Tag>> Get(const std::string& tag_name) const;

    std::string TagPath(const std::string& tag_name) const;

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string root_path_;
    std::string branch_;
};
}  // namespace paimon
