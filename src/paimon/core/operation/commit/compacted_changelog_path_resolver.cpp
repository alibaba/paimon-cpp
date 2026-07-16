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

#include "paimon/core/operation/commit/compacted_changelog_path_resolver.h"

#include <string>
#include <vector>

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"

namespace paimon {

bool CompactedChangelogPathResolver::IsCompactedChangelogPath(const std::string& path) {
    const std::string file_name = PathUtil::GetName(path);
    return StringUtils::StartsWith(file_name, "compacted-changelog-");
}

std::string CompactedChangelogPathResolver::Resolve(const std::string& path) {
    if (!IsCompactedChangelogPath(path)) {
        return path;
    }
    const std::string file_name = PathUtil::GetName(path);

    const size_t dot_pos = file_name.find_last_of('.');
    if (dot_pos == std::string::npos || dot_pos + 1 >= file_name.size()) {
        return path;
    }

    const std::string name_without_ext = file_name.substr(0, dot_pos);
    const std::string format = file_name.substr(dot_pos + 1);
    const size_t dollar_pos = name_without_ext.find('$');
    if (dollar_pos == std::string::npos || dollar_pos + 1 >= name_without_ext.size()) {
        return path;
    }

    const std::string base_name = name_without_ext.substr(0, dollar_pos);
    const std::string suffix = name_without_ext.substr(dollar_pos + 1);
    const std::vector<std::string> split_tokens = StringUtils::Split(suffix, "-", false);

    // Real compacted changelog path pattern: ...$bucket-len.ext
    if (split_tokens.size() == 2) {
        return path;
    }

    // Fake compacted changelog path pattern: ...$bucket-len-offset-sliceLen.ext
    if (split_tokens.size() < 4) {
        return path;
    }

    const std::string& bucket = split_tokens[0];
    const std::string& total_len = split_tokens[1];
    const std::string real_file_name = base_name + "$" + bucket + "-" + total_len + "." + format;

    const std::string parent = PathUtil::GetParentDirPath(path);
    const std::string grand_parent = PathUtil::GetParentDirPath(parent);
    if (parent.empty() || grand_parent.empty()) {
        return path;
    }

    return PathUtil::JoinPath(PathUtil::JoinPath(grand_parent, "bucket-" + bucket), real_file_name);
}

}  // namespace paimon
