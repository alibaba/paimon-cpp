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

#include "paimon/common/utils/path_util.h"

#include <cstddef>
#include <cstdint>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/status.h"

namespace paimon {
std::string Path::ToString() const {
    std::string ret;
    if (!scheme.empty()) {
        ret = scheme + ":";
    }
    if (!authority.empty()) {
        ret += "//";
        ret += authority;
    }
    if (!path.empty()) {
        ret += path;
    }
    return ret;
}

std::string PathUtil::JoinPath(const std::string& path, const std::string& name) noexcept {
    if (path.empty()) {
        return name;
    }
    if (name.empty()) {
        return path;
    }
    int slash_cnt = (*(path.rbegin()) == '/') + (*(name.begin()) == '/');
    if (!slash_cnt) {
        return path + "/" + name;
    } else if (slash_cnt == 2) {
        return path + name.substr(1);
    }
    return path + name;
}

std::string PathUtil::NormalizeInnerPath(const std::string& path) noexcept {
    if (path.empty()) {
        return path;
    }
    std::string ret;
    ret.reserve(path.size());
    char last_char = path[0];
    ret.append(1, last_char);
    for (size_t i = 1; i < path.size(); ++i) {
        if (last_char == '/' && path[i] == '/') {
            continue;
        }
        last_char = path[i];
        ret.append(1, last_char);
    }
    TrimLastDelim(&ret);
    return ret;
}

Result<std::string> PathUtil::NormalizePath(const std::string& path_str) noexcept {
    PAIMON_ASSIGN_OR_RAISE(Path path, ToPath(path_str));
    return path.ToString();
}

Result<Path> PathUtil::ToPath(const std::string& path) noexcept {
    // TODO(yonghao.fyh): support Windows Driver
    if (path.empty()) {
        return Status::Invalid("path is an empty string.");
    }
    std::string scheme;
    std::string authority;
    int32_t start = 0;

    // parse scheme
    auto colon = path.find(':');
    auto slash = path.find('/');
    if ((colon != std::string::npos) && (slash == std::string::npos || colon < slash)) {
        // has a scheme
        scheme.append(path, 0, colon);
        start = colon + 1;
    }

    // parse authority
    if (StringUtils::StartsWith(path, "//", start) && (path.length() - start > 2)) {
        // has authority
        int32_t next_slash = path.find('/', start + 2);
        int32_t auth_end = next_slash > 0 ? next_slash : path.length();
        authority = path.substr(start + 2, auth_end - start - 2);
        start = auth_end;
    }

    // parse path in uri
    std::string inner_path = NormalizeInnerPath(path.substr(start));
    return Path(scheme, authority, inner_path);
}

std::string PathUtil::GetParentDirPath(const std::string& path) noexcept {
    std::string::const_reverse_iterator it;
    for (it = path.rbegin(); it != path.rend() && *it == '/'; it++) {
    }
    for (; it != path.rend() && *it != '/'; it++) {
    }
    for (; it != path.rend() && *it == '/'; it++) {
    }
    return path.substr(0, path.rend() - it);
}

std::string PathUtil::GetName(const std::string& path) noexcept {
    std::string dir_path = path;
    TrimLastDelim(&dir_path);
    std::string::const_reverse_iterator it;
    for (it = dir_path.rbegin(); it != dir_path.rend() && *it != '/'; it++) {
    }
    return dir_path.substr(dir_path.rend() - it);
}

void PathUtil::TrimLastDelim(std::string* dir_path) noexcept {
    if (dir_path == nullptr || dir_path->empty()) {
        return;
    }
    if (dir_path->length() > 1 && *(dir_path->rbegin()) == '/') {
        dir_path->erase(dir_path->size() - 1, 1);
    }
}

Result<std::string> PathUtil::CreateTempPath(const std::string& path) noexcept {
    std::string uuid;
    if (!UUID::Generate(&uuid)) {
        return Status::Invalid("generate uuid failed");
    }
    return JoinPath(GetParentDirPath(path), fmt::format(".{}.{}.tmp", GetName(path), uuid));
}

}  // namespace paimon
