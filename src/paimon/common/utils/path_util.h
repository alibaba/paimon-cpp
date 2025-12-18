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

#include <string>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
struct PAIMON_EXPORT Path {
    Path(const std::string& _scheme, const std::string& _authority, const std::string& _path)
        : scheme(_scheme), authority(_authority), path(_path) {}

    std::string ToString() const;

    std::string scheme;
    std::string authority;
    std::string path;
};

class PAIMON_EXPORT PathUtil {
 public:
    PathUtil() = delete;
    ~PathUtil() = delete;

    static std::string JoinPath(const std::string& path, const std::string& name) noexcept;
    // TODO(jinli.zjw): should pass `Path.path` and normalize; otherwise if path is
    // "oss://bucket1/", GetParentDirPath will return "oss:"
    static std::string GetParentDirPath(const std::string& path) noexcept;
    static std::string GetName(const std::string& path) noexcept;
    static void TrimLastDelim(std::string* dir_path) noexcept;
    static Result<std::string> CreateTempPath(const std::string& path) noexcept;
    static Result<Path> ToPath(const std::string& path) noexcept;
    static Result<std::string> NormalizePath(const std::string& path) noexcept;

 private:
    static std::string NormalizeInnerPath(const std::string& path) noexcept;
};

}  // namespace paimon
