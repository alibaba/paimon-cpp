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

#include "fmt/format.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/status.h"

namespace paimon::lance {
inline Status LanceToPaimonStatus(int32_t error_code, const std::string& error_message) {
    if (error_code != 0) {
        return Status::Invalid(error_message);
    }
    return Status::OK();
}

class LanceUtils {
 public:
    LanceUtils() = delete;
    ~LanceUtils() = delete;
    static Result<std::string> NormalizeLanceFilePath(const std::string& path_string) {
        // local file system does not support path_string with scheme, e.g., "file:/tmp" will be
        // rewritten to "/tmp"
        PAIMON_ASSIGN_OR_RAISE(Path path, PathUtil::ToPath(path_string));
        if (!path.scheme.empty() && StringUtils::ToLowerCase(path.scheme) == "file") {
            return path.path;
        }
        return path.ToString();
    }
};
}  // namespace paimon::lance
