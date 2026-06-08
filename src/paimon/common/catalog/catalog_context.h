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
#include <map>
#include <string>

#include "paimon/fs/file_system.h"

namespace paimon {
struct CatalogContext {
    CatalogContext(const std::string& _root_path,
                   const std::map<std::string, std::string>& _options,
                   const std::shared_ptr<FileSystem>& _file_system)
        : root_path(_root_path), options(_options), file_system(_file_system) {}

    std::string root_path;
    std::map<std::string, std::string> options;
    std::shared_ptr<FileSystem> file_system;
};

}  // namespace paimon
