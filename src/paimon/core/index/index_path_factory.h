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

#include "paimon/core/index/index_file_meta.h"

namespace paimon {
/// Path factory to create an index path.
class IndexPathFactory {
 public:
    virtual ~IndexPathFactory() = default;

    virtual std::string NewPath() const = 0;
    virtual std::string ToPath(const std::string& file_name) const = 0;
    virtual std::string ToPath(const std::shared_ptr<IndexFileMeta>& file) const = 0;
    virtual bool IsExternalPath() const = 0;

    static inline const char INDEX_PREFIX[] = "index-";
};
}  // namespace paimon
