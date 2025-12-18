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

#include "paimon/result.h"
#include "paimon/visibility.h"
namespace paimon {
class InputStream;
/// Abstract interface for reading global index files from storage.
class PAIMON_EXPORT GlobalIndexFileReader {
 public:
    virtual ~GlobalIndexFileReader() = default;

    /// Opens an input stream for reading the specified global index file.
    virtual Result<std::unique_ptr<InputStream>> GetInputStream(
        const std::string& file_name) const = 0;
};

}  // namespace paimon
