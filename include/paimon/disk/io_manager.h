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

#include <cstdint>
#include <memory>
#include <string>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
/// The facade for the provided disk I/O services.
class PAIMON_EXPORT IOManager {
 public:
    virtual ~IOManager() = default;
    static std::unique_ptr<IOManager> Create(const std::string& tmp_dir);

    /// @return Temp directory path.
    virtual const std::string& GetTempDir() const = 0;

    virtual Result<std::string> GenerateTempFilePath(const std::string& prefix) const = 0;
};
}  // namespace paimon
