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
#include <optional>
#include <string>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/operation/restore_files.h"
#include "paimon/result.h"

namespace paimon {

/// Restore for write to restore data files by partition and bucket from file system.
class WriteRestore {
 public:
    static Result<std::optional<int32_t>> ExtractDataFiles(
        const std::vector<ManifestEntry>& entries,
        std::vector<std::shared_ptr<DataFileMeta>>* data_files);

    virtual ~WriteRestore() = default;

    virtual Result<int64_t> LatestCommittedIdentifier(const std::string& user) const = 0;

    virtual Result<std::shared_ptr<RestoreFiles>> GetRestoreFiles() const = 0;
};

}  // namespace paimon
