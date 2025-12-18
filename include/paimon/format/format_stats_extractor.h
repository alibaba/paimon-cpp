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
#include <utility>
#include <vector>

#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace paimon {

/// Extracts statistics directly from file.
class PAIMON_EXPORT FormatStatsExtractor {
 public:
    virtual ~FormatStatsExtractor() = default;
    /// File info fetched from physical file, currently only include row count.
    class PAIMON_EXPORT FileInfo {
     public:
        explicit FileInfo(int64_t row_count) : row_count_(row_count) {}

        int64_t GetRowCount() const {
            return row_count_;
        }

     private:
        int64_t row_count_;
    };

    /// Extracts statistics for each column of a data file based on the file path and file system.
    virtual Result<ColumnStatsVector> Extract(const std::shared_ptr<FileSystem>& file_system,
                                              const std::string& path,
                                              const std::shared_ptr<MemoryPool>& pool) = 0;

    /// Extracts statistics for each column and `FileInfo` of a data file based on the file path and
    /// file system.
    virtual Result<std::pair<ColumnStatsVector, FileInfo>> ExtractWithFileInfo(
        const std::shared_ptr<FileSystem>& file_system, const std::string& path,
        const std::shared_ptr<MemoryPool>& pool) = 0;
};

}  // namespace paimon
