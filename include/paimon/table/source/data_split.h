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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/table/source/split.h"
#include "paimon/visibility.h"

namespace paimon {
class MemoryPool;

/// Input data split for reading operation. Needed by most batch computation engines.
class PAIMON_EXPORT DataSplit : public Split {
 public:
    /// Metadata structure for simple data files.
    ///
    /// Contains essential information about a data file including its location,
    /// size, row count, sequence numbers, schema information, and timestamps.
    /// This structure is used to track file metadata without loading the actual file content.
    struct SimpleDataFileMeta {
        SimpleDataFileMeta(const std::string& _file_path, int64_t _file_size, int64_t _row_count,
                           int64_t _min_sequence_number, int64_t _max_sequence_number,
                           int64_t _schema_id, int32_t _level, const Timestamp& _creation_time,
                           const std::optional<int64_t>& _delete_row_count)
            : file_path(_file_path),
              file_size(_file_size),
              row_count(_row_count),
              min_sequence_number(_min_sequence_number),
              max_sequence_number(_max_sequence_number),
              schema_id(_schema_id),
              level(_level),
              creation_time(_creation_time),
              delete_row_count(_delete_row_count) {}

        /// Absolute path of the data file.
        ///
        /// If external path is enabled, `file_path` indicates the actual location in the external
        /// storage system.
        std::string file_path;
        int64_t file_size;
        int64_t row_count;
        int64_t min_sequence_number;
        int64_t max_sequence_number;
        int64_t schema_id;
        int32_t level;
        Timestamp creation_time;
        std::optional<int64_t> delete_row_count;

        bool operator==(const SimpleDataFileMeta& other) const;

        std::string ToString() const;
    };

    /// Get the list of metadata for all data files in this split.
    /// @note This method will be removed in future versions and is only used for append tables.
    virtual std::vector<SimpleDataFileMeta> GetFileList() const = 0;
};
}  // namespace paimon
