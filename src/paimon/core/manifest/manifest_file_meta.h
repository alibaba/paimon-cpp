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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "paimon/core/stats/simple_stats.h"
#include "paimon/visibility.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {

/// Metadata of a manifest file.
class PAIMON_EXPORT ManifestFileMeta {
 public:
    static const std::shared_ptr<arrow::DataType>& DataType();

    ManifestFileMeta(const std::string& file_name, int64_t file_size, int64_t num_added_files,
                     int64_t num_deleted_files, const SimpleStats& partition_stats,
                     int64_t schema_id, const std::optional<int32_t>& min_bucket,
                     const std::optional<int32_t>& max_bucket,
                     const std::optional<int32_t>& min_level,
                     const std::optional<int32_t>& max_level,
                     const std::optional<int64_t>& min_row_id,
                     const std::optional<int64_t>& max_row_id);

    const std::string& FileName() const {
        return file_name_;
    }

    int64_t FileSize() const {
        return file_size_;
    }

    int64_t NumAddedFiles() const {
        return num_added_files_;
    }

    int64_t NumDeletedFiles() const {
        return num_deleted_files_;
    }

    SimpleStats PartitionStats() const {
        return partition_stats_;
    }

    int64_t SchemaId() const {
        return schema_id_;
    }

    const std::optional<int32_t>& MinBucket() const {
        return min_bucket_;
    }
    const std::optional<int32_t>& MaxBucket() const {
        return max_bucket_;
    }
    const std::optional<int32_t>& MinLevel() const {
        return min_level_;
    }
    const std::optional<int32_t>& MaxLevel() const {
        return max_level_;
    }

    const std::optional<int64_t>& MinRowId() const {
        return min_row_id_;
    }
    const std::optional<int64_t>& MaxRowId() const {
        return max_row_id_;
    }

    std::string ToString() const;

    bool operator==(const ManifestFileMeta& other) const;

 private:
    std::string file_name_;
    int64_t file_size_;
    int64_t num_added_files_;
    int64_t num_deleted_files_;
    SimpleStats partition_stats_;
    int64_t schema_id_;
    std::optional<int32_t> min_bucket_;
    std::optional<int32_t> max_bucket_;
    std::optional<int32_t> min_level_;
    std::optional<int32_t> max_level_;
    std::optional<int64_t> min_row_id_;
    std::optional<int64_t> max_row_id_;
};

}  // namespace paimon
