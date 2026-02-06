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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class FileKind;
struct DataFileMeta;
class BinaryRowPartitionComputer;

class PartitionEntry {
 public:
    PartitionEntry(const BinaryRow& partition, int64_t record_count, int64_t file_size_in_bytes,
                   int64_t file_count, int64_t last_file_creation_time, int32_t total_buckets);

    const BinaryRow& Partition() const {
        return partition_;
    }
    int64_t RecordCount() const {
        return record_count_;
    }
    int64_t FileSizeInBytes() const {
        return file_size_in_bytes_;
    }
    int64_t FileCount() const {
        return file_count_;
    }

    // supposed to return UTC time
    int64_t LastFileCreationTime() const {
        return last_file_creation_time_;
    }
    int32_t TotalBuckets() const {
        return total_buckets_;
    }
    PartitionEntry Merge(const PartitionEntry& entry) const;

    static Result<PartitionEntry> FromManifestEntry(const ManifestEntry& entry) {
        return FromDataFile(entry.Partition(), entry.Kind(), entry.File(), entry.TotalBuckets());
    }

    static Result<PartitionEntry> FromDataFile(const BinaryRow& partition, const FileKind& kind,
                                               const std::shared_ptr<DataFileMeta>& file,
                                               int32_t total_buckets);

    Result<PartitionStatistics> ToPartitionStatistics(
        const BinaryRowPartitionComputer* partition_computer) const;

    static Status Merge(const std::vector<ManifestEntry>& from,
                        std::unordered_map<BinaryRow, PartitionEntry>* to);

    bool operator==(const PartitionEntry& other) const;

 private:
    BinaryRow partition_;
    int64_t record_count_;
    int64_t file_size_in_bytes_;
    int64_t file_count_;
    int64_t last_file_creation_time_;
    int32_t total_buckets_;
};
}  // namespace paimon
