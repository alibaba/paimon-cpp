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

#include "paimon/core/manifest/partition_entry.h"

#include <algorithm>
#include <map>
#include <string>
#include <tuple>
#include <utility>

#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"

namespace paimon {
PartitionEntry::PartitionEntry(const BinaryRow& partition, int64_t record_count,
                               int64_t file_size_in_bytes, int64_t file_count,
                               int64_t last_file_creation_time, int32_t total_buckets)
    : partition_(partition),
      record_count_(record_count),
      file_size_in_bytes_(file_size_in_bytes),
      file_count_(file_count),
      last_file_creation_time_(last_file_creation_time),
      total_buckets_(total_buckets) {}

PartitionEntry PartitionEntry::Merge(const PartitionEntry& entry) const {
    return PartitionEntry(
        partition_, record_count_ + entry.record_count_,
        file_size_in_bytes_ + entry.file_size_in_bytes_, file_count_ + entry.file_count_,
        std::max(last_file_creation_time_, entry.last_file_creation_time_), entry.total_buckets_);
}

Result<PartitionEntry> PartitionEntry::FromDataFile(const BinaryRow& partition,
                                                    const FileKind& kind,
                                                    const std::shared_ptr<DataFileMeta>& file,
                                                    const int32_t total_buckets) {
    int64_t record_count = kind == FileKind::Delete() ? -file->row_count : file->row_count;
    int64_t file_size_in_bytes = kind == FileKind::Delete() ? -file->file_size : file->file_size;
    int64_t file_count = kind == FileKind::Delete() ? -1 : 1;
    PAIMON_ASSIGN_OR_RAISE(int64_t utc_millis, file->CreationTimeEpochMillis());
    return PartitionEntry(partition, record_count, file_size_in_bytes, file_count, utc_millis,
                          total_buckets);
}

Status PartitionEntry::Merge(const std::vector<ManifestEntry>& from,
                             std::unordered_map<BinaryRow, PartitionEntry>* to) {
    for (const auto& entry : from) {
        PAIMON_ASSIGN_OR_RAISE(PartitionEntry partition_entry, FromManifestEntry(entry));
        auto iter = to->find(partition_entry.Partition());
        if (iter == to->end()) {
            to->emplace(std::piecewise_construct,
                        std::forward_as_tuple(partition_entry.Partition()),
                        std::forward_as_tuple(partition_entry));
        } else {
            iter->second = iter->second.Merge(partition_entry);
        }
    }
    return Status::OK();
}

Result<PartitionStatistics> PartitionEntry::ToPartitionStatistics(
    const BinaryRowPartitionComputer* partition_computer) const {
    std::vector<std::pair<std::string, std::string>> part_values;
    PAIMON_ASSIGN_OR_RAISE(part_values, partition_computer->GeneratePartitionVector(partition_));
    std::map<std::string, std::string> part_values_map(part_values.begin(), part_values.end());
    return PartitionStatistics(part_values_map, record_count_, file_size_in_bytes_, file_count_,
                               last_file_creation_time_, total_buckets_);
}

bool PartitionEntry::operator==(const PartitionEntry& other) const {
    if (this == &other) {
        return true;
    }
    return partition_ == other.partition_ && record_count_ == other.record_count_ &&
           file_size_in_bytes_ == other.file_size_in_bytes_ && file_count_ == other.file_count_ &&
           last_file_creation_time_ == other.last_file_creation_time_ &&
           total_buckets_ == other.total_buckets_;
}

}  // namespace paimon
