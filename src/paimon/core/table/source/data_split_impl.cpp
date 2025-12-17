/*
 * Copyright 2025-present Alibaba Inc.
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

#include "paimon/core/table/source/data_split_impl.h"

namespace paimon {

bool DataSplit::SimpleDataFileMeta::operator==(const SimpleDataFileMeta& other) const {
    if (this == &other) {
        return true;
    }
    return file_path == other.file_path && file_size == other.file_size &&
           row_count == other.row_count && min_sequence_number == other.min_sequence_number &&
           max_sequence_number == other.max_sequence_number && schema_id == other.schema_id &&
           level == other.level && creation_time == other.creation_time &&
           delete_row_count == other.delete_row_count;
}
std::string DataSplit::SimpleDataFileMeta::ToString() const {
    return fmt::format(
        "{{filePath: {}, fileSize: {}, rowCount: {}, minSequenceNumber: {}, "
        "maxSequenceNumber:{}, schemaId: {}, level: {}, creationTime: {}, deleteRowCount: "
        "{}}}",
        file_path, file_size, row_count, min_sequence_number, max_sequence_number, schema_id, level,
        creation_time.ToString(),
        delete_row_count == std::nullopt ? "null" : std::to_string(delete_row_count.value()));
}

Result<std::optional<int64_t>> DataSplitImpl::LatestFileCreationEpochMillis() const {
    if (data_files_.empty()) {
        return std::optional<int64_t>();
    }
    int64_t epoch = INT64_MIN;
    for (const auto& file : data_files_) {
        PAIMON_ASSIGN_OR_RAISE(int64_t epoch_milli, file->CreationTimeEpochMillis());
        epoch = std::max(epoch, epoch_milli);
    }
    return std::optional<int64_t>(epoch);
}

int64_t DataSplitImpl::RowCount() const {
    int64_t row_count = 0;
    for (const auto& file : data_files_) {
        row_count += file->row_count;
    }
    return row_count;
}

std::vector<DataSplit::SimpleDataFileMeta> DataSplitImpl::GetFileList() const {
    std::vector<DataSplit::SimpleDataFileMeta> result_files;
    result_files.reserve(data_files_.size());
    for (const auto& file : data_files_) {
        std::string result_file_path;
        if (!file->external_path) {
            result_file_path = PathUtil::JoinPath(bucket_path_, file->file_name);
        } else {
            result_file_path = file->external_path.value();
        }
        result_files.emplace_back(result_file_path, file->file_size, file->row_count,
                                  file->min_sequence_number, file->max_sequence_number,
                                  file->schema_id, file->level, file->creation_time,
                                  file->delete_row_count);
    }
    return result_files;
}

bool DataSplitImpl::operator==(const DataSplitImpl& other) const {
    if (this == &other) {
        return true;
    }
    return snapshot_id_ == other.snapshot_id_ && partition_ == other.partition_ &&
           bucket_ == other.bucket_ && bucket_path_ == other.bucket_path_ &&
           total_buckets_ == other.total_buckets_ &&
           ObjectUtils::Equal(before_files_, other.before_files_) &&
           before_deletion_files_ == other.before_deletion_files_ &&
           ObjectUtils::Equal(data_files_, other.data_files_) &&
           data_deletion_files_ == other.data_deletion_files_ &&
           is_streaming_ == other.is_streaming_ && raw_convertible_ == other.raw_convertible_;
}

bool DataSplitImpl::TEST_Equal(const DataSplitImpl& other) const {
    if (this == &other) {
        return true;
    }
    return snapshot_id_ == other.snapshot_id_ && partition_ == other.partition_ &&
           bucket_ == other.bucket_ && bucket_path_ == other.bucket_path_ &&
           total_buckets_ == other.total_buckets_ &&
           ObjectUtils::TEST_Equal(before_files_, other.before_files_) &&
           before_deletion_files_ == other.before_deletion_files_ &&
           ObjectUtils::TEST_Equal(data_files_, other.data_files_) &&
           data_deletion_files_ == other.data_deletion_files_ &&
           is_streaming_ == other.is_streaming_ && raw_convertible_ == other.raw_convertible_;
}

int64_t DataSplitImpl::PartialMergedRowCount() const {
    if (!raw_convertible_) {
        return 0l;
    }
    int64_t sum = 0;
    for (size_t i = 0; i < data_files_.size(); i++) {
        const auto& data_file = data_files_[i];
        if (data_deletion_files_.empty() || data_deletion_files_[i] == std::nullopt) {
            sum += data_file->row_count;
        } else if (data_deletion_files_[i].value().cardinality != std::nullopt) {
            sum += data_file->row_count - data_deletion_files_[i].value().cardinality.value();
        }
    }
    return sum;
}

Result<std::unique_ptr<ObjectSerializer<std::shared_ptr<DataFileMeta>>>>
DataSplitImpl::GetFileMetaSerializer(int32_t version, const std::shared_ptr<MemoryPool>& pool) {
    if (version == 1) {
        // TODO(xinyu.lxy): C++ paimon do not support data file meta 08
        return Status::NotImplemented("Do not support data file meta 08.");
    } else if (version == 2) {
        return std::make_unique<DataFileMeta09Serializer>(pool);
    } else if (version == 3 || version == 4) {
        return std::make_unique<DataFileMeta10Serializer>(pool);
    } else if (version == 5 || version == 6) {
        return std::make_unique<DataFileMeta12Serializer>(pool);
    } else if (version == 7) {
        return std::make_unique<DataFileMetaFirstRowIdLegacySerializer>(pool);
    } else if (version == VERSION) {
        return std::make_unique<DataFileMetaSerializer>(pool);
    } else {
        return Status::Invalid(
            fmt::format("Expecting DataSplit version to be smaller or equal than {}, but found {}.",
                        VERSION, version));
    }
}

std::string DataSplitImpl::ToString() const {
    return fmt::format(
        "snapshotId={}, partition={}, bucket={}, bucketPath={}, totalBuckets={}, "
        "beforeFiles={}, "
        "beforeDeletionFiles={}, dataFiles={}, dataDeletionFiles={}, isStreaming={}, "
        "rawConvertible={}",
        snapshot_id_, partition_.ToString(), bucket_, bucket_path_,
        total_buckets_ == std::nullopt ? "null" : std::to_string(total_buckets_.value()),
        StringUtils::VectorToString(before_files_),
        StringUtils::VectorToString(before_deletion_files_),
        StringUtils::VectorToString(data_files_), StringUtils::VectorToString(data_deletion_files_),
        is_streaming_, raw_convertible_);
}

}  // namespace paimon
