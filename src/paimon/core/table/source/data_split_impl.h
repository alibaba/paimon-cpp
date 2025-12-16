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

#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/preconditions.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta_09_serializer.h"
#include "paimon/core/io/data_file_meta_10_serializer.h"
#include "paimon/core/io/data_file_meta_12_serializer.h"
#include "paimon/core/io/data_file_meta_first_row_id_legacy_serializer.h"
#include "paimon/core/io/data_file_meta_serializer.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/table/source/data_split.h"

namespace paimon {
/// Input splits. Needed by most batch computation engines.
class DataSplitImpl : public DataSplit {
 public:
    static constexpr int64_t MAGIC = -2394839472490812314L;
    static constexpr int32_t VERSION = 8;

    int64_t SnapshotId() const {
        return snapshot_id_;
    }

    const BinaryRow& Partition() const {
        return partition_;
    }

    int32_t Bucket() const {
        return bucket_;
    }

    const std::string& BucketPath() const {
        return bucket_path_;
    }

    const std::optional<int32_t>& TotalBuckets() const {
        return total_buckets_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& BeforeFiles() const {
        return before_files_;
    }

    const std::vector<std::optional<DeletionFile>>& BeforeDeletionFiles() const {
        return before_deletion_files_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& DataFiles() const {
        return data_files_;
    }

    const std::vector<std::optional<DeletionFile>>& DeletionFiles() const {
        return data_deletion_files_;
    }

    bool IsStreaming() const {
        return is_streaming_;
    }

    bool RawConvertible() const {
        return raw_convertible_;
    }

    Result<std::optional<int64_t>> LatestFileCreationEpochMillis() const;

    int64_t RowCount() const;

    std::vector<SimpleDataFileMeta> GetFileList() const override;

    bool operator==(const DataSplitImpl& other) const;
    bool TEST_Equal(const DataSplitImpl& other) const;

    /// Obtain merged row count as much as possible. There are two scenarios where accurate row
    /// count
    /// can be calculated:
    ///
    /// 1. raw file and no deletion file.
    ///
    /// 2. raw file + deletion file with cardinality.
    int64_t PartialMergedRowCount() const;

    // Builder
    /// Builder for `DataSplitImpl`.
    class Builder {
     public:
        Builder(const BinaryRow& partition, int32_t bucket, const std::string& bucket_path,
                std::vector<std::shared_ptr<DataFileMeta>>&& data_files)
            : split_(std::shared_ptr<DataSplitImpl>(
                  new DataSplitImpl(partition, bucket, bucket_path, std::move(data_files)))) {}

        const std::vector<std::shared_ptr<DataFileMeta>>& DataFiles() {
            return split_->DataFiles();
        }

        Builder& WithTotalBuckets(const std::optional<int32_t>& total_buckets) {
            split_->total_buckets_ = total_buckets;
            return *this;
        }

        Builder& WithSnapshot(int64_t snapshot) {
            split_->snapshot_id_ = snapshot;
            return *this;
        }

        Builder& WithBeforeFiles(std::vector<std::shared_ptr<DataFileMeta>>&& before_files) {
            split_->before_files_ = std::move(before_files);
            return *this;
        }

        Builder& WithBeforeDeletionFiles(
            const std::vector<std::optional<DeletionFile>>& before_deletion_files) {
            split_->before_deletion_files_ = before_deletion_files;
            return *this;
        }

        Builder& WithDataDeletionFiles(
            const std::vector<std::optional<DeletionFile>>& data_deletion_files) {
            split_->data_deletion_files_ = data_deletion_files;
            return *this;
        }

        Builder& IsStreaming(bool is_streaming) {
            split_->is_streaming_ = is_streaming;
            return *this;
        }

        Builder& RawConvertible(bool raw_convertible) {
            split_->raw_convertible_ = raw_convertible;
            return *this;
        }

        Result<std::shared_ptr<DataSplitImpl>> Build() const {
            PAIMON_RETURN_NOT_OK(Preconditions::CheckArgument(split_->bucket_ != -1));
            return split_;
        }

     private:
        std::shared_ptr<DataSplitImpl> split_;
    };

    static Result<std::unique_ptr<ObjectSerializer<std::shared_ptr<DataFileMeta>>>>
    GetFileMetaSerializer(int32_t version, const std::shared_ptr<MemoryPool>& pool);

    std::string ToString() const;

 private:
    DataSplitImpl(const BinaryRow& partition, int32_t bucket, const std::string& bucket_path,
                  std::vector<std::shared_ptr<DataFileMeta>>&& data_files)
        : partition_(partition),
          bucket_(bucket),
          bucket_path_(bucket_path),
          data_files_(std::move(data_files)) {}

 private:
    int64_t snapshot_id_ = 0;
    BinaryRow partition_ = BinaryRow::EmptyRow();
    int32_t bucket_ = -1;
    std::string bucket_path_;
    std::optional<int32_t> total_buckets_;

    std::vector<std::shared_ptr<DataFileMeta>> before_files_;
    std::vector<std::optional<DeletionFile>> before_deletion_files_;
    std::vector<std::shared_ptr<DataFileMeta>> data_files_;
    std::vector<std::optional<DeletionFile>> data_deletion_files_;

    bool is_streaming_ = false;
    bool raw_convertible_ = false;
};
}  // namespace paimon
