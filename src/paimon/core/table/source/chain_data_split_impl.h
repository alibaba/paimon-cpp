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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/table/source/data_split_impl.h"

namespace paimon {

class ChainDataSplitImpl : public DataSplitImpl {
 public:
    static constexpr const char* VIRTUAL_BUCKET_PATH = "placeholder::virtual-bucket-path";

    ChainDataSplitImpl(const std::shared_ptr<DataSplitImpl>& base_split, bool all_snapshot_split,
                       const BinaryRow& read_partition,
                       std::unordered_map<std::string, std::string>&& file_bucket_path_mapping,
                       std::unordered_map<std::string, std::string>&& file_branch_mapping)
        : DataSplitImpl(base_split->Partition(), base_split->Bucket(), base_split->BucketPath(),
                        std::vector<std::shared_ptr<DataFileMeta>>(base_split->DataFiles())),
          all_snapshot_split_(all_snapshot_split),
          read_partition_(read_partition),
          file_bucket_path_mapping_(std::move(file_bucket_path_mapping)),
          file_branch_mapping_(std::move(file_branch_mapping)) {
        CopyMetadataFrom(*base_split);
    }

    bool AllSnapshotSplit() const {
        return all_snapshot_split_;
    }

    const BinaryRow& ReadPartition() const {
        return read_partition_;
    }

    const std::unordered_map<std::string, std::string>& FileBucketPathMapping() const {
        return file_bucket_path_mapping_;
    }

    const std::unordered_map<std::string, std::string>& FileBranchMapping() const {
        return file_branch_mapping_;
    }

 private:
    bool all_snapshot_split_;
    BinaryRow read_partition_;
    std::unordered_map<std::string, std::string> file_bucket_path_mapping_;
    std::unordered_map<std::string, std::string> file_branch_mapping_;
};

}  // namespace paimon
