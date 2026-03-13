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
#include <optional>
#include <vector>

#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/snapshot.h"

namespace paimon {

/// Restored files with snapshot and total buckets.
class RestoreFiles {
 public:
    RestoreFiles() = default;

    RestoreFiles(const std::optional<Snapshot>& snapshot,
                 const std::optional<int32_t>& total_buckets,
                 const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
                 const std::shared_ptr<IndexFileMeta>& dynamic_bucket_index,
                 const std::vector<std::shared_ptr<IndexFileMeta>>& delete_vectors_index)
        : snapshot_(snapshot),
          total_buckets_(total_buckets),
          data_files_(data_files),
          dynamic_bucket_index_(dynamic_bucket_index),
          delete_vectors_index_(delete_vectors_index) {}

    std::optional<Snapshot> GetSnapshot() const {
        return snapshot_;
    }
    std::optional<int32_t> TotalBuckets() const {
        return total_buckets_;
    }
    std::vector<std::shared_ptr<DataFileMeta>> DataFiles() const {
        return data_files_;
    }
    std::shared_ptr<IndexFileMeta> DynamicBucketIndex() const {
        return dynamic_bucket_index_;
    }
    std::vector<std::shared_ptr<IndexFileMeta>> DeleteVectorsIndex() const {
        return delete_vectors_index_;
    }

    static std::shared_ptr<RestoreFiles> Empty() {
        return std::make_shared<RestoreFiles>();
    }

 private:
    std::optional<Snapshot> snapshot_;
    std::optional<int32_t> total_buckets_;
    std::vector<std::shared_ptr<DataFileMeta>> data_files_;
    std::shared_ptr<IndexFileMeta> dynamic_bucket_index_;
    std::vector<std::shared_ptr<IndexFileMeta>> delete_vectors_index_;
};

}  // namespace paimon
