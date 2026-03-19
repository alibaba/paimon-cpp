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
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/result.h"

namespace paimon {
class Snapshot;

class IndexFileHandler {
 public:
    using IndexFileMetaGroups = std::unordered_map<std::pair<BinaryRow, int32_t>,
                                                   std::vector<std::shared_ptr<IndexFileMeta>>>;

    IndexFileHandler(const std::shared_ptr<FileSystem>& fs,
                     std::unique_ptr<IndexManifestFile>&& index_manifest_file,
                     const std::shared_ptr<IndexFilePathFactories>& path_factories,
                     bool dv_bitmap64, const std::shared_ptr<MemoryPool>& pool)
        : fs_(fs),
          index_manifest_file_(std::move(index_manifest_file)),
          path_factories_(path_factories),
          dv_bitmap64_(dv_bitmap64),
          pool_(pool) {}

    /// 1.Scan specified index_type index. 2.Cluster with partition & bucket.
    Result<IndexFileMetaGroups> Scan(const Snapshot& snapshot, const std::string& index_type,
                                     const std::unordered_set<BinaryRow>& partitions) const;

    Result<std::vector<std::shared_ptr<IndexFileMeta>>> Scan(const Snapshot& snapshot,
                                                             const std::string& index_type,
                                                             const BinaryRow& partition,
                                                             int32_t bucket) const;

    /// Scan specified all typed index.
    Result<std::vector<IndexManifestEntry>> Scan(
        const Snapshot& snapshot,
        std::function<Result<bool>(const IndexManifestEntry&)> filter) const;

    Result<std::string> FilePath(const BinaryRow& partition, int32_t bucket,
                                 const std::shared_ptr<IndexFileMeta>& file) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexPathFactory> factory,
                               path_factories_->Get(partition, bucket));
        return factory->ToPath(file);
    }

    Result<std::unique_ptr<DeletionVectorsIndexFile>> DvIndex(const BinaryRow& partition,
                                                              int32_t bucket) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexPathFactory> index_path_factory,
                               path_factories_->Get(partition, bucket));
        return std::make_unique<DeletionVectorsIndexFile>(fs_, index_path_factory, dv_bitmap64_,
                                                          pool_);
    }

    Result<std::map<std::string, std::shared_ptr<DeletionVector>>> ReadAllDeletionVectors(
        const BinaryRow& partition, int32_t bucket,
        const std::vector<std::shared_ptr<IndexFileMeta>>& file_metas) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<DeletionVectorsIndexFile> dv_index,
                               DvIndex(partition, bucket));
        return dv_index->ReadAllDeletionVectors(file_metas);
    }

 private:
    std::shared_ptr<FileSystem> fs_;
    std::unique_ptr<IndexManifestFile> index_manifest_file_;
    std::shared_ptr<IndexFilePathFactories> path_factories_;
    bool dv_bitmap64_;
    std::shared_ptr<MemoryPool> pool_;
};
}  // namespace paimon
