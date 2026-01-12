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

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class DataFilePathFactory;
class ExternalPathProvider;
class PathFactory;
class MemoryPool;

class FileStorePathFactory : public std::enable_shared_from_this<FileStorePathFactory> {
 public:
    static constexpr char BUCKET_PATH_PREFIX[] = "bucket-";

    static Result<std::unique_ptr<FileStorePathFactory>> Create(
        const std::string& root, const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<std::string>& partition_keys, const std::string& default_part_value,
        const std::string& identifier, const std::string& data_file_prefix,
        bool legacy_partition_name_enabled, const std::vector<std::string>& external_paths,
        const std::optional<std::string>& global_index_external_path,
        bool index_file_in_data_file_dir, const std::shared_ptr<MemoryPool>& memory_pool);

    static std::string ManifestPath(const std::string& root) {
        return PathUtil::JoinPath(root, "/manifest");
    }
    static std::string IndexPath(const std::string& root) {
        return PathUtil::JoinPath(root, "/index");
    }
    static std::string StatisticsPath(const std::string& root) {
        return PathUtil::JoinPath(root, "/statistics");
    }

    std::unique_ptr<PathFactory> CreateManifestFileFactory();
    std::unique_ptr<PathFactory> CreateManifestListFactory();
    std::unique_ptr<PathFactory> CreateIndexManifestFileFactory();
    Result<std::unique_ptr<IndexPathFactory>> CreateIndexFileFactory(const BinaryRow& partition,
                                                                     int32_t bucket);
    std::unique_ptr<IndexPathFactory> CreateGlobalIndexFileFactory();
    Result<std::shared_ptr<DataFilePathFactory>> CreateDataFilePathFactory(
        const BinaryRow& partition, int32_t bucket) const;
    Result<BinaryRow> ToBinaryRow(const std::map<std::string, std::string>& partition) const;
    Result<std::string> BucketPath(const BinaryRow& partition, int32_t bucket) const;
    Result<std::string> RelativeBucketPath(const BinaryRow& partition, int32_t bucket) const;
    Result<std::vector<std::pair<std::string, std::string>>> GeneratePartitionVector(
        const BinaryRow& partition) const {
        return partition_computer_->GeneratePartitionVector(partition);
    }
    Result<std::vector<std::string>> GetHierarchicalPartitionPath(const BinaryRow& partition) const;

    const std::string& RootPath() const {
        return root_;
    }
    const std::string& UUID() const {
        return uuid_;
    }
    const std::vector<std::string>& GetExternalPaths() const {
        return external_paths_;
    }

    const std::optional<std::string>& GetGlobalIndexExternalPath() const {
        return global_index_external_path_;
    }

    /// @note This method is NOT THREAD SAFE.
    Result<std::string> GetPartitionString(const BinaryRow& partition) const;
    std::string NewManifestFile() const {
        return PathUtil::JoinPath(
            ManifestPath(root_),
            "manifest-" + uuid_ + "-" + std::to_string(manifest_file_count_.fetch_add(1)));
    }
    std::string NewManifestList() const {
        return PathUtil::JoinPath(
            ManifestPath(root_),
            "manifest-list-" + uuid_ + "-" + std::to_string(manifest_list_count_.fetch_add(1)));
    }
    std::string NewIndexManifestFile() const {
        return PathUtil::JoinPath(
            ManifestPath(root_),
            "index-manifest-" + uuid_ + "-" + std::to_string(index_manifest_count_.fetch_add(1)));
    }
    std::string NewIndexFile() const {
        return PathUtil::JoinPath(IndexPath(root_),
                                  IndexPathFactory::INDEX_PREFIX + uuid_ + "-" +
                                      std::to_string(index_file_count_->fetch_add(1)));
    }
    std::string NewStatsFile() const {
        return PathUtil::JoinPath(
            StatisticsPath(root_),
            "stats-" + uuid_ + "-" + std::to_string(stats_file_count_.fetch_add(1)));
    }
    std::string ToManifestFilePath(const std::string& file_name) const {
        return PathUtil::JoinPath(ManifestPath(root_), file_name);
    }
    std::string ToManifestListPath(const std::string& file_name) const {
        return PathUtil::JoinPath(ManifestPath(root_), file_name);
    }
    std::string ToIndexFilePath(const std::string& file_name) const {
        return PathUtil::JoinPath(IndexPath(root_), file_name);
    }
    std::string ToStatsFilePath(const std::string& file_name) const {
        return PathUtil::JoinPath(StatisticsPath(root_), file_name);
    }

 private:
    FileStorePathFactory(const std::string& root, const std::string& format_identifier,
                         const std::string& data_file_prefix, const std::string& uuid,
                         std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
                         const std::vector<std::string>& external_paths,
                         const std::optional<std::string>& global_index_external_path,
                         bool index_file_in_data_file_dir);

    Result<std::unique_ptr<ExternalPathProvider>> CreateExternalPathProvider(
        const BinaryRow& partition, int32_t bucket) const;

 private:
    std::string root_;
    std::string format_identifier_;
    std::string data_file_prefix_;
    std::string uuid_;
    std::unique_ptr<BinaryRowPartitionComputer> partition_computer_;
    std::vector<std::string> external_paths_;
    std::optional<std::string> global_index_external_path_;
    bool index_file_in_data_file_dir_;

    mutable std::atomic<int32_t> manifest_file_count_ = 0;
    mutable std::atomic<int32_t> manifest_list_count_ = 0;
    mutable std::atomic<int32_t> index_manifest_count_ = 0;
    std::shared_ptr<std::atomic<int32_t>> index_file_count_ =
        std::make_shared<std::atomic<int32_t>>(0);
    mutable std::atomic<int32_t> stats_file_count_ = 0;

    mutable std::map<std::map<std::string, std::string>, BinaryRow> map_to_row_cache_;
    mutable std::unordered_map<BinaryRow, std::string> row_to_str_cache_;
};

}  // namespace paimon
