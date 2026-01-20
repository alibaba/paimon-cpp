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

#include "paimon/core/utils/file_store_path_factory.h"

#include <cassert>

#include "paimon/common/fs/external_path_provider.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_in_data_file_dir_path_factory.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/utils/partition_path_utils.h"
#include "paimon/core/utils/path_factory.h"
#include "paimon/macros.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class MemoryPool;

FileStorePathFactory::FileStorePathFactory(
    const std::string& root, const std::string& format_identifier,
    const std::string& data_file_prefix, const std::string& uuid,
    std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
    const std::vector<std::string>& external_paths,
    const std::optional<std::string>& global_index_external_path, bool index_file_in_data_file_dir)
    : root_(root),
      format_identifier_(format_identifier),
      data_file_prefix_(data_file_prefix),
      uuid_(uuid),
      partition_computer_(std::move(partition_computer)),
      external_paths_(external_paths),
      global_index_external_path_(global_index_external_path),
      index_file_in_data_file_dir_(index_file_in_data_file_dir) {}

Result<std::unique_ptr<FileStorePathFactory>> FileStorePathFactory::Create(
    const std::string& root, const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<std::string>& partition_keys, const std::string& default_part_value,
    const std::string& identifier, const std::string& data_file_prefix,
    bool legacy_partition_name_enabled, const std::vector<std::string>& external_paths,
    const std::optional<std::string>& global_index_external_path, bool index_file_in_data_file_dir,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (memory_pool == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    std::string uuid;
    if (PAIMON_UNLIKELY(!UUID::Generate(&uuid))) {
        return Status::Invalid("fail to generate uuid for file store path factory");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
        BinaryRowPartitionComputer::Create(partition_keys, schema, default_part_value,
                                           legacy_partition_name_enabled, memory_pool));
    return std::unique_ptr<FileStorePathFactory>(new FileStorePathFactory(
        root, identifier, data_file_prefix, uuid, std::move(partition_computer), external_paths,
        global_index_external_path, index_file_in_data_file_dir));
}

std::unique_ptr<PathFactory> FileStorePathFactory::CreateManifestFileFactory() {
    class ManifestFileFactory : public PathFactory {
     public:
        explicit ManifestFileFactory(const std::shared_ptr<FileStorePathFactory>& factory)
            : factory_(factory) {
            assert(factory_);
        }

        std::string NewPath() const override {
            return factory_->NewManifestFile();
        }
        std::string ToPath(const std::string& file_name) const override {
            return factory_->ToManifestFilePath(file_name);
        }

     private:
        std::shared_ptr<FileStorePathFactory> factory_ = nullptr;
    };
    return std::make_unique<ManifestFileFactory>(shared_from_this());
}
std::unique_ptr<PathFactory> FileStorePathFactory::CreateManifestListFactory() {
    class ManifestListFactory : public PathFactory {
     public:
        explicit ManifestListFactory(const std::shared_ptr<FileStorePathFactory>& factory)
            : factory_(factory) {
            assert(factory_);
        }

        std::string NewPath() const override {
            return factory_->NewManifestList();
        }
        std::string ToPath(const std::string& file_name) const override {
            return factory_->ToManifestListPath(file_name);
        }

     private:
        std::shared_ptr<FileStorePathFactory> factory_;
    };
    return std::make_unique<ManifestListFactory>(shared_from_this());
}
std::unique_ptr<PathFactory> FileStorePathFactory::CreateIndexManifestFileFactory() {
    class IndexManifestFileFactory : public PathFactory {
     public:
        explicit IndexManifestFileFactory(const std::shared_ptr<FileStorePathFactory>& factory)
            : factory_(factory) {
            assert(factory_);
        }

        std::string NewPath() const override {
            return factory_->NewIndexManifestFile();
        }
        std::string ToPath(const std::string& file_name) const override {
            return factory_->ToManifestFilePath(file_name);
        }

     private:
        std::shared_ptr<FileStorePathFactory> factory_;
    };
    return std::make_unique<IndexManifestFileFactory>(shared_from_this());
}

Result<std::unique_ptr<IndexPathFactory>> FileStorePathFactory::CreateIndexFileFactory(
    const BinaryRow& partition, int32_t bucket) {
    if (index_file_in_data_file_dir_) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                               CreateDataFilePathFactory(partition, bucket));
        return std::make_unique<IndexInDataFileDirPathFactory>(uuid_, index_file_count_,
                                                               data_file_path_factory);
    }
    return CreateGlobalIndexFileFactory();
}

std::unique_ptr<IndexPathFactory> FileStorePathFactory::CreateGlobalIndexFileFactory() {
    class IndexPathFactoryImpl : public IndexPathFactory {
     public:
        explicit IndexPathFactoryImpl(const std::shared_ptr<FileStorePathFactory>& factory)
            : factory_(factory) {
            assert(factory_);
        }
        std::string NewPath() const override {
            return factory_->NewIndexFile();
        }
        std::string ToPath(const std::shared_ptr<IndexFileMeta>& file) const override {
            const auto& external_path = file->ExternalPath();
            if (external_path) {
                return external_path.value();
            }
            return PathUtil::JoinPath(factory_->IndexPath(factory_->RootPath()), file->FileName());
        }
        std::string ToPath(const std::string& file_name) const override {
            const auto& external_path = factory_->GetGlobalIndexExternalPath();
            if (external_path) {
                return PathUtil::JoinPath(external_path.value(), file_name);
            }
            return PathUtil::JoinPath(factory_->IndexPath(factory_->RootPath()), file_name);
        }
        bool IsExternalPath() const override {
            return factory_->GetGlobalIndexExternalPath() != std::nullopt;
        }

     private:
        std::shared_ptr<FileStorePathFactory> factory_;
    };
    return std::make_unique<IndexPathFactoryImpl>(shared_from_this());
}

Result<std::shared_ptr<DataFilePathFactory>> FileStorePathFactory::CreateDataFilePathFactory(
    const BinaryRow& partition, int32_t bucket) const {
    auto data_file_path_factory = std::make_shared<DataFilePathFactory>();
    PAIMON_ASSIGN_OR_RAISE(std::string bucket_path, BucketPath(partition, bucket));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ExternalPathProvider> external_path_provider,
                           CreateExternalPathProvider(partition, bucket));
    PAIMON_RETURN_NOT_OK(data_file_path_factory->Init(
        bucket_path, format_identifier_, data_file_prefix_, std::move(external_path_provider)));
    return data_file_path_factory;
}

Result<std::unique_ptr<ExternalPathProvider>> FileStorePathFactory::CreateExternalPathProvider(
    const BinaryRow& partition, int32_t bucket) const {
    if (external_paths_.empty()) {
        return std::unique_ptr<ExternalPathProvider>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::string bucket_path, RelativeBucketPath(partition, bucket));
    return ExternalPathProvider::Create(external_paths_, bucket_path);
}

Result<std::string> FileStorePathFactory::RelativeBucketPath(const BinaryRow& partition,
                                                             int32_t bucket) const {
    PAIMON_ASSIGN_OR_RAISE(std::string partition_path, GetPartitionString(partition));
    std::string bucket_name =
        bucket == BucketModeDefine::POSTPONE_BUCKET ? "postpone" : std::to_string(bucket);
    return PathUtil::JoinPath(partition_path, std::string(BUCKET_PATH_PREFIX) + bucket_name);
}

Result<std::string> FileStorePathFactory::BucketPath(const BinaryRow& partition,
                                                     int32_t bucket) const {
    PAIMON_ASSIGN_OR_RAISE(std::string relative_bucket_path, RelativeBucketPath(partition, bucket));
    return PathUtil::JoinPath(root_, relative_bucket_path);
}

Result<std::string> FileStorePathFactory::GetPartitionString(const BinaryRow& partition) const {
    if (partition.GetSegments().size() == 0) {
        return Status::Invalid("invalid binary row partition");
    }
    auto iter = row_to_str_cache_.find(partition);
    if (PAIMON_LIKELY(iter != row_to_str_cache_.end())) {
        return iter->second;
    }
    std::vector<std::pair<std::string, std::string>> part_values;
    PAIMON_ASSIGN_OR_RAISE(part_values, partition_computer_->GeneratePartitionVector(partition));
    PAIMON_ASSIGN_OR_RAISE(std::string part_str,
                           PartitionPathUtils::GeneratePartitionPath(part_values))
    return row_to_str_cache_.insert({partition, part_str}).first->second;
}

Result<BinaryRow> FileStorePathFactory::ToBinaryRow(
    const std::map<std::string, std::string>& partition) const {
    auto iter = map_to_row_cache_.find(partition);
    if (PAIMON_LIKELY(iter != map_to_row_cache_.end())) {
        return iter->second;
    }
    PAIMON_ASSIGN_OR_RAISE(BinaryRow row, partition_computer_->ToBinaryRow(partition));
    return map_to_row_cache_.insert({partition, row}).first->second;
}

Result<std::vector<std::string>> FileStorePathFactory::GetHierarchicalPartitionPath(
    const BinaryRow& partition) const {
    std::vector<std::pair<std::string, std::string>> part_values;
    PAIMON_ASSIGN_OR_RAISE(part_values, partition_computer_->GeneratePartitionVector(partition));
    std::vector<std::string> result;
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> relative_paths,
                           PartitionPathUtils::GenerateHierarchicalPartitionPaths(part_values));
    for (const auto& relative_path : relative_paths) {
        result.push_back(PathUtil::JoinPath(root_, relative_path));
    }
    return result;
}

}  // namespace paimon
