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

#include <map>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "paimon/common/utils/preconditions.h"
#include "paimon/core/operation/abstract_file_store_write.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/postpone/postpone_bucket_writer.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/format/file_format.h"

namespace paimon {

class PostponeBucketFileStoreWrite : public AbstractFileStoreWrite {
 public:
    static Result<std::unique_ptr<PostponeBucketFileStoreWrite>> Create(
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<SchemaManager>& schema_manager, const std::string& commit_user,
        const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<arrow::Schema>& schema,
        const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& options,
        bool is_streaming_mode, bool ignore_num_bucket_check,
        const std::optional<int32_t>& write_id,
        const std::map<std::string, std::string>& fs_scheme_to_identifier_map,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool,
        const std::shared_ptr<FileSystem>& file_system) {
        // Each writer should have its unique prefix, so files from the same writer can be consumed
        // by the same compaction reader to keep the input order.
        std::optional<int32_t> id = write_id;
        if (id == std::nullopt) {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<int32_t> dist(0, INT32_MAX - 1);
            id = std::optional<int32_t>(dist(gen));
        }

        auto options_map = options.ToMap();
        options_map[Options::DATA_FILE_PREFIX] =
            fmt::format("{}-u-{}-s-{}-w-", options.DataFilePrefix(), commit_user, id.value());
        PAIMON_ASSIGN_OR_RAISE(
            CoreOptions new_options,
            CoreOptions::FromMap(options_map, file_system, fs_scheme_to_identifier_map));
        // prepare FileStorePathFactory
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                               new_options.CreateExternalPaths());
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                               new_options.CreateGlobalIndexExternalPath());

        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileStorePathFactory> file_store_path_factory,
            FileStorePathFactory::Create(
                root_path, schema, table_schema->PartitionKeys(),
                new_options.GetPartitionDefaultName(),
                new_options.GetWriteFileFormat()->Identifier(), new_options.DataFilePrefix(),
                new_options.LegacyPartitionNameEnabled(), external_paths,
                global_index_external_path, new_options.IndexFileInDataFileDir(), pool));

        // Ignoring previous files saves scanning time.
        // For postpone bucket tables, we only append new files to bucket = -2 directories.
        // Also, we don't need to know current largest sequence id, because when compacting these
        // files, we will read the records file by file without merging, and then give them to
        // normal bucket writers.
        // Because there is no merging when reading, sequence id across files are useless.
        return std::unique_ptr<PostponeBucketFileStoreWrite>(new PostponeBucketFileStoreWrite(
            file_store_path_factory, snapshot_manager, schema_manager, commit_user, root_path,
            table_schema, schema, partition_schema, new_options, /*ignore_previous_files=*/true,
            is_streaming_mode, ignore_num_bucket_check, executor, pool));
    }

 private:
    PostponeBucketFileStoreWrite(
        const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<SchemaManager>& schema_manager, const std::string& commit_user,
        const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<arrow::Schema>& schema,
        const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& options,
        bool ignore_previous_files, bool is_streaming_mode, bool ignore_num_bucket_check,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool)
        : AbstractFileStoreWrite(
              file_store_path_factory, snapshot_manager, schema_manager, commit_user, root_path,
              table_schema, schema, /*write_schema=*/schema, partition_schema, options,
              ignore_previous_files, is_streaming_mode, ignore_num_bucket_check, executor, pool) {}

    Result<std::pair<int32_t, std::shared_ptr<BatchWriter>>> CreateWriter(
        const BinaryRow& partition, int32_t bucket, bool ignore_previous_files) override {
        PAIMON_RETURN_NOT_OK(
            Preconditions::CheckState(bucket == BucketModeDefine::POSTPONE_BUCKET,
                                      "bucket mode is supposed to be postpone bucket"));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                               table_schema_->TrimmedPrimaryKeys());
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<DataFilePathFactory> data_file_path_factory,
            file_store_path_factory_->CreateDataFilePathFactory(partition, bucket));
        auto writer =
            std::make_shared<PostponeBucketWriter>(trimmed_primary_keys, data_file_path_factory,
                                                   table_schema_->Id(), schema_, options_, pool_);
        int32_t total_buckets = GetDefaultBucketNum();
        return std::pair<int32_t, std::shared_ptr<BatchWriter>>(total_buckets, writer);
    }

    Result<std::unique_ptr<FileStoreScan>> CreateFileStoreScan(
        const std::shared_ptr<ScanFilter>& filter) const override {
        return Status::Invalid("do not support scan process in PostponeBucketFileStoreWrite");
    }
};

}  // namespace paimon
