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

#include "paimon/core/operation/key_value_file_store_write.h"

#include <optional>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/mergetree/merge_tree_writer.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/key_value_file_store_scan.h"
#include "paimon/core/operation/restore_files.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class DataFilePathFactory;
class Executor;
class MemoryPool;
class SchemaManager;
struct KeyValue;
template <typename T>
class MergeFunctionWrapper;

KeyValueFileStoreWrite::KeyValueFileStoreWrite(
    const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager, const std::string& commit_user,
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::Schema>& partition_schema,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
    const CoreOptions& options, bool ignore_previous_files, bool is_streaming_mode,
    bool ignore_num_bucket_check, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool)
    : AbstractFileStoreWrite(file_store_path_factory, snapshot_manager, schema_manager, commit_user,
                             root_path, table_schema, schema, /*write_schema=*/schema,
                             partition_schema, options, ignore_previous_files, is_streaming_mode,
                             ignore_num_bucket_check, executor, pool),
      key_comparator_(key_comparator),
      user_defined_seq_comparator_(user_defined_seq_comparator),
      merge_function_wrapper_(merge_function_wrapper),
      logger_(Logger::GetLogger("KeyValueFileStoreWrite")) {}

Result<std::unique_ptr<FileStoreScan>> KeyValueFileStoreWrite::CreateFileStoreScan(
    const std::shared_ptr<ScanFilter>& scan_filter) const {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(options_.GetFileSystem(), options_.GetManifestFormat(),
                             options_.GetManifestCompression(), file_store_path_factory_, pool_));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(options_.GetFileSystem(), options_.GetManifestFormat(),
                             options_.GetManifestCompression(), file_store_path_factory_,
                             options_.GetManifestTargetFileSize(), pool_, options_,
                             partition_schema_));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan,
                           KeyValueFileStoreScan::Create(
                               snapshot_manager_, schema_manager_, manifest_list, manifest_file,
                               table_schema_, schema_, scan_filter, options_, executor_, pool_));
    return scan;
}

Result<std::pair<int32_t, std::shared_ptr<BatchWriter>>> KeyValueFileStoreWrite::CreateWriter(
    const BinaryRow& partition, int32_t bucket, bool ignore_previous_files) {
    PAIMON_LOG_DEBUG(logger_, "Creating key value writer for partition %s, bucket %d",
                     partition.ToString().c_str(), bucket);
    int32_t total_buckets = GetDefaultBucketNum();
    std::vector<std::shared_ptr<DataFileMeta>> restore_data_files;
    if (!ignore_previous_files) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RestoreFiles> restore_files,
                               ScanExistingFileMetas(partition, bucket));
        restore_data_files = restore_files->DataFiles();
        if (restore_files->TotalBuckets()) {
            total_buckets = restore_files->TotalBuckets().value();
        }
    }
    int64_t max_sequence_number = DataFileMeta::GetMaxSequenceNumber(restore_data_files);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                           file_store_path_factory_->CreateDataFilePathFactory(partition, bucket));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema_->TrimmedPrimaryKeys());
    auto writer = std::make_shared<MergeTreeWriter>(
        max_sequence_number, trimmed_primary_keys, data_file_path_factory, key_comparator_,
        user_defined_seq_comparator_, merge_function_wrapper_, table_schema_->Id(), schema_,
        options_, pool_);
    return std::pair<int32_t, std::shared_ptr<BatchWriter>>(total_buckets, writer);
}

}  // namespace paimon
