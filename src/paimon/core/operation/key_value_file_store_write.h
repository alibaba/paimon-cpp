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
#include <utility>

#include "paimon/core/mergetree/compact/merge_function_wrapper.h"
#include "paimon/core/operation/abstract_file_store_write.h"
#include "paimon/core/utils/batch_writer.h"
#include "paimon/logging.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class FieldsComparator;
class FileStoreScan;
class ScanFilter;
class BinaryRow;
class CoreOptions;
class Executor;
class FileStorePathFactory;
class MemoryPool;
class SchemaManager;
class SnapshotManager;
class TableSchema;
struct KeyValue;
template <typename T>
class MergeFunctionWrapper;

class KeyValueFileStoreWrite : public AbstractFileStoreWrite {
 public:
    KeyValueFileStoreWrite(
        const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<SchemaManager>& schema_manager, const std::string& commit_user,
        const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<arrow::Schema>& schema,
        const std::shared_ptr<arrow::Schema>& partition_schema,
        const std::shared_ptr<BucketedDvMaintainer::Factory>& dv_maintainer_factory,
        const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
        const CoreOptions& options, bool ignore_previous_files, bool is_streaming_mode,
        bool ignore_num_bucket_check, const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& pool);

 private:
    Result<std::shared_ptr<BatchWriter>> CreateWriter(
        const BinaryRow& partition, int32_t bucket,
        const std::vector<std::shared_ptr<DataFileMeta>>& restore_data_files,
        int64_t restore_max_seq_number,
        const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer) override;

    Result<std::unique_ptr<FileStoreScan>> CreateFileStoreScan(
        const std::shared_ptr<ScanFilter>& filter) const override;

 private:
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<FieldsComparator> user_defined_seq_comparator_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon
