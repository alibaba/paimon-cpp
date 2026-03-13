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
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "paimon/commit_message.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/core_options.h"
#include "paimon/file_store_write.h"
#include "paimon/logging.h"
#include "paimon/metrics.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"

struct ArrowSchema;

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

struct DataFileMeta;
class BatchWriter;
class CompactionMetrics;
class FileStoreScan;
class FileStorePathFactory;
class ScanFilter;
class Snapshot;
class SnapshotManager;
class SchemaManager;
class TableSchema;
class MetricsImpl;
class BinaryRow;
class Executor;
class MemoryPool;
class RecordBatch;
class RestoreFiles;

class AbstractFileStoreWrite : public FileStoreWrite {
 public:
    // schema indicates all fields in table schema, write_schema indicates actual write fields while
    // "data-evolution.enabled" is true
    AbstractFileStoreWrite(const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
                           const std::shared_ptr<SnapshotManager>& snapshot_manager,
                           const std::shared_ptr<SchemaManager>& schema_manager,
                           const std::string& commit_user, const std::string& root_path,
                           const std::shared_ptr<TableSchema>& table_schema,
                           const std::shared_ptr<arrow::Schema>& schema,
                           const std::shared_ptr<arrow::Schema>& write_schema,
                           const std::shared_ptr<arrow::Schema>& partition_schema,
                           const CoreOptions& options, bool ignore_previous_files,
                           bool is_streaming_mode, bool ignore_num_bucket_check,
                           const std::shared_ptr<Executor>& executor,
                           const std::shared_ptr<MemoryPool>& pool);

    Status Write(std::unique_ptr<RecordBatch>&& batch) override;
    Status Compact(const std::map<std::string, std::string>& partition, int32_t bucket,
                   bool full_compaction) override;

    Result<std::vector<std::shared_ptr<CommitMessage>>> PrepareCommit(
        bool wait_compaction, int64_t commit_identifier) override;
    Status Close() override;
    std::shared_ptr<Metrics> GetMetrics() const override;

    const CoreOptions& GetOptions() const {
        return options_;
    }

    template <typename T>
    struct WriterContainer {
     public:
        WriterContainer() = default;
        WriterContainer(const std::shared_ptr<T>& writer, int32_t total_buckets)
            : writer(writer), total_buckets(total_buckets) {}
        std::shared_ptr<T> writer;
        int64_t last_modified_commit_identifier = std::numeric_limits<int64_t>::min();
        int32_t total_buckets = -1;
    };

 protected:
    // return actual total bucket and writer in the specific partition
    virtual Result<std::pair<int32_t, std::shared_ptr<BatchWriter>>> CreateWriter(
        const BinaryRow& partition, int32_t bucket, bool ignore_previous_files) = 0;

    virtual Result<std::unique_ptr<FileStoreScan>> CreateFileStoreScan(
        const std::shared_ptr<ScanFilter>& filter) const = 0;

    Result<std::shared_ptr<RestoreFiles>> ScanExistingFileMetas(const BinaryRow& partition,
                                                                int32_t bucket) const;
    int32_t GetDefaultBucketNum() const;

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<FileStorePathFactory> file_store_path_factory_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<SchemaManager> schema_manager_;
    std::string commit_user_;
    std::string root_path_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::Schema> write_schema_;
    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<arrow::Schema> partition_schema_;
    CoreOptions options_;
    std::shared_ptr<Executor> compact_executor_;
    std::shared_ptr<CompactionMetrics> compaction_metrics_;

 private:
    Result<std::shared_ptr<BatchWriter>> GetWriter(const BinaryRow& partition, int32_t bucket);

 private:
    std::unordered_map<BinaryRow, std::unordered_map<int32_t, WriterContainer<BatchWriter>>>
        writers_;
    bool ignore_previous_files_ = false;
    bool is_streaming_mode_ = false;
    bool ignore_num_bucket_check_ = false;
    bool batch_committed_ = false;

    std::shared_ptr<MetricsImpl> metrics_;
    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon
