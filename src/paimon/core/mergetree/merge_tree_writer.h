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
#include <vector>

#include "arrow/api.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/compact/compact_manager.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/merge_function_wrapper.h"
#include "paimon/core/mergetree/write_buffer.h"
#include "paimon/core/utils/batch_writer.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/core/utils/path_factory.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Array;
class DataType;
class Schema;
class StructArray;
}  // namespace arrow

namespace paimon {
class DataFilePathFactory;
class FieldsComparator;
class MemoryPool;
class Metrics;
template <typename T>
class MergeFunctionWrapper;

class MergeTreeWriter : public BatchWriter {
 public:
    MergeTreeWriter(int64_t last_sequence_number,
                    const std::vector<std::string>& trimmed_primary_keys,
                    const std::shared_ptr<DataFilePathFactory>& path_factory,
                    const std::shared_ptr<FieldsComparator>& key_comparator,
                    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
                    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
                    int64_t schema_id, const std::shared_ptr<arrow::Schema>& value_schema,
                    const CoreOptions& options,
                    const std::shared_ptr<CompactManager>& compact_manager,
                    const std::shared_ptr<MemoryPool>& pool);

    Status Write(std::unique_ptr<RecordBatch>&& batch) override;

    Status Compact(bool full_compaction) override;

    Result<bool> CompactNotCompleted() override;

    Status Sync() override;

    Result<CommitIncrement> PrepareCommit(bool wait_compaction) override;

    Status Close() override {
        return DoClose();
    }

    std::shared_ptr<Metrics> GetMetrics() const override {
        return metrics_;
    }

 private:
    Status DoClose();

    Status Flush(bool wait_for_latest_compaction, bool forced_full_compaction);
    Result<CommitIncrement> DrainIncrement();

    std::unique_ptr<RollingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>
    CreateRollingRowWriter() const;

    Status TrySyncLatestCompaction(bool blocking);
    Status UpdateCompactResult(const std::shared_ptr<CompactResult>& compact_result);
    Status UpdateCompactDeletionFile(const std::shared_ptr<CompactDeletionFile>& new_deletion_file);

 private:
    int64_t last_sequence_number_;
    std::shared_ptr<MemoryPool> pool_;
    std::vector<std::string> trimmed_primary_keys_;
    CoreOptions options_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<FieldsComparator> user_defined_seq_comparator_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
    int64_t schema_id_;
    // write_schema = value_schema + special fields
    std::shared_ptr<arrow::Schema> write_schema_;

    std::shared_ptr<CompactManager> compact_manager_;

    std::unique_ptr<WriteBuffer> write_buffer_;

    std::shared_ptr<Metrics> metrics_;

    std::vector<std::shared_ptr<DataFileMeta>> new_files_;
    std::vector<std::shared_ptr<DataFileMeta>> deleted_files_;
    std::vector<std::shared_ptr<DataFileMeta>> compact_before_;
    std::vector<std::shared_ptr<DataFileMeta>> compact_after_;

    std::shared_ptr<CompactDeletionFile> compact_deletion_file_;
};
}  // namespace paimon
