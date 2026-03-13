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
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "paimon/common/data/columnar/columnar_row.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/key_value.h"
#include "paimon/core/utils/batch_writer.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
template <typename TypeClass>
class NumericArray;
}  // namespace arrow
struct ArrowArray;

namespace paimon {
class DataFilePathFactory;
class MemoryPool;
class Metrics;

class PostponeBucketWriter : public BatchWriter {
 public:
    PostponeBucketWriter(const std::vector<std::string>& trimmed_primary_keys,
                         const std::shared_ptr<DataFilePathFactory>& path_factory,
                         int64_t schema_id, const std::shared_ptr<arrow::Schema>& value_schema,
                         const CoreOptions& options, const std::shared_ptr<MemoryPool>& pool);

    ~PostponeBucketWriter() override {
        [[maybe_unused]] auto status = DoClose();
    }

    Status Write(std::unique_ptr<RecordBatch>&& batch) override;

    Status Compact(bool full_compaction) override {
        return Status::NotImplemented("not implemented");
    }

    Result<bool> CompactNotCompleted() override {
        return false;
    }

    Status Sync() override {
        return Status::NotImplemented("not implemented");
    }

    Result<CommitIncrement> PrepareCommit(bool wait_compaction) override;

    Status Close() override {
        return DoClose();
    }

    std::shared_ptr<Metrics> GetMetrics() const override {
        return metrics_;
    }

 private:
    Status DoClose() {
        sequence_number_array_.reset();
        row_kind_array_.reset();
        if (writer_) {
            writer_->Abort();
            writer_.reset();
        }
        return Status::OK();
    }

    Result<std::shared_ptr<arrow::StructArray>> CheckAndCastValueArray(
        ArrowArray* value_array) const;

    Result<std::shared_ptr<arrow::Array>> PrepareSequenceNumberArray(int32_t value_array_length);

    Result<std::shared_ptr<arrow::Array>> PrepareRowKindArray(
        int32_t value_array_length, const std::vector<RecordBatch::RowKind>& row_kind_vec);

    static int64_t GetDeleteRowCount(const std::vector<RecordBatch::RowKind>& row_kind_vec);

    Result<std::pair<std::shared_ptr<ColumnarRow>, std::shared_ptr<ColumnarRow>>> PrepareMinMaxKey(
        const std::shared_ptr<arrow::StructArray>& value_struct_array) const;

    Status Flush();
    Result<CommitIncrement> DrainIncrement();

    std::unique_ptr<RollingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>
    CreateRollingRowWriter() const;

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<arrow::MemoryPool> arrow_pool_;
    std::vector<std::string> trimmed_primary_keys_;
    CoreOptions options_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    int64_t schema_id_;
    // write_schema = value_schema + special fields
    std::shared_ptr<arrow::DataType> value_type_;
    std::shared_ptr<arrow::Schema> write_schema_;
    std::shared_ptr<Metrics> metrics_;
    std::vector<std::shared_ptr<DataFileMeta>> new_files_;
    std::unique_ptr<RollingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>> writer_;
    std::shared_ptr<arrow::Array> sequence_number_array_;
    std::shared_ptr<arrow::NumericArray<arrow::Int8Type>> row_kind_array_;
};
}  // namespace paimon
