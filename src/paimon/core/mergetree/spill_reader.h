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

#include <memory>

#include "arrow/array/array_primitive.h"
#include "arrow/ipc/api.h"
#include "paimon/common/data/columnar/columnar_batch_context.h"
#include "paimon/core/disk/file_io_channel.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"

namespace arrow {
class MemoryPool;
}  // namespace arrow

namespace paimon {

class ArrowInputStreamAdapter;
class Metrics;

class SpillReader : public KeyValueRecordReader {
 public:
    static Result<std::unique_ptr<SpillReader>> Create(
        const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<arrow::Schema>& key_schema,
        const std::shared_ptr<arrow::Schema>& value_schema, const std::shared_ptr<MemoryPool>& pool,
        const FileIOChannel::ID& channel_id);

    SpillReader(const SpillReader&) = delete;
    SpillReader& operator=(const SpillReader&) = delete;

    class Iterator : public KeyValueRecordReader::Iterator {
     public:
        explicit Iterator(SpillReader* reader);
        bool HasNext() const override;
        Result<KeyValue> Next() override;

     private:
        int64_t cursor_ = 0;
        SpillReader* reader_ = nullptr;
    };

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;
    void Close() override;

 private:
    SpillReader(const std::shared_ptr<FileSystem>& fs,
                const std::shared_ptr<arrow::Schema>& key_schema,
                const std::shared_ptr<arrow::Schema>& value_schema,
                const std::shared_ptr<MemoryPool>& pool);

    Status Open(const FileIOChannel::ID& channel_id);
    void Reset();

    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<arrow::Schema> key_schema_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<Metrics> metrics_;

    std::shared_ptr<InputStream> in_stream_;
    std::shared_ptr<ArrowInputStreamAdapter> arrow_input_stream_adapter_;
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> arrow_reader_;
    int32_t current_batch_index_ = 0;
    int32_t num_record_batches_ = 0;

    int64_t batch_length_ = 0;
    std::shared_ptr<arrow::NumericArray<arrow::Int64Type>> sequence_number_array_;
    std::shared_ptr<arrow::NumericArray<arrow::Int8Type>> row_kind_array_;
    std::shared_ptr<ColumnarBatchContext> key_ctx_;
    std::shared_ptr<ColumnarBatchContext> value_ctx_;
};

}  // namespace paimon
