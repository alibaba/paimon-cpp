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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/type_fwd.h"
#include "paimon/core/core_options.h"
#include "paimon/core/disk/file_io_channel.h"
#include "paimon/core/mergetree/in_memory_sort_buffer.h"
#include "paimon/core/mergetree/sort_buffer.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class FieldsComparator;
class IOManager;
class KeyValueRecordReader;
class MemoryPool;
class SpillChannelManager;

/// Spillable SortBuffer. Buffers RecordBatches in an underlying in-memory sort buffer;
/// when the in-memory budget is reached, sorted data is spilled to a new on-disk file.
class ExternalSortBuffer : public SortBuffer {
 public:
    static Result<std::unique_ptr<ExternalSortBuffer>> Create(
        std::unique_ptr<InMemorySortBuffer>&& in_memory_buffer,
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::vector<std::string>& trimmed_primary_keys,
        const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const CoreOptions& options, const std::shared_ptr<IOManager>& io_manager,
        const std::shared_ptr<MemoryPool>& pool);
    ~ExternalSortBuffer() override;

    void Clear() override;
    uint64_t GetMemorySize() const override;
    Result<bool> FlushMemory() override;
    Result<bool> Write(std::unique_ptr<RecordBatch>&& batch) override;
    Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CreateReaders() override;
    bool HasData() const override;

 private:
    void DoClear();
    bool HasSpilledData() const;
    Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CollectSpillReaders() const;
    Result<int64_t> SpillToDisk(std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers,
                                int32_t write_batch_size);
    Status MergeSpilledFiles();
    Status SpillMemoryBuffer(std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers);
    void CleanupSpillFiles();

    ExternalSortBuffer(std::unique_ptr<InMemorySortBuffer>&& in_memory_buffer,
                       const std::shared_ptr<arrow::Schema>& key_schema,
                       const std::shared_ptr<arrow::Schema>& value_schema,
                       const std::shared_ptr<FieldsComparator>& key_comparator,
                       const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
                       const CoreOptions& options,
                       const std::shared_ptr<FileIOChannel::Enumerator>& spill_channel_enumerator,
                       const std::shared_ptr<MemoryPool>& pool);

    std::unique_ptr<InMemorySortBuffer> in_memory_buffer_;

    const std::shared_ptr<MemoryPool> pool_;
    const std::shared_ptr<arrow::Schema> key_schema_;
    const std::shared_ptr<arrow::Schema> value_schema_;
    const std::shared_ptr<FieldsComparator> key_comparator_;
    const std::shared_ptr<FieldsComparator> user_defined_seq_comparator_;
    const std::shared_ptr<arrow::Schema> write_schema_;
    const CoreOptions options_;
    const std::shared_ptr<SpillChannelManager> spill_channel_manager_;

    std::shared_ptr<FileIOChannel::Enumerator> spill_channel_enumerator_;
    int64_t total_spill_disk_bytes_ = 0;
};

}  // namespace paimon
