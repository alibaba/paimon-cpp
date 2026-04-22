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
#include "paimon/core/mergetree/binary_in_memory_sort_buffer.h"
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

/// BinaryExternalSortBuffer is a SortBuffer implementation that supports spill-to-disk.
/// It holds a BinaryInMemorySortBuffer for in-memory buffering and spills data to disk
/// when the memory buffer is full.
class BinaryExternalSortBuffer : public SortBuffer {
 public:
    static Result<std::unique_ptr<BinaryExternalSortBuffer>> Create(
        std::unique_ptr<BinaryInMemorySortBuffer>&& in_memory_buffer,
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::vector<std::string>& trimmed_primary_keys,
        const std::vector<std::string>& user_defined_sequence_fields,
        const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const CoreOptions& options, const std::shared_ptr<IOManager>& io_manager,
        const std::shared_ptr<MemoryPool>& pool);

    int64_t Size() const override;
    Status Clear() override;
    uint64_t GetMemorySize() const override;
    Result<bool> FlushMemory() override;
    Result<bool> Write(std::unique_ptr<RecordBatch>&& batch) override;
    Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> SortedIterator() override;
    bool HasData() const override;

 private:
    bool HasSpilledData() const;
    std::vector<FileIOChannel::ID> GetSpillChannelIdsSnapshot() const;
    Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CreateRawSpillReaders() const;
    Status MergeSpilledFiles();
    Result<bool> Spill(int64_t spill_disk_budget,
                       const std::vector<BufferedWriteBatch>& buffered_batches);
    Status CleanupSpillFiles();

    BinaryExternalSortBuffer(
        std::unique_ptr<BinaryInMemorySortBuffer>&& in_memory_buffer,
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::vector<std::string>& trimmed_primary_keys,
        const std::vector<std::string>& user_defined_sequence_fields,
        const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const CoreOptions& options, const std::shared_ptr<IOManager>& io_manager,
        const std::shared_ptr<FileIOChannel::Enumerator>& spill_channel_enumerator,
        const std::shared_ptr<MemoryPool>& pool);

    std::unique_ptr<BinaryInMemorySortBuffer> in_memory_buffer_;

    const std::shared_ptr<MemoryPool> pool_;
    const std::shared_ptr<arrow::Schema> key_schema_;
    const std::shared_ptr<arrow::Schema> value_schema_;
    const std::vector<std::string> trimmed_primary_keys_;
    const std::vector<std::string> user_defined_sequence_fields_;
    const std::shared_ptr<FieldsComparator> key_comparator_;
    const std::shared_ptr<FieldsComparator> user_defined_seq_comparator_;
    const std::shared_ptr<arrow::Schema> write_schema_;
    const CoreOptions options_;
    const std::shared_ptr<IOManager> io_manager_;
    const std::shared_ptr<SpillChannelManager> spill_channel_manager_;

    std::shared_ptr<FileIOChannel::Enumerator> spill_channel_enumerator_;
    int64_t total_spill_disk_bytes_ = 0;
};

}  // namespace paimon
