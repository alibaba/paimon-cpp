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

#include "paimon/core/mergetree/binary_external_sort_buffer.h"

#include <algorithm>
#include <cassert>
#include <limits>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/api.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/io/async_key_value_producer_and_consumer.h"
#include "paimon/core/io/key_value_in_memory_record_reader.h"
#include "paimon/core/io/key_value_meta_projection_consumer.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/core/mergetree/compact/raw_sort_merge_reader_with_min_heap.h"
#include "paimon/core/mergetree/spill_channel_manager.h"
#include "paimon/core/mergetree/spill_reader.h"
#include "paimon/core/mergetree/spill_writer.h"

namespace paimon {

namespace {

std::shared_ptr<arrow::Schema> BuildKeySchema(const std::shared_ptr<arrow::Schema>& value_schema,
                                              const std::vector<std::string>& primary_keys) {
    arrow::FieldVector key_fields;
    key_fields.reserve(primary_keys.size());
    for (const auto& primary_key : primary_keys) {
        auto key_field = value_schema->GetFieldByName(primary_key);
        assert(key_field != nullptr);
        key_fields.push_back(key_field);
    }
    return arrow::schema(key_fields);
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CreateSortedBufferedBatchReaders(
    const std::vector<BufferedWriteBatch>& buffered_batches,
    const std::vector<std::string>& primary_keys,
    const std::vector<std::string>& user_defined_sequence_fields, bool sequence_fields_ascending,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<MemoryPool>& pool) {
    std::vector<std::unique_ptr<KeyValueRecordReader>> readers;
    readers.reserve(buffered_batches.size());
    for (const auto& buffered_batch : buffered_batches) {
        if (buffered_batch.struct_array == nullptr) {
            return Status::Invalid("invalid buffered batch for spill");
        }

        auto struct_array = buffered_batch.struct_array;
        auto row_kinds = buffered_batch.row_kinds;
        readers.push_back(std::make_unique<KeyValueInMemoryRecordReader>(
            buffered_batch.first_sequence_number, std::move(struct_array), std::move(row_kinds),
            primary_keys, user_defined_sequence_fields, sequence_fields_ascending, key_comparator,
            pool));
    }
    return readers;
}

Status WriteMergedSpillFile(std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers,
                            const std::shared_ptr<FieldsComparator>& key_comparator,
                            const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
                            const std::shared_ptr<arrow::Schema>& write_schema,
                            SpillWriter* spill_writer, int32_t write_batch_size,
                            const std::shared_ptr<MemoryPool>& pool) {
    auto sort_merge_reader = std::make_unique<RawSortMergeReaderWithMinHeap>(
        std::move(readers), key_comparator, user_defined_seq_comparator);
    auto create_consumer = [target_schema = write_schema, pool = pool]()
        -> Result<std::unique_ptr<RowToArrowArrayConverter<KeyValue, KeyValueBatch>>> {
        return KeyValueMetaProjectionConsumer::Create(target_schema, pool);
    };
    auto async_key_value_producer_consumer =
        std::make_unique<AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch>>(
            std::move(sort_merge_reader), create_consumer, write_batch_size,
            /*projection_thread_num=*/1, pool);
    auto close_guard = ScopeGuard([&]() { async_key_value_producer_consumer->Close(); });

    while (true) {
        PAIMON_ASSIGN_OR_RAISE(KeyValueBatch key_value_batch,
                               async_key_value_producer_consumer->NextBatch());
        if (key_value_batch.batch == nullptr) {
            break;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::RecordBatch> record_batch,
            arrow::ImportRecordBatch(key_value_batch.batch.get(), write_schema));
        PAIMON_RETURN_NOT_OK(spill_writer->WriteBatch(record_batch));
    }
    return Status::OK();
}

}  // namespace

Result<std::unique_ptr<BinaryExternalSortBuffer>> BinaryExternalSortBuffer::Create(
    std::unique_ptr<BinaryInMemorySortBuffer>&& in_memory_buffer,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::vector<std::string>& trimmed_primary_keys,
    const std::vector<std::string>& user_defined_sequence_fields,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const CoreOptions& options, const std::shared_ptr<IOManager>& io_manager,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(auto spill_channel_enumerator, io_manager->CreateChannelEnumerator());
    return std::unique_ptr<BinaryExternalSortBuffer>(new BinaryExternalSortBuffer(
        std::move(in_memory_buffer), value_schema, trimmed_primary_keys,
        user_defined_sequence_fields, key_comparator, user_defined_seq_comparator, options,
        io_manager, spill_channel_enumerator, pool));
}

BinaryExternalSortBuffer::BinaryExternalSortBuffer(
    std::unique_ptr<BinaryInMemorySortBuffer>&& in_memory_buffer,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::vector<std::string>& trimmed_primary_keys,
    const std::vector<std::string>& user_defined_sequence_fields,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const CoreOptions& options, const std::shared_ptr<IOManager>& io_manager,
    const std::shared_ptr<FileIOChannel::Enumerator>& spill_channel_enumerator,
    const std::shared_ptr<MemoryPool>& pool)
    : in_memory_buffer_(std::move(in_memory_buffer)),
      pool_(pool),
      key_schema_(BuildKeySchema(value_schema, trimmed_primary_keys)),
      value_schema_(value_schema),
      trimmed_primary_keys_(trimmed_primary_keys),
      user_defined_sequence_fields_(user_defined_sequence_fields),
      key_comparator_(key_comparator),
      user_defined_seq_comparator_(user_defined_seq_comparator),
      write_schema_(SpecialFields::CompleteSequenceAndValueKindField(value_schema)),
      options_(options),
      io_manager_(io_manager),
      spill_channel_manager_(std::make_shared<SpillChannelManager>(
          options.GetFileSystem(), options_.GetLocalSortMaxNumFileHandles())),
      spill_channel_enumerator_(spill_channel_enumerator) {}

bool BinaryExternalSortBuffer::HasSpilledData() const {
    return !spill_channel_manager_->GetChannels().empty();
}

std::vector<FileIOChannel::ID> BinaryExternalSortBuffer::GetSpillChannelIdsSnapshot() const {
    const auto& channels = spill_channel_manager_->GetChannels();
    std::vector<FileIOChannel::ID> spill_channel_ids;
    spill_channel_ids.reserve(channels.size());
    for (const auto& spill_channel_id : channels) {
        spill_channel_ids.push_back(spill_channel_id);
    }
    return spill_channel_ids;
}

int64_t BinaryExternalSortBuffer::Size() const {
    return in_memory_buffer_->Size();
}

Status BinaryExternalSortBuffer::Clear() {
    PAIMON_RETURN_NOT_OK(in_memory_buffer_->Clear());
    return CleanupSpillFiles();
}

uint64_t BinaryExternalSortBuffer::GetMemorySize() const {
    return in_memory_buffer_->GetMemorySize();
}

Result<bool> BinaryExternalSortBuffer::FlushMemory() {
    if (!in_memory_buffer_->HasData()) {
        return true;
    }

    int64_t spill_disk_budget = std::numeric_limits<int64_t>::max();
    if (options_.GetWriteBufferSpillMaxDiskSize() != std::numeric_limits<int64_t>::max()) {
        spill_disk_budget = std::max<int64_t>(
            0, options_.GetWriteBufferSpillMaxDiskSize() - total_spill_disk_bytes_);
    }
    if (spill_disk_budget == 0) {
        return false;
    }

    const auto& buffered_batches = in_memory_buffer_->GetBufferedBatches();
    if (buffered_batches.empty()) {
        return true;
    }

    PAIMON_ASSIGN_OR_RAISE(bool spill_result, Spill(spill_disk_budget, buffered_batches));
    if (spill_result) {
        PAIMON_RETURN_NOT_OK(in_memory_buffer_->Clear());
    }
    return spill_result;
}

Result<bool> BinaryExternalSortBuffer::Write(std::unique_ptr<RecordBatch>&& batch) {
    PAIMON_ASSIGN_OR_RAISE(bool has_remaining_capacity, in_memory_buffer_->Write(std::move(batch)));
    if (has_remaining_capacity) {
        return true;
    }
    return FlushMemory();
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>>
BinaryExternalSortBuffer::SortedIterator() {
    if (!HasSpilledData()) {
        return in_memory_buffer_->SortedIterator();
    }

    // Spill remaining in-memory data to disk before merging all spill files
    if (in_memory_buffer_->HasData()) {
        PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] bool spill_result, FlushMemory());
    }

    // Merge all spill files into a single sorted stream
    PAIMON_RETURN_NOT_OK(MergeSpilledFiles());
    return CreateRawSpillReaders();
}

bool BinaryExternalSortBuffer::HasData() const {
    return in_memory_buffer_->HasData() || HasSpilledData();
}

Status BinaryExternalSortBuffer::CleanupSpillFiles() {
    spill_channel_manager_->Reset();
    total_spill_disk_bytes_ = 0;
    return Status::OK();
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>>
BinaryExternalSortBuffer::CreateRawSpillReaders() const {
    std::vector<std::unique_ptr<KeyValueRecordReader>> readers;
    const auto& channel_ids = spill_channel_manager_->GetChannels();
    readers.reserve(channel_ids.size());
    for (const auto& channel_id : channel_ids) {
        PAIMON_ASSIGN_OR_RAISE(auto spill_reader,
                               SpillReader::Create(options_.GetFileSystem(), key_schema_,
                                                   value_schema_, pool_, channel_id));
        readers.push_back(std::move(spill_reader));
    }
    return readers;
}

Result<bool> BinaryExternalSortBuffer::Spill(
    int64_t spill_disk_budget, const std::vector<BufferedWriteBatch>& buffered_batches) {
    const auto& spill_compress_options = options_.GetSpillCompressOptions();
    PAIMON_ASSIGN_OR_RAISE(
        auto spill_writer,
        SpillWriter::Create(options_.GetFileSystem(), write_schema_, spill_channel_enumerator_,
                            spill_channel_manager_, spill_compress_options.compress,
                            spill_compress_options.zstd_level));
    auto cleanup_guard = ScopeGuard([&]() {
        if (!spill_writer->GetChannelId().GetPath().empty()) {
            [[maybe_unused]] auto status =
                spill_channel_manager_->DeleteChannel(spill_writer->GetChannelId());
        }
    });

    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
        CreateSortedBufferedBatchReaders(
            buffered_batches, trimmed_primary_keys_, user_defined_sequence_fields_,
            options_.SequenceFieldSortOrderIsAscending(), key_comparator_, pool_));
    PAIMON_RETURN_NOT_OK(WriteMergedSpillFile(
        std::move(readers), key_comparator_, user_defined_seq_comparator_, write_schema_,
        spill_writer.get(), options_.GetWriteBatchSize(), pool_));
    PAIMON_RETURN_NOT_OK(spill_writer->Close());
    PAIMON_ASSIGN_OR_RAISE(int64_t spill_file_size, spill_writer->GetFileSize());
    if (spill_disk_budget != std::numeric_limits<int64_t>::max() &&
        spill_file_size > spill_disk_budget) {
        return false;
    }

    total_spill_disk_bytes_ += spill_file_size;
    cleanup_guard.Release();

    if (options_.GetLocalSortMaxNumFileHandles() > 0 &&
        static_cast<int32_t>(spill_channel_manager_->GetChannels().size()) >=
            options_.GetLocalSortMaxNumFileHandles()) {
        PAIMON_RETURN_NOT_OK(MergeSpilledFiles());
    }
    return true;
}

Status BinaryExternalSortBuffer::MergeSpilledFiles() {
    if (spill_channel_manager_->GetChannels().size() < 2) {
        return Status::OK();
    }

    auto spill_channel_ids_before_merge = GetSpillChannelIdsSnapshot();
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
                           CreateRawSpillReaders());
    const auto& spill_compress_options = options_.GetSpillCompressOptions();
    PAIMON_ASSIGN_OR_RAISE(
        auto spill_writer,
        SpillWriter::Create(options_.GetFileSystem(), write_schema_, spill_channel_enumerator_,
                            spill_channel_manager_, spill_compress_options.compress,
                            spill_compress_options.zstd_level));
    auto cleanup_guard = ScopeGuard([&]() {
        if (!spill_writer->GetChannelId().GetPath().empty()) {
            [[maybe_unused]] auto status =
                spill_channel_manager_->DeleteChannel(spill_writer->GetChannelId());
        }
    });

    PAIMON_RETURN_NOT_OK(WriteMergedSpillFile(
        std::move(readers), key_comparator_, user_defined_seq_comparator_, write_schema_,
        spill_writer.get(), options_.GetWriteBatchSize(), pool_));
    PAIMON_RETURN_NOT_OK(spill_writer->Close());
    PAIMON_ASSIGN_OR_RAISE(int64_t merged_file_size, spill_writer->GetFileSize());

    if (options_.GetWriteBufferSpillMaxDiskSize() != std::numeric_limits<int64_t>::max() &&
        merged_file_size > options_.GetWriteBufferSpillMaxDiskSize()) {
        return Status::Invalid("merged spill file exceeds write-buffer-spill.max-disk-size");
    }

    for (const auto& spill_channel_id : spill_channel_ids_before_merge) {
        [[maybe_unused]] auto status = spill_channel_manager_->DeleteChannel(spill_channel_id);
    }
    total_spill_disk_bytes_ = merged_file_size;
    cleanup_guard.Release();
    return Status::OK();
}

}  // namespace paimon
