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

#include "paimon/core/mergetree/external_sort_buffer.h"

#include <cassert>
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
#include "paimon/core/mergetree/compact/sort_merge_reader_with_min_heap.h"
#include "paimon/core/mergetree/spill_channel_manager.h"
#include "paimon/core/mergetree/spill_reader.h"
#include "paimon/core/mergetree/spill_writer.h"

namespace paimon {

Result<std::unique_ptr<ExternalSortBuffer>> ExternalSortBuffer::Create(
    std::unique_ptr<InMemorySortBuffer>&& in_memory_buffer,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::vector<std::string>& trimmed_primary_keys,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const CoreOptions& options, const std::shared_ptr<IOManager>& io_manager,
    const std::shared_ptr<MemoryPool>& pool) {
    arrow::FieldVector key_fields;
    key_fields.reserve(trimmed_primary_keys.size());
    for (const auto& primary_key : trimmed_primary_keys) {
        auto key_field = value_schema->GetFieldByName(primary_key);
        assert(key_field != nullptr);
        key_fields.push_back(key_field);
    }
    auto key_schema = arrow::schema(key_fields);

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIOChannel::Enumerator> spill_channel_enumerator,
                           io_manager->CreateChannelEnumerator());
    return std::unique_ptr<ExternalSortBuffer>(new ExternalSortBuffer(
        std::move(in_memory_buffer), key_schema, value_schema, key_comparator,
        user_defined_seq_comparator, options, spill_channel_enumerator, pool));
}

ExternalSortBuffer::ExternalSortBuffer(
    std::unique_ptr<InMemorySortBuffer>&& in_memory_buffer,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const CoreOptions& options,
    const std::shared_ptr<FileIOChannel::Enumerator>& spill_channel_enumerator,
    const std::shared_ptr<MemoryPool>& pool)
    : in_memory_buffer_(std::move(in_memory_buffer)),
      pool_(pool),
      key_schema_(key_schema),
      value_schema_(value_schema),
      key_comparator_(key_comparator),
      user_defined_seq_comparator_(user_defined_seq_comparator),
      write_schema_(SpecialFields::CompleteSequenceAndValueKindField(value_schema)),
      options_(options),
      spill_channel_manager_(std::make_shared<SpillChannelManager>(
          options_.GetFileSystem(), options_.GetLocalSortMaxNumFileHandles())),
      spill_channel_enumerator_(spill_channel_enumerator) {}

ExternalSortBuffer::~ExternalSortBuffer() {
    DoClear();
}

bool ExternalSortBuffer::HasSpilledData() const {
    return !spill_channel_manager_->GetChannels().empty();
}

void ExternalSortBuffer::DoClear() {
    in_memory_buffer_->Clear();
    CleanupSpillFiles();
}

void ExternalSortBuffer::Clear() {
    DoClear();
}

uint64_t ExternalSortBuffer::GetMemorySize() const {
    return in_memory_buffer_->GetMemorySize();
}

Result<bool> ExternalSortBuffer::FlushMemory() {
    if (!in_memory_buffer_->HasData()) {
        return true;
    }

    int64_t max_spill_disk_size = options_.GetWriteBufferSpillMaxDiskSize();

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> memory_buffer_readers,
                           in_memory_buffer_->CreateReaders());
    PAIMON_RETURN_NOT_OK(SpillMemoryBuffer(std::move(memory_buffer_readers)));
    in_memory_buffer_->Clear();
    return total_spill_disk_bytes_ < max_spill_disk_size;
}

Result<bool> ExternalSortBuffer::Write(std::unique_ptr<RecordBatch>&& batch) {
    PAIMON_ASSIGN_OR_RAISE(bool has_remaining_memory, in_memory_buffer_->Write(std::move(batch)));
    if (has_remaining_memory) {
        return true;
    }
    return FlushMemory();
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> ExternalSortBuffer::CreateReaders() {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
                           CollectSpillReaders());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> memory_readers,
                           in_memory_buffer_->CreateReaders());

    readers.insert(readers.end(), std::make_move_iterator(memory_readers.begin()),
                   std::make_move_iterator(memory_readers.end()));
    return readers;
}

bool ExternalSortBuffer::HasData() const {
    return in_memory_buffer_->HasData() || HasSpilledData();
}

void ExternalSortBuffer::CleanupSpillFiles() {
    spill_channel_manager_->Reset();
    total_spill_disk_bytes_ = 0;
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> ExternalSortBuffer::CollectSpillReaders()
    const {
    std::vector<std::unique_ptr<KeyValueRecordReader>> readers;
    const auto& channel_ids = spill_channel_manager_->GetChannels();
    readers.reserve(channel_ids.size());
    for (const auto& channel_id : channel_ids) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SpillReader> spill_reader,
                               SpillReader::Create(options_.GetFileSystem(), key_schema_,
                                                   value_schema_, pool_, channel_id));
        readers.push_back(std::move(spill_reader));
    }
    return readers;
}

Result<int64_t> ExternalSortBuffer::SpillToDisk(
    std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers, int32_t write_batch_size) {
    const auto& spill_compress_options = options_.GetSpillCompressOptions();
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<SpillWriter> spill_writer,
        SpillWriter::Create(options_.GetFileSystem(), write_schema_, spill_channel_enumerator_,
                            spill_channel_manager_, spill_compress_options.compress,
                            spill_compress_options.zstd_level));
    auto cleanup_guard = ScopeGuard([&]() {
        [[maybe_unused]] auto status =
            spill_channel_manager_->DeleteChannel(spill_writer->GetChannelId());
    });

    auto sorted_reader = std::make_unique<SortMergeReaderWithMinHeap>(
        std::move(readers), key_comparator_, user_defined_seq_comparator_,
        /*merge_function_wrapper=*/nullptr);
    auto create_consumer = [target_schema = write_schema_, pool = pool_]()
        -> Result<std::unique_ptr<RowToArrowArrayConverter<KeyValue, KeyValueBatch>>> {
        return KeyValueMetaProjectionConsumer::Create(target_schema, pool);
    };
    auto async_key_value_producer_consumer =
        std::make_unique<AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch>>(
            std::move(sorted_reader), create_consumer, write_batch_size,
            /*projection_thread_num=*/1, pool_);
    auto close_guard = ScopeGuard([&]() { async_key_value_producer_consumer->Close(); });

    while (true) {
        PAIMON_ASSIGN_OR_RAISE(KeyValueBatch key_value_batch,
                               async_key_value_producer_consumer->NextBatch());
        if (key_value_batch.batch == nullptr) {
            break;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::RecordBatch> record_batch,
            arrow::ImportRecordBatch(key_value_batch.batch.get(), write_schema_));
        PAIMON_RETURN_NOT_OK(spill_writer->WriteBatch(record_batch));
    }

    PAIMON_RETURN_NOT_OK(spill_writer->Close());
    PAIMON_ASSIGN_OR_RAISE(int64_t spilled_file_size, spill_writer->GetFileSize());
    cleanup_guard.Release();
    return spilled_file_size;
}

Status ExternalSortBuffer::SpillMemoryBuffer(
    std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers) {
    PAIMON_ASSIGN_OR_RAISE(int64_t spill_file_size,
                           SpillToDisk(std::move(readers), options_.GetWriteBatchSize()));
    total_spill_disk_bytes_ += spill_file_size;

    if (options_.GetLocalSortMaxNumFileHandles() > 0 &&
        static_cast<int32_t>(spill_channel_manager_->GetChannels().size()) >=
            options_.GetLocalSortMaxNumFileHandles()) {
        PAIMON_RETURN_NOT_OK(MergeSpilledFiles());
    }
    return Status::OK();
}

Status ExternalSortBuffer::MergeSpilledFiles() {
    if (spill_channel_manager_->GetChannels().size() < 2) {
        return Status::OK();
    }
    auto spill_channel_ids_before_merge = spill_channel_manager_->GetChannels();
    auto cleanup_guard = ScopeGuard([&]() {
        for (const auto& spill_channel_id : spill_channel_ids_before_merge) {
            [[maybe_unused]] auto status = spill_channel_manager_->DeleteChannel(spill_channel_id);
        }
    });

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
                           CollectSpillReaders());
    PAIMON_ASSIGN_OR_RAISE(int64_t merged_file_size,
                           SpillToDisk(std::move(readers), options_.GetWriteBatchSize()));
    total_spill_disk_bytes_ = merged_file_size;

    return Status::OK();
}

}  // namespace paimon
