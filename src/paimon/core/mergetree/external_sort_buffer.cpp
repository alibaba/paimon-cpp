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

#include <algorithm>
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
    bool enable_multi_thread_spill, const std::shared_ptr<MemoryPool>& pool) {
    if (options.GetLocalSortMaxNumFileHandles() < kSpillMinFanIn) {
        return Status::Invalid(fmt::format(
            "invalid '{}': {}, must be at least {}", Options::LOCAL_SORT_MAX_NUM_FILE_HANDLES,
            options.GetLocalSortMaxNumFileHandles(), kSpillMinFanIn));
    }
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
    return std::unique_ptr<ExternalSortBuffer>(
        new ExternalSortBuffer(std::move(in_memory_buffer), key_schema, value_schema,
                               key_comparator, user_defined_seq_comparator, options,
                               spill_channel_enumerator, enable_multi_thread_spill, pool));
}

ExternalSortBuffer::ExternalSortBuffer(
    std::unique_ptr<InMemorySortBuffer>&& in_memory_buffer,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const CoreOptions& options,
    const std::shared_ptr<FileIOChannel::Enumerator>& spill_channel_enumerator,
    bool enable_multi_thread_spill, const std::shared_ptr<MemoryPool>& pool)
    : in_memory_buffer_(std::move(in_memory_buffer)),
      pool_(pool),
      key_schema_(key_schema),
      value_schema_(value_schema),
      key_comparator_(key_comparator),
      user_defined_seq_comparator_(user_defined_seq_comparator),
      write_schema_(SpecialFields::CompleteSequenceAndValueKindField(value_schema)),
      options_(options),
      max_fan_in_(options.GetLocalSortMaxNumFileHandles()),
      enable_multi_thread_spill_(enable_multi_thread_spill),
      spill_channel_manager_(
          std::make_shared<SpillChannelManager>(options_.GetFileSystem(), max_fan_in_)),
      spill_merger_(std::make_unique<SpillFileMerger>(max_fan_in_)),
      spill_channel_enumerator_(spill_channel_enumerator),
      actual_max_fan_in_(max_fan_in_),
      spill_batch_size_(options_.GetWriteBatchSize()) {}

ExternalSortBuffer::~ExternalSortBuffer() {
    DoClear();
}

bool ExternalSortBuffer::HasSpilledData() const {
    return !spill_channel_manager_->GetChannels().empty();
}

void ExternalSortBuffer::DoClear() {
    in_memory_buffer_->Clear();

    spill_channel_manager_->Reset();
    total_spill_disk_bytes_ = 0;
    spill_merger_->Clear();
}

void ExternalSortBuffer::Clear() {
    DoClear();
}

uint64_t ExternalSortBuffer::GetMemorySize() const {
    return in_memory_buffer_->GetMemorySize();
}

void ExternalSortBuffer::UpdateSpillParameters() {
    int64_t estimated_row_size = in_memory_buffer_->GetEstimateMemoryUseForEachRow();
    if (estimated_row_size <= 0) {
        return;
    }

    const int32_t max_batch_size = options_.GetWriteBatchSize();
    const int32_t min_batch_size = std::min(kSpillMinBatchSize, max_batch_size);
    const int64_t merge_budget = options_.GetWriteBufferSize();
    const int64_t max_memory_use_per_handle = merge_budget / max_fan_in_;

    spill_batch_size_ = max_memory_use_per_handle / estimated_row_size;
    spill_batch_size_ = std::clamp(spill_batch_size_, min_batch_size, max_batch_size);

    actual_max_fan_in_ = merge_budget / (spill_batch_size_ * estimated_row_size);
    actual_max_fan_in_ = std::clamp(actual_max_fan_in_, kSpillMinFanIn, max_fan_in_);

    // Re-derive spill_batch_size_ from the clamped actual_max_fan_in_ to stay within merge_budget.
    spill_batch_size_ = merge_budget / (actual_max_fan_in_ * estimated_row_size);
    spill_batch_size_ = std::clamp(spill_batch_size_, 1, max_batch_size);

    spill_merger_->SetMaxFanIn(actual_max_fan_in_);
}

Result<bool> ExternalSortBuffer::FlushMemory() {
    if (!in_memory_buffer_->HasData()) {
        return true;
    }

    UpdateSpillParameters();
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> memory_buffer_readers,
                           in_memory_buffer_->CreateReaders());
    PAIMON_RETURN_NOT_OK(SpillMemoryBuffer(std::move(memory_buffer_readers)));
    in_memory_buffer_->Clear();
    return total_spill_disk_bytes_ < options_.GetWriteBufferSpillMaxDiskSize();
}

Result<bool> ExternalSortBuffer::Write(std::unique_ptr<RecordBatch>&& batch) {
    PAIMON_ASSIGN_OR_RAISE(bool has_remaining_memory, in_memory_buffer_->Write(std::move(batch)));
    if (has_remaining_memory) {
        return true;
    }
    return FlushMemory();
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> ExternalSortBuffer::CreateReaders() {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> memory_readers,
                           in_memory_buffer_->CreateReaders());
    if (!HasSpilledData()) {
        return memory_readers;
    }

    int32_t max_spill_files = actual_max_fan_in_ - 1;
    PAIMON_RETURN_NOT_OK(
        spill_merger_->RunFinalMergeIfNeeded(max_spill_files, CreateSpillFileMergeFn()));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
                           CreateSpillReaders(spill_merger_->GetAllFiles()));
    readers.insert(readers.end(), std::make_move_iterator(memory_readers.begin()),
                   std::make_move_iterator(memory_readers.end()));
    return readers;
}

bool ExternalSortBuffer::HasData() const {
    return in_memory_buffer_->HasData() || HasSpilledData();
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> ExternalSortBuffer::CreateSpillReaders(
    const std::vector<FileChannelInfo>& files) const {
    std::vector<std::unique_ptr<KeyValueRecordReader>> readers;
    readers.reserve(files.size());
    for (const auto& file : files) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<SpillReader> reader,
            SpillReader::Create(options_.GetFileSystem(), key_schema_, value_schema_,
                                enable_multi_thread_spill_, file.channel_id, pool_));
        readers.push_back(std::move(reader));
    }
    return readers;
}

Result<FileChannelInfo> ExternalSortBuffer::SpillToDisk(
    std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers, int32_t write_batch_size) {
    const auto& spill_compress_options = options_.GetSpillCompressOptions();
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<SpillWriter> spill_writer,
        SpillWriter::Create(options_.GetFileSystem(), write_schema_, spill_channel_enumerator_,
                            spill_channel_manager_, spill_compress_options.compress,
                            spill_compress_options.zstd_level, enable_multi_thread_spill_, pool_));
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
    return FileChannelInfo{spill_writer->GetChannelId(), spilled_file_size};
}

Status ExternalSortBuffer::SpillMemoryBuffer(
    std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers) {
    PAIMON_ASSIGN_OR_RAISE(FileChannelInfo file_info,
                           SpillToDisk(std::move(readers), spill_batch_size_));
    total_spill_disk_bytes_ += file_info.file_size;
    spill_merger_->AddFile(file_info);
    return spill_merger_->RunMergeIfNeeded(CreateSpillFileMergeFn());
}

SpillFileMerger::MergeFn ExternalSortBuffer::CreateSpillFileMergeFn() {
    return [this](const std::vector<FileChannelInfo>& files) -> Result<FileChannelInfo> {
        return MergeAndReplaceFiles(files);
    };
}

Result<FileChannelInfo> ExternalSortBuffer::MergeAndReplaceFiles(
    const std::vector<FileChannelInfo>& files) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
                           CreateSpillReaders(files));
    PAIMON_ASSIGN_OR_RAISE(FileChannelInfo output,
                           SpillToDisk(std::move(readers), spill_batch_size_));
    total_spill_disk_bytes_ += output.file_size;

    for (const auto& file : files) {
        [[maybe_unused]] auto status = spill_channel_manager_->DeleteChannel(file.channel_id);
        total_spill_disk_bytes_ -= file.file_size;
    }
    return output;
}

}  // namespace paimon
