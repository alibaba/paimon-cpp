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

#include "paimon/core/mergetree/merge_tree_writer.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/async_key_value_producer_and_consumer.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/io/key_value_data_file_writer.h"
#include "paimon/core/io/key_value_in_memory_record_reader.h"
#include "paimon/core/io/key_value_meta_projection_consumer.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/mergetree/compact/sort_merge_reader_with_loser_tree.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/data/decimal.h"
#include "paimon/format/file_format.h"
#include "paimon/format/writer_builder.h"
#include "paimon/metrics.h"

namespace paimon {
class FieldsComparator;
class MemoryPool;
template <typename T>
class MergeFunctionWrapper;
class FormatStatsExtractor;

MergeTreeWriter::MergeTreeWriter(
    int64_t last_sequence_number, const std::vector<std::string>& trimmed_primary_keys,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
    int64_t schema_id, const std::shared_ptr<arrow::Schema>& value_schema,
    const CoreOptions& options, const std::shared_ptr<MemoryPool>& pool)
    : last_sequence_number_(last_sequence_number + 1),
      current_memory_in_bytes_(0),
      pool_(pool),
      trimmed_primary_keys_(trimmed_primary_keys),
      options_(options),
      path_factory_(path_factory),
      key_comparator_(key_comparator),
      user_defined_seq_comparator_(user_defined_seq_comparator),
      merge_function_wrapper_(merge_function_wrapper),
      schema_id_(schema_id),
      value_type_(arrow::struct_(value_schema->fields())),
      metrics_(std::make_shared<MetricsImpl>()) {
    arrow::FieldVector target_fields;
    target_fields.push_back(
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber()));
    target_fields.push_back(DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind()));
    target_fields.insert(target_fields.end(), value_schema->fields().begin(),
                         value_schema->fields().end());
    write_schema_ = arrow::schema(target_fields);
}

Status MergeTreeWriter::Write(std::unique_ptr<RecordBatch>&& moved_batch) {
    if (ArrowArrayIsReleased(moved_batch->GetData())) {
        return Status::Invalid("invalid batch: data is released");
    }
    std::unique_ptr<RecordBatch> batch = std::move(moved_batch);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(batch->GetData(), value_type_));
    auto value_struct_array =
        arrow::internal::checked_pointer_cast<arrow::StructArray>(arrow_array);
    if (value_struct_array == nullptr) {
        return Status::Invalid("invalid RecordBatch: cannot cast to StructArray");
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t memory_in_bytes, EstimateMemoryUse(value_struct_array));
    current_memory_in_bytes_ += memory_in_bytes;

    batch_vec_.push_back(std::move(value_struct_array));
    row_kinds_vec_.push_back(batch->GetRowKind());
    if (current_memory_in_bytes_ >= options_.GetWriteBufferSize()) {
        return Flush();
    }
    return Status::OK();
}

Result<CommitIncrement> MergeTreeWriter::PrepareCommit(bool wait_compaction) {
    // TODO(xinyu.lxy): support wait_compaction
    PAIMON_RETURN_NOT_OK(Flush());
    return DrainIncrement();
}

Status MergeTreeWriter::Flush() {
    if (batch_vec_.empty()) {
        return Status::OK();
    }
    // 1. create key value iter for each record batch
    std::vector<std::unique_ptr<KeyValueRecordReader>> readers;
    readers.reserve(batch_vec_.size());
    for (size_t i = 0; i < batch_vec_.size(); ++i) {
        int64_t sequence_number = last_sequence_number_;
        last_sequence_number_ += batch_vec_[i]->length();
        auto in_memory_reader = std::make_unique<KeyValueInMemoryRecordReader>(
            sequence_number, std::move(batch_vec_[i]), std::move(row_kinds_vec_[i]),
            trimmed_primary_keys_, options_.GetSequenceField(), key_comparator_,
            merge_function_wrapper_, pool_);
        readers.push_back(std::move(in_memory_reader));
    }
    batch_vec_.clear();
    row_kinds_vec_.clear();
    current_memory_in_bytes_ = 0;
    // 2. prepare loser tree sort merge reader
    auto sort_merge_reader = std::make_unique<SortMergeReaderWithLoserTree>(
        std::move(readers), key_comparator_, user_defined_seq_comparator_, merge_function_wrapper_);
    // 3. project key value to arrow array
    auto create_consumer = [target_schema = write_schema_, pool = pool_]()
        -> Result<std::unique_ptr<RowToArrowArrayConverter<KeyValue, KeyValueBatch>>> {
        return KeyValueMetaProjectionConsumer::Create(target_schema, pool);
    };
    // consumer batch size is WriteBatchSize
    auto async_key_value_producer_consumer =
        std::make_unique<AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch>>(
            std::move(sort_merge_reader), create_consumer,
            std::min(options_.GetWriteBatchSize(), MAX_PROJECTION_BATCH_SIZE),
            /*projection_thread_num=*/1, pool_);
    auto rolling_writer = CreateRollingRowWriter();
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(KeyValueBatch key_value_batch,
                               async_key_value_producer_consumer->NextBatch());
        if (key_value_batch.batch == nullptr) {
            break;
        }
        PAIMON_RETURN_NOT_OK(rolling_writer->Write(std::move(key_value_batch)));
    }
    PAIMON_RETURN_NOT_OK(rolling_writer->Close());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> flushed_files,
                           rolling_writer->GetResult());
    new_files_.insert(new_files_.end(), flushed_files.begin(), flushed_files.end());
    metrics_->Merge(rolling_writer->GetMetrics());
    return Status::OK();
}

Result<CommitIncrement> MergeTreeWriter::DrainIncrement() {
    DataIncrement data_increment(std::move(new_files_), std::move(deleted_files_), {});
    CompactIncrement compact_increment({}, {}, {});
    new_files_.clear();
    deleted_files_.clear();
    return CommitIncrement(data_increment, compact_increment);
}

std::unique_ptr<RollingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>
MergeTreeWriter::CreateRollingRowWriter() const {
    auto create_file_writer = [&]()
        -> Result<std::unique_ptr<SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>> {
        ::ArrowSchema arrow_schema;
        ScopeGuard guard([&arrow_schema]() { ArrowSchemaRelease(&arrow_schema); });
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*write_schema_, &arrow_schema));
        auto format = options_.GetWriteFileFormat();
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<WriterBuilder> writer_builder,
            format->CreateWriterBuilder(&arrow_schema, options_.GetWriteBatchSize()));
        writer_builder->WithMemoryPool(pool_);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*write_schema_, &arrow_schema));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatStatsExtractor> stats_extractor,
                               format->CreateStatsExtractor(&arrow_schema));
        auto converter = [](KeyValueBatch key_value_batch, ArrowArray* array) -> Status {
            ArrowArrayMove(key_value_batch.batch.get(), array);
            return Status::OK();
        };
        auto writer = std::make_unique<KeyValueDataFileWriter>(
            options_.GetFileCompression(), converter, schema_id_, FileSource::Append(),
            trimmed_primary_keys_, stats_extractor, write_schema_, path_factory_->IsExternalPath(),
            pool_);
        PAIMON_RETURN_NOT_OK(
            writer->Init(options_.GetFileSystem(), path_factory_->NewPath(), writer_builder));
        return writer;
    };
    return std::make_unique<RollingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>>(
        options_.GetTargetFileSize(/*has_primary_key=*/true), create_file_writer);
}

Result<int64_t> MergeTreeWriter::EstimateMemoryUse(const std::shared_ptr<arrow::Array>& array) {
    arrow::Type::type type = array->type()->id();
    int64_t null_bits_size_in_bytes = (array->length() + 7) / 8;
    switch (type) {
        case arrow::Type::type::BOOL:
            return null_bits_size_in_bytes + array->length() * sizeof(bool);
        case arrow::Type::type::INT8:
            return null_bits_size_in_bytes + array->length() * sizeof(int8_t);
        case arrow::Type::type::INT16:
            return null_bits_size_in_bytes + array->length() * sizeof(int16_t);
        case arrow::Type::type::INT32:
            return null_bits_size_in_bytes + array->length() * sizeof(int32_t);
        case arrow::Type::type::DATE32:
            return null_bits_size_in_bytes + array->length() * sizeof(int32_t);
        case arrow::Type::type::INT64:
            return null_bits_size_in_bytes + array->length() * sizeof(int64_t);
        case arrow::Type::type::FLOAT:
            return null_bits_size_in_bytes + array->length() * sizeof(float);
        case arrow::Type::type::DOUBLE:
            return null_bits_size_in_bytes + array->length() * sizeof(double);
        case arrow::Type::type::TIMESTAMP:
            return null_bits_size_in_bytes + array->length() * sizeof(int64_t);
        case arrow::Type::type::DECIMAL:
            return null_bits_size_in_bytes + array->length() * sizeof(Decimal::int128_t);
        case arrow::Type::type::STRING:
        case arrow::Type::type::BINARY: {
            auto binary_array =
                arrow::internal::checked_cast<const arrow::BinaryArray*>(array.get());
            assert(binary_array);
            int64_t value_length = binary_array->total_values_length();
            int64_t offset_length = array->length() * sizeof(int32_t);
            return null_bits_size_in_bytes + value_length + offset_length;
        }
        case arrow::Type::type::LIST: {
            auto list_array = arrow::internal::checked_cast<const arrow::ListArray*>(array.get());
            assert(list_array);
            PAIMON_ASSIGN_OR_RAISE(int64_t value_mem, EstimateMemoryUse(list_array->values()));
            return null_bits_size_in_bytes + value_mem;
        }
        case arrow::Type::type::MAP: {
            auto map_array = arrow::internal::checked_cast<const arrow::MapArray*>(array.get());
            assert(map_array);
            PAIMON_ASSIGN_OR_RAISE(int64_t key_mem, EstimateMemoryUse(map_array->keys()));
            PAIMON_ASSIGN_OR_RAISE(int64_t item_mem, EstimateMemoryUse(map_array->items()));
            return null_bits_size_in_bytes + key_mem + item_mem;
        }
        case arrow::Type::type::STRUCT: {
            auto struct_array =
                arrow::internal::checked_cast<const arrow::StructArray*>(array.get());
            assert(struct_array);
            int64_t struct_mem = 0;
            for (const auto& field : struct_array->fields()) {
                PAIMON_ASSIGN_OR_RAISE(int64_t field_mem, EstimateMemoryUse(field));
                struct_mem += field_mem;
            }
            return null_bits_size_in_bytes + struct_mem;
        }
        default:
            assert(false);
            return Status::Invalid(fmt::format("Do not support type {} in EstimateMemoryUse",
                                               array->type()->ToString()));
    }
}

}  // namespace paimon
